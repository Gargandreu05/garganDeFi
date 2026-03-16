"""
agents/coordinator.py — Central Orchestrator
==============================================
The brain. Implements the Observe → Analyze → Decide → Act → Verify loop.
Orchestrates the full pipeline between agents and manages circuit breakers.
"""

from __future__ import annotations

import asyncio
import os
import time
import structlog
from typing import Optional, Any

from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair

log = structlog.get_logger(__name__)

def _load_keypair() -> Keypair:
    raw = os.getenv("WALLET_PRIVATE_KEY_BASE58", "").strip()
    if not raw:
        raise EnvironmentError("WALLET_PRIVATE_KEY_BASE58 not set in .env")
    try:
        return Keypair.from_base58_string(raw)
    except Exception as exc:
        raise EnvironmentError(f"Invalid private key: {exc}") from exc

class Coordinator:
    """
    Orchestrates the Multi-Agent system loop and circuit breakers.
    """

    def __init__(
        self,
        market: Any,
        scanner: Any,
        risk: Any,
        executor: Any,
        discord: Any,
        db: Any
    ) -> None:
        self._market_agent = market
        self._scanner_agent = scanner
        self._risk_agent = risk
        self._execution_agent = executor
        self._discord_agent = discord
        self._db = db

        # Core RPC and Auth
        url = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
        self._rpc = AsyncClient(url)
        self._keypair = _load_keypair()

        # State / Controls
        self._pending_migration: Optional[dict] = None
        self._failure_count = 0
        self._paused_until = 0.0
        self._circuit_breaker_minutes = int(os.getenv("CIRCUIT_BREAKER_PAUSE_MINUTES", "30"))
        self._autonomous = os.getenv("AUTONOMOUS_POOL_SWITCHING", "false").lower() == "true"

    async def run_forever(self) -> None:
        """Main Orchestration Loop."""
        log.info("coordinator_loop_started")
        
        # Start background harvesting cycle for Phase 2
        harvest_task = asyncio.create_task(self._harvest_rewards_cycle())
        
        while True:
            try:
                if time.time() < self._paused_until:
                    await asyncio.sleep(60)
                    continue

                await self._orchestrate_cycle()
                
            except Exception as exc:
                log.error("coordinator_cycle_failed", error=str(exc))
                
            await asyncio.sleep(int(os.getenv("POOL_SCAN_INTERVAL_SECONDS", "300")))

    async def _harvest_rewards_cycle(self) -> None:
        """Periodic harvesting and re-investing (Auto-Compounding)."""
        while True:
            try:
                log.info("harvest_cycle_triggered")
                active_pool = os.getenv("ACTIVE_POOL_ID", "")
                if active_pool:
                     res = await self._execution_agent.harvest_rewards(active_pool, self._rpc)
                     if res.success and res.data.get("amount_harvested", 0) > 0:
                          await self._discord_agent.send_update(f"🌾 Auto-Compounded: Re-invested {res.data['amount_harvested']} rewards.")
                          
            except Exception as exc:
                 log.warning("harvest_cycle_failed", error=str(exc))
            
            # Run every 12 hours
            await asyncio.sleep(12 * 3600)

    async def _orchestrate_cycle(self) -> None:
        """Single Observe -> Analyze -> Decide loop cycle."""
        log.info("orchestrate_cycle_starting")

        # 1. Observe
        wallet_snapshot = await self._market_agent.get_wallet_snapshot(self._rpc, self._keypair)
        sol_price = await self._market_agent.get_sol_price_usd()

        # 2. Analyze (Scan)
        candidates = await self._scanner_agent.discover_pools()
        if not candidates:
            log.info("no_candidates_found")
            return

        # Phase 2.1: Multi-Pool Allocation (Invest in top 2 candidates)
        max_pools = int(os.getenv("MAX_ACTIVE_POOLS", "2"))
        targets = candidates[:max_pools]
        log.info("multi_pool_allocation_targets", count=len(targets))

        for cand in targets:
            log.info("evaluating_candidate", pool=cand["name"])

            # ── 🔬 Phase 5: Filtro de Estabilidad Histórica ───────────────────
            p_id = cand["pool_id"]
            if hasattr(self, "_db") and hasattr(self._db, "get_evaluations_for_pool"):
                hist = await self._db.get_evaluations_for_pool(p_id, limit=30)
                if hist:
                    from datetime import datetime, timezone
                    try:
                        # oldest is hist[-1] since ordered by ts DESC
                        oldest_ts = datetime.fromisoformat(hist[-1]["ts"])
                        hours_tracked = (datetime.now(timezone.utc) - oldest_ts).total_seconds() / 3600.0
                    except Exception:
                        hours_tracked = 0.0

                    req_hours = float(os.getenv("HISTORIC_STABILITY_HOURS", "24.0"))
                    if hours_tracked < req_hours:
                        log.info("pool_needs_more_history_skipping", pool=cand["name"], hours_tracked=f"{hours_tracked:.2f}", required=req_hours)
                        continue
                else:
                    log.info("pool_no_history_skipping", pool=cand["name"])
                    continue

            # 3. Decide (Risk)
            risk_result = await self._risk_agent.validate_migration(
                cand, wallet_snapshot, sol_price
            )

            if not risk_result.approved:
                log.info("migration_rejected_by_risk", pool=cand["name"], reason=risk_result.reason)
                continue  # Try next

            # Divide allocation by pool count to diversify
            if len(targets) > 1:
                risk_result.sol_to_swap = risk_result.sol_to_swap / len(targets)
                log.info("sol_to_swap_diversified", pool=cand["name"], new_amount=risk_result.sol_to_swap)

            self._pending_migration = {
                "candidate": cand,
                "risk_result": risk_result,
                "wallet_snapshot": wallet_snapshot
            }

            if self._autonomous:
                log.info("autonomous_execution_triggered")
                await self.approve_migration_cmd()
            else:
                log.info("hitl_alert_triggered")
                await self._discord_agent.send_migration_alert(
                    current={"name": "Active", "net_apy": 0.0},
                    candidate=cand,
                    reason=f"Risk approved (Safe/Multi-pool). Ready to invest."
                )
                # For safety in HITL, we only present ONE pending at a time
                break

    # ── Command/HITL Hooks ───────────────────────────────────────────────────

    async def approve_migration_cmd(self) -> None:
        """Triggered via Discord buttons or autonomous mode approving pending."""
        if not self._pending_migration:
            log.warning("approve_migration_cmd_no_pending")
            return

        cand = self._pending_migration["candidate"]
        risk_res = self._pending_migration["risk_result"]

        log.info("executing_migration", pool_id=cand["pool_id"])
        self._pending_migration = None  # Clear

        # 1. Withdraw
        active_pool = os.getenv("ACTIVE_POOL_ID", "")
        active_lp = os.getenv("ACTIVE_LP_MINT", "")
        if active_pool:
            withdraw_res = await self._execution_agent.withdraw_lp(active_pool, active_lp, self._rpc)
            if not withdraw_res.success:
                await self._handle_failure(f"Withdraw failed: {withdraw_res.error}")
                return

        # Wait for settlement
        await asyncio.sleep(5)

        # 2. Swap
        quote_mint = cand.get("quote_mint") or os.getenv("ACTIVE_POOL_QUOTE_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
        swap_res = await self._execution_agent.swap_to_quote(risk_res, quote_mint, self._rpc)
        if not swap_res.success:
            await self._handle_failure(f"Swap failed: {swap_res.error}")
            return

        # 3. Deposit
        # Re-fetch amounts from swap_res or compute if skipped
        base_lamports = int(risk_res.sol_to_swap * 1_000_000_000) # Simple split or recalculate split
        # In execution_agent, it provides zap_result data if not skipped
        quote_raw = int(swap_res.data.get("out_amount_raw", risk_res.existing_quote_raw))

        deposit_res = await self._execution_agent.deposit_liquidity(
            pool_id=cand["pool_id"],
            base_lamports=base_lamports,
            quote_raw=quote_raw,
            rpc=self._rpc,
            dex_id=cand.get("dex_id", "raydium")
        )

        if deposit_res.success:
            self._failure_count = 0  # Reset
            log.info("migration_success", pool_id=cand["pool_id"])
            await self._discord_agent.send_update(f"✅ Migration to **{cand['name']}** successful! TX: `{deposit_res.tx_sig[:20]}…`")
            # Update ENV
            self._update_env_active_pool(cand["pool_id"], quote_mint)
        else:
            await self._handle_failure(f"Deposit failed: {deposit_res.error}")

    async def reject_migration(self) -> None:
        self._pending_migration = None
        log.info("migration_rejected_by_user")

    async def execute_full_withdraw(self) -> None:
        """Exits all token positions back into SOL."""
        log.info("full_withdraw_starting")
        wallet_snapshot = await self._market_agent.get_wallet_snapshot(self._rpc, self._keypair)
        
        # Risk Agent check
        risk_res = await self._risk_agent.validate_withdraw(wallet_snapshot)
        if not risk_res.approved:
            await self._discord_agent.send_update(f"ℹ️ {risk_res.reason}", success=True)
            return

        res = await self._execution_agent.withdraw_all_tokens(wallet_snapshot, self._rpc)
        if res.success:
            await self._discord_agent.send_update(f"✅ Full withdrawal complete! Swapped {res.data.get('swapped')} tokens.")
        else:
             await self._discord_agent.send_update(f"⚠️ Partial withdrawal: {res.error}", success=False)

    async def emergency_withdraw(self) -> None:
        """Pull LP immediately."""
        active_pool = os.getenv("ACTIVE_POOL_ID", "")
        active_lp = os.getenv("ACTIVE_LP_MINT", "")
        if active_pool:
            res = await self._execution_agent.withdraw_lp(active_pool, active_lp, self._rpc)
            if res.success:
                 await self._discord_agent.send_update("🚨 Emergency LP withdrawal completed.")
            else:
                 await self._discord_agent.send_update(f"❌ Emergency LP withdrawal failed: {res.error}", success=False)

    async def manual_invest(self, pool_id: str) -> None:
        """Creates a synthetic evaluation to test/trigger coordinates."""
        # Simple scaffold: update Env and trigger a cycle or force approve-path
        self._update_env_active_pool(pool_id, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
        await self._discord_agent.send_update(f"Invested manual tracking for `{pool_id}`")

    # ── Helpers ──────────────────────────────────────────────────────────────

    async def _handle_failure(self, error_msg: str) -> None:
        self._failure_count += 1
        log.error("execution_failure", failures=self._failure_count, error=error_msg)
        await self._discord_agent.send_update(f"❌ Execution Failure: {error_msg}", success=False)

        if self._failure_count >= 2:
            self._paused_until = time.time() + (self._circuit_breaker_minutes * 60)
            log.warning("circuit_breaker_tripped", minutes=self._circuit_breaker_minutes)
            await self._discord_agent.send_update(
                f"🚨 **Circuit Breaker Tripped!** Pausing operations for {self._circuit_breaker_minutes} minutes.",
                success=False
            )

    def _update_env_active_pool(self, pool_id: str, quote_mint: str) -> None:
        """Update active pool in dotenv."""
        from dotenv import set_key, find_dotenv
        try:
            path = find_dotenv(usecwd=True) or ".env"
            set_key(path, "ACTIVE_POOL_ID", pool_id)
            set_key(path, "ACTIVE_POOL_QUOTE_MINT", quote_mint)
            os.environ["ACTIVE_POOL_ID"] = pool_id
            os.environ["ACTIVE_POOL_QUOTE_MINT"] = quote_mint
            log.info("env_updated", pool_id=pool_id)
        except Exception as exc:
            log.error("env_update_failed", error=str(exc))

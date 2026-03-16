"""
agents/execution_agent.py — On-Chain Operations
================================================
Executes on-chain operations (withdraw, swap, deposit).
Receives pre-validated RiskResult. Never makes its own risk decisions.
Updates trade status in database.
"""

from __future__ import annotations

import os
import structlog
from dataclasses import dataclass, field
from typing import Optional, Any

from defi_engine.execution import RaydiumExecutor
from defi_engine.jupiter_zap import JupiterZap

# ── Jito Executors (Phase 2) ──────────────────────────────────────────────────
from agents.jito_executor import JitoRaydiumExecutor, JitoJupiterZap

log = structlog.get_logger(__name__)

@dataclass
class ExecResult:
    success: bool
    tx_sig: Optional[str] = None
    error: Optional[str] = None
    data: dict = field(default_factory=dict)

class ExecutionAgent:
    """
    Executes transactions on Solana and records results in DB.
    """

    def __init__(self, db: Any) -> None:
        self._db = db

    async def withdraw_lp(self, pool_id: str, lp_mint: str, rpc: Any) -> ExecResult:
        """
        Remove liquidity from a Raydium pool.
        If lp_mint is empty or balance is 0, skips cleanly without error.
        """
        log.info("withdraw_lp_starting", pool_id=pool_id, lp_mint=lp_mint)
        trade_id = None

        try:
            use_jito = os.getenv("USE_JITO", "true").lower() == "true"
            executor_cls = JitoRaydiumExecutor if use_jito else RaydiumExecutor
            async with executor_cls(rpc) as executor:
                if not lp_mint:
                    log.info("withdraw_lp_skip_no_mint")
                    return ExecResult(success=True, error="No LP mint provided, skipped.")

                lp_balance = await executor.get_lp_token_balance(lp_mint)
                if lp_balance <= 0:
                    log.info("withdraw_lp_skip_zero_balance")
                    return ExecResult(success=True, error="LP balance is zero, skipped.")

                trade_id = await self._db.insert_trade(
                    trade_type="WITHDRAW",
                    pool_id=pool_id,
                    pool_name="Raydium Pool",
                    status="PENDING",
                )

                sig = await executor.remove_liquidity(pool_id=pool_id, lp_amount=lp_balance)
                
                await self._db.update_trade_status(trade_id, "CONFIRMED", sig)
                return ExecResult(success=True, tx_sig=sig)

        except Exception as exc:
            log.error("withdraw_lp_failed", pool_id=pool_id, error=str(exc))
            if trade_id:
                await self._db.update_trade_status(trade_id, "FAILED")
            return ExecResult(success=False, error=str(exc))

    async def swap_to_quote(self, risk_result: Any, quote_mint: str, rpc: Any) -> ExecResult:
        """
        Swap SOL to quote token.
        If risk_result.already_balanced is True, skips swap and returns success.
        """
        log.info("swap_to_quote_starting", quote_mint=quote_mint)
        trade_id = None

        if risk_result.already_balanced:
            log.info("swap_to_quote_skip_already_balanced")
            return ExecResult(success=True, error="Already balanced, skipped.", data={"tx_signature": "SKIPPED"})

        try:
            use_jito = os.getenv("USE_JITO", "true").lower() == "true"
            zap_cls = JitoJupiterZap if use_jito else JupiterZap
            async with zap_cls(rpc) as zap:
                trade_id = await self._db.insert_trade(
                    trade_type="SWAP",
                    pool_id="N/A",  # Not specific to pool yet
                    pool_name="Jupiter Swap",
                    status="PENDING",
                )

                gas_reserve = float(os.getenv("GAS_RESERVE_SOL", "0.02"))
                zap_result = await zap.zap_sol_to_token(
                    quote_mint=quote_mint,
                    gas_reserve_sol=gas_reserve,
                )

                sig = zap_result["tx_signature"]
                await self._db.update_trade_status(trade_id, "CONFIRMED", sig)
                return ExecResult(success=True, tx_sig=sig, data=zap_result)

        except Exception as exc:
            log.error("swap_to_quote_failed", error=str(exc))
            if trade_id:
                await self._db.update_trade_status(trade_id, "FAILED")
            return ExecResult(success=False, error=str(exc))

    async def deposit_liquidity(
        self, 
        pool_id: str, 
        base_lamports: int, 
        quote_raw: int, 
        rpc: Any,
        dex_id: str = "raydium"
    ) -> ExecResult:
        """
        Deposit liquidity into pool.
        Uses JitoRaydiumExecutor for Raydium, or Node worker for Meteora.
        """
        log.info("deposit_liquidity_starting", pool_id=pool_id, dex_id=dex_id)
        
        if dex_id.lower() == "meteora":
            import asyncio
            try:
                log.info("triggering_meteora_ts_worker", pool_id=pool_id)
                # Ensure using exact absolute layout for security if needed, relative is fine if starting context is workspace
                proc = await asyncio.create_subprocess_exec(
                    "node", "execution_worker/dist/deposit_dlmm.js",
                    pool_id, str(base_lamports), str(quote_raw),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                out_str = stdout.decode()
                err_str = stderr.decode()

                if proc.returncode != 0:
                     log.error("meteora_ts_worker_failed", error=err_str)
                     return ExecResult(success=False, error=err_str)

                # Fetch sig from stdout output format: "✅ Success! Sig: ..."
                log.info("meteora_ts_worker_success", output=out_str)
                # Assuming the output contains the signature directly or can be parsed
                # For now, returning the whole output as signature for simplicity
                return ExecResult(success=True, tx_sig=out_str.strip())
            except Exception as exc:
                log.error("meteora_ts_worker_exception", error=str(exc))
                return ExecResult(success=False, error=str(exc))

        trade_id = None
        try:
            use_jito = os.getenv("USE_JITO", "true").lower() == "true"
            executor_cls = JitoRaydiumExecutor if use_jito else RaydiumExecutor
            async with executor_cls(rpc) as executor:
                trade_id = await self._db.insert_trade(
                    trade_type="DEPOSIT",
                    pool_id=pool_id,
                    pool_name="Raydium Pool",
                    status="PENDING",
                )

                sig = await executor.add_liquidity(
                    pool_id=pool_id,
                    base_amount_lamports=base_lamports,
                    quote_amount=quote_raw,
                )

                await self._db.update_trade_status(trade_id, "CONFIRMED", sig)
                return ExecResult(success=True, tx_sig=sig)

        except Exception as exc:
            log.error("deposit_liquidity_failed", pool_id=pool_id, error=str(exc))
            if trade_id:
                await self._db.update_trade_status(trade_id, "FAILED")
            return ExecResult(success=False, error=str(exc))

    async def withdraw_all_tokens(self, wallet_snapshot: dict, rpc: Any) -> ExecResult:
        """
        Swap all non-SOL tokens in the snapshot back to SOL.
        Tracks swapped_count and failed_count separately.
        """
        log.info("withdraw_all_tokens_starting")
        swapped_count = 0
        failed_count = 0
        errors = []
        final_sol = 0.0

        try:
            use_jito = os.getenv("USE_JITO", "true").lower() == "true"
            zap_cls = JitoJupiterZap if use_jito else JupiterZap
            async with zap_cls(rpc) as zap:
                # To track swappable tokens, we can use risk_result validation or just iterate
                tokens = wallet_snapshot.get("tokens", [])
                
                for t in tokens:
                    mint = t.get("mint")
                    amount_raw = int(t.get("amount_raw", 0))
                    amount_ui = float(t.get("amount_ui", 0.0))

                    if amount_raw == 0 or mint == "So11111111111111111111111111111111111111112":
                        continue
                    if amount_ui < 0.01:
                        continue

                    try:
                        log.info("swapping_token_to_sol", mint=mint, amount=amount_ui)
                        result = await zap.zap_token_to_sol(
                            token_mint=mint,
                            token_amount_raw=amount_raw,
                        )
                        
                        await self._db.insert_trade(
                            trade_type="WITHDRAW",
                            pool_id="N/A",
                            pool_name="Withdraw All",
                            amount_token=amount_ui,
                            token_mint=mint,
                            tx_signature=result["tx_signature"],
                            status="CONFIRMED",
                        )
                        swapped_count += 1
                        
                    except Exception as exc:
                        log.error("swap_token_to_sol_failed", mint=mint, error=str(exc))
                        failed_count += 1
                        errors.append(f"Failed to swap {mint[:10]}...: {exc}")
                        continue

                final_sol = await zap.get_wallet_sol_balance()

            log.info(
                "withdraw_all_tokens_completed",
                swapped=swapped_count,
                failed=failed_count
            )
            return ExecResult(
                success=failed_count == 0,
                error=", ".join(errors) if errors else None,
                data={
                    "swapped": swapped_count,
                    "failed": failed_count,
                    "final_sol": final_sol,
                    "errors": errors
                }
            )

        except Exception as exc:
            log.critical("withdraw_all_tokens_critical_failed", error=str(exc))
            return ExecResult(success=False, error=str(exc))

    async def harvest_rewards(self, pool_id: str, rpc: Any) -> ExecResult:
        """
        [Phase 2] Harvest farming rewards and re-invest (Auto-Compounding).
        Scaffold: requires Raydium Farm / Staking layout integration.
        """
        log.info("harvest_rewards_scaffold_triggered", pool_id=pool_id)
        # Mocking successful check without action to prevent crashes
        return ExecResult(success=True, data={"amount_harvested": 0.0, "status": "scaffold_active"})

    # ── [Phase 2] Concentrated Liquidity (CLMM/DLMM) Scaffold ───────────────

    async def add_liquidity_clmm(self, pool_id: str, tick_lower: int, tick_upper: int, amount: int) -> ExecResult:
        """
        [Phase 2] Add liquidity to Orca/Meteora CLMM.
        Scaffold: requires CLMM instruction builder layout.
        """
        log.warning("clmm_add_liquidity_not_implemented", pool_id=pool_id)
        return ExecResult(success=False, error="CLMM Layout instruction builder required.")

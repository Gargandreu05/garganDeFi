"""
agents/risk_agent.py — Pre-Execution Validation
================================================
Validates that an action is safe to execute BEFORE execution_agent is called.
Acts as a gate — execution_agent never runs without risk_agent approval.
"""

from __future__ import annotations

import os
import structlog
from dataclasses import dataclass, field
from typing import List, Optional, Any

log = structlog.get_logger(__name__)

@dataclass
class RiskResult:
    approved: bool
    reason: str
    already_balanced: bool = False
    sol_to_swap: float = 0.0
    existing_quote_raw: int = 0
    swappable_tokens: List[dict] = field(default_factory=list)

class RiskAgent:
    """
    Validates on-chain actions for risk and balance safety.
    """

    def __init__(self, market_agent: Any) -> None:
        self._market_agent = market_agent
        # Load risk parameters with defaults
        self._gas_reserve = float(os.getenv("GAS_RESERVE_SOL", "0.02"))
        self._min_invest = float(os.getenv("MIN_INVEST_SOL", "0.05"))

    async def validate_migration(
        self, 
        candidate: dict, 
        wallet_snapshot: dict, 
        sol_price: float
    ) -> RiskResult:
        """
        Validates a pool migration proposal.
        
        Checks:
          1. deployable_sol >= MIN_INVEST_SOL
          2. Pool liquidity hasn't dropped > 20% since scan
          3. Wallet already balanced check
          4. candidate pool_id != active pool_id
        """
        log.info("validate_migration_starting", pool_id=candidate.get("pool_id"))

        active_pool_id = os.getenv("ACTIVE_POOL_ID", "")
        if cand_id := candidate.get("pool_id"):
            if cand_id == active_pool_id:
                return RiskResult(
                    approved=False,
                    reason="Candidate pool is already the active pool."
                )

        sol_balance = wallet_snapshot.get("sol_balance", 0.0)
        deployable_sol = sol_balance - self._gas_reserve

        if deployable_sol < self._min_invest:
            return RiskResult(
                approved=False,
                reason=f"Insufficient deployable SOL ({deployable_sol:.4f} < {self._min_invest:.4f} min)."
            )

        # 2. Check Liquidity Drop
        # Fetch current liquidity from market_agent to compare with candidate data
        current_data = await self._market_agent.get_pool_data(candidate["pool_id"])
        if not current_data:
            return RiskResult(
                approved=False,
                reason="Could not verify candidate pool liquidity data."
            )

        scanned_liq = float(candidate.get("liquidity", 0))
        current_liq = float(current_data.get("liquidity_usd", 0))

        if scanned_liq > 0 and current_liq < scanned_liq * 0.80:
            return RiskResult(
                approved=False,
                reason=f"Liquidity dropped > 20% (Scanned: ${scanned_liq:,.0f}, Current: ${current_liq:,.0f})."
            )

        # 3. Already Balanced Check
        from defi_engine.math_engine import compute_zap_amounts
        amounts = compute_zap_amounts(sol_balance, self._gas_reserve)
        sol_to_swap = amounts["sol_to_swap"]

        quote_mint = candidate.get("quote_mint") or os.getenv("ACTIVE_POOL_QUOTE_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
        existing_quote_raw = 0
        existing_quote_ui = 0.0

        for token in wallet_snapshot.get("tokens", []):
            if token.get("mint") == quote_mint:
                existing_quote_raw = int(token.get("amount_raw", 0))
                existing_quote_ui = float(token.get("amount_ui", 0.0))
                break

        # Fix calculations: expected_quote = deployable_sol/2 * sol_price
        # Wait, compute_zap_amounts handles the math.
        # target_quote_raw = sol_to_swap * sol_price * 1_000_000 (if USDC, handles decimals or just compare raw/ui?)
        # Let's use UI amounts for safety or compute raw correctly.
        # USDC has 6 decimals.
        expected_usdc = (deployable_sol / 2) * sol_price
        
        already_balanced = existing_quote_ui >= (expected_usdc * 0.80)

        log.info(
            "validate_migration_checks_passed",
            already_balanced=already_balanced,
            deployable_sol=deployable_sol,
            sol_to_swap=sol_to_swap
        )

        return RiskResult(
            approved=True,
            reason="Migration approved.",
            already_balanced=already_balanced,
            sol_to_swap=sol_to_swap if not already_balanced else 0.0,
            existing_quote_raw=existing_quote_raw
        )

    async def validate_withdraw(self, wallet_snapshot: dict) -> RiskResult:
        """
        Validates a withdraw/exit action.
        Checks that there are actual tokens to swap back to SOL.
        """
        tokens = wallet_snapshot.get("tokens", [])
        swappable = []

        for t in tokens:
            mint = t.get("mint")
            amount_raw = int(t.get("amount_raw", 0))
            amount_ui = float(t.get("amount_ui", 0.0))

            # Skip dust and WSOL
            if amount_raw == 0 or mint == "So11111111111111111111111111111111111111112":
                continue
            if amount_ui < 0.01:
                continue

            swappable.append(t)

        if not swappable:
            return RiskResult(
                approved=False,
                reason="No swappable tokens found. Wallet is already in SOL (or holding only dust)."
            )

        return RiskResult(
            approved=True,
            reason=f"Found {len(swappable)} swappable tokens.",
            swappable_tokens=swappable
        )

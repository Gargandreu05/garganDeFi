"""
defi_engine/math_engine.py — Pure Mathematical DeFi Heuristics
================================================================
No AI, no ML — every function here is a deterministic formula.

Provides:
  • impermanent_loss()   — classic AMM IL formula
  • swap_fee_estimate()  — fee from Raydium pool tier
  • jupiter_swap_cost()  — estimated gas + JUP fee for a swap
  • pool_net_apy()       — IL-adjusted net APY
  • is_migration_profitable() — Boolean decision gate
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import structlog

log = structlog.get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
LAMPORTS_PER_SOL: int = 1_000_000_000
USDC_DECIMALS: int = 6
SOL_DECIMALS: int = 9

# Typical Raydium fee tiers (bps → fraction)
RAYDIUM_FEE_TIERS: dict[str, float] = {
    "standard": 0.0025,   # 0.25 %
    "stable":   0.0001,   # 0.01 %
    "cpmm":     0.003,    # 0.30 %
}

# Average Solana transaction fee (5000 lamports per signature, 2 sigs worst-case)
_BASE_TX_FEE_LAMPORTS: int = 10_000
# Priority fee added by Jupiter swaps (estimated)
_JUPITER_PRIORITY_LAMPORTS: int = 100_000


# ──────────────────────────────────────────────────────────────────────────────
#  Core AMM math
# ──────────────────────────────────────────────────────────────────────────────

def impermanent_loss(price_ratio: float) -> float:
    """
    Standard AMM Impermanent Loss given a price ratio k = P_new / P_old.

    IL = 2 * sqrt(k) / (1 + k) - 1

    Returns a negative fraction (e.g. -0.057 means -5.7 %).
    """
    if price_ratio <= 0:
        raise ValueError(f"price_ratio must be positive, got {price_ratio}")
    sqrt_k = math.sqrt(price_ratio)
    return (2 * sqrt_k / (1 + price_ratio)) - 1


def impermanent_loss_pct(price_ratio: float) -> float:
    """Return IL as a positive percentage (e.g. 5.7 for -5.7 % IL)."""
    return abs(impermanent_loss(price_ratio)) * 100.0


def required_price_ratio_for_il(target_il_pct: float) -> float:
    """
    Return the price-change ratio that would cause `target_il_pct` percent IL.
    Useful for alerting the user when a pool is becoming risky.
    Numerically solved via binary search.
    """
    if target_il_pct <= 0 or target_il_pct >= 100:
        raise ValueError("target_il_pct must be in (0, 100)")
    target = target_il_pct / 100.0
    lo, hi = 1.0, 1000.0
    for _ in range(64):   # 64 iterations is more than enough
        mid = (lo + hi) / 2
        if impermanent_loss_pct(mid) < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


# ── Fee calculations ──────────────────────────────────────────────────────────

def swap_fee_estimate(
    amount_usd: float,
    pool_tier: str = "standard",
) -> float:
    """Estimated LP fee (USD) earned on a single swap of `amount_usd`."""
    rate = RAYDIUM_FEE_TIERS.get(pool_tier, RAYDIUM_FEE_TIERS["standard"])
    return amount_usd * rate


def jupiter_swap_cost_sol(sol_price_usd: float) -> dict[str, float]:
    """
    Estimate total cost of one Jupiter swap in SOL and USD.
    Includes base network fee + typical priority fee.
    """
    total_lamports = _BASE_TX_FEE_LAMPORTS + _JUPITER_PRIORITY_LAMPORTS
    sol_cost = total_lamports / LAMPORTS_PER_SOL
    usd_cost = sol_cost * sol_price_usd
    return {"sol": sol_cost, "usd": usd_cost}


def deposit_cost_sol(sol_price_usd: float) -> dict[str, float]:
    """Estimated cost of the addLiquidity transaction on Raydium."""
    # Raydium deposits typically use 2 signatures (approve + addLiquidity)
    total_lamports = _BASE_TX_FEE_LAMPORTS * 2 + _JUPITER_PRIORITY_LAMPORTS
    sol_cost = total_lamports / LAMPORTS_PER_SOL
    return {"sol": sol_cost, "usd": sol_cost * sol_price_usd}


# ── APY / Yield math ──────────────────────────────────────────────────────────

def annualise_from_7d(fee_7d_usd: float, liquidity_usd: float) -> float:
    """
    Convert 7-day fee revenue to APY percentage.
    APY = (fee_7d / liquidity) * (365 / 7) * 100
    """
    if liquidity_usd <= 0:
        return 0.0
    return (fee_7d_usd / liquidity_usd) * (365 / 7) * 100.0


@dataclass
class PoolMetrics:
    pool_id: str
    pool_name: str
    raw_apy_pct: float          # Reported / computed gross APY
    fee_7d_usd: float
    liquidity_usd: float
    price_now: float            # Current base/quote price
    price_entry: float          # Price at time of deposit (for IL calc)
    sol_price_usd: float
    pool_tier: str = "standard"


def pool_net_apy(metrics: PoolMetrics, capital_usd: float) -> dict[str, float]:
    """
    Calculate IL-adjusted net APY for a given pool position.

    Returns dict with:
      raw_apy_pct, il_pct, fee_7d_usd, annualised_gas_usd, net_apy_pct
    """
    # 1. Impermanent Loss
    if metrics.price_entry > 0 and metrics.price_now > 0:
        ratio = metrics.price_now / metrics.price_entry
        il = impermanent_loss_pct(ratio)
    else:
        il = 0.0

    # 2. Annualised gas (swap + deposit amortised over 365 days)
    gas = (
        jupiter_swap_cost_sol(metrics.sol_price_usd)["usd"]
        + deposit_cost_sol(metrics.sol_price_usd)["usd"]
    )
    annualised_gas_pct = (gas / max(capital_usd, 1e-9)) * 100.0

    # 3. IL as annualised drag — assume IL accrues at entry and is held 365 days
    il_annual_drag_pct = il   # conservative: full IL on first day

    net = metrics.raw_apy_pct - il_annual_drag_pct - annualised_gas_pct

    log.debug(
        "pool_net_apy",
        pool=metrics.pool_name,
        raw_apy=round(metrics.raw_apy_pct, 2),
        il=round(il, 2),
        gas_drag=round(annualised_gas_pct, 4),
        net=round(net, 2),
    )

    return {
        "raw_apy_pct": metrics.raw_apy_pct,
        "il_pct": il,
        "fee_7d_usd": metrics.fee_7d_usd,
        "annualised_gas_usd": gas,
        "net_apy_pct": net,
    }


# ── Decision gate ─────────────────────────────────────────────────────────────

def is_migration_profitable(
    current_net_apy: float,
    candidate_net_apy: float,
    min_improvement_pct: float = 5.0,
    max_il_pct: float = 10.0,
    candidate_il_pct: float = 0.0,
) -> tuple[bool, str]:
    """
    Decide whether migrating to a new pool is mathematically worthwhile.

    Returns (decision: bool, reason: str).
    """
    if candidate_il_pct > max_il_pct:
        return False, (
            f"Candidate IL {candidate_il_pct:.2f}% exceeds max tolerance "
            f"{max_il_pct:.2f}%."
        )

    improvement = candidate_net_apy - current_net_apy
    if improvement < min_improvement_pct:
        return False, (
            f"Net APY improvement {improvement:.2f}% is below threshold "
            f"{min_improvement_pct:.2f}%."
        )

    return True, (
        f"Migration profitable: +{improvement:.2f}% net APY improvement, "
        f"IL within tolerance ({candidate_il_pct:.2f}%)."
    )


# ── Zap sizing ────────────────────────────────────────────────────────────────

def compute_zap_amounts(
    total_sol: float,
    gas_reserve_sol: float = 0.02,
) -> dict[str, float]:
    """
    Given total SOL in wallet, compute how much to:
      • Keep as gas reserve
      • Swap to Quote Token (50 % of deployable)
      • Keep as base token for liquidity pair (50 % of deployable)

    Returns dict with keys: gas_reserve, sol_to_swap, sol_for_base, deployable
    """
    deployable = max(total_sol - gas_reserve_sol, 0.0)
    sol_to_swap = deployable / 2.0
    sol_for_base = deployable / 2.0

    log.debug(
        "zap_sizing",
        total_sol=total_sol,
        gas_reserve=gas_reserve_sol,
        deployable=deployable,
        sol_to_swap=sol_to_swap,
        sol_for_base=sol_for_base,
    )

    return {
        "gas_reserve": gas_reserve_sol,
        "deployable": deployable,
        "sol_to_swap": sol_to_swap,
        "sol_for_base": sol_for_base,
    }

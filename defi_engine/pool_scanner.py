"""
defi_engine/pool_scanner.py — Whitelist-Only Pool Screener
===========================================================
Scans ONLY the hardcoded VIP whitelist every POOL_SCAN_INTERVAL_SECONDS.
Uses Raydium's public API to fetch current APY / volume / fee data.

HITL flow:
  1. Fetch metrics for all whitelist pools.
  2. Compute net APY (IL-adjusted) using math_engine.
  3. If a candidate pool beats the current active one by ≥ threshold AND
     AUTONOMOUS_POOL_SWITCHING=false → emit a Discord alert and pause.
  4. If AUTONOMOUS_POOL_SWITCHING=true → auto-migrate (not default).
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Optional

import aiohttp
import structlog
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from defi_engine.math_engine import (
    PoolMetrics,
    pool_net_apy,
    is_migration_profitable,
    compute_zap_amounts,
)
from ui.database import Database

log = structlog.get_logger(__name__)

# ── Whitelist ─────────────────────────────────────────────────────────────────
# pool_id → human name.  Only these pools will EVER be considered.
POOL_WHITELIST: dict[str, dict] = {
    "58oQChx4yWmvKnVgSTweG8AntmDhmr57fCz5rqVGkS8J": {
        "name": "SOL/USDC",
        "base":  "So11111111111111111111111111111111111111112",
        "quote": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "tier":  "standard",
    },
    "BbZjQanvobx9tEBvSMkAJZBAEwVFjMGxQDf2eMNMEFhw": {
        "name": "JUP/USDC",
        "base":  "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
        "quote": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "tier":  "standard",
    },
    "6UmmUiYoBjSrhakAobJw8BYkpaltEi5I1R1HN4DXqiSo": {
        "name": "RAY/USDC",
        "base":  "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        "quote": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "tier":  "standard",
    },
    "DvDNLHa5UzCFhbSh8TNBEBZZ17VLMV4UkBeqB2FHDXDE": {
        "name": "PYTH/USDC",
        "base":  "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3",
        "quote": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "tier":  "standard",
    },
}

# Raydium public API base (no auth required)
RAYDIUM_API_BASE = "https://api.raydium.io/v2/ammV3/ammPools"
# Raydium V2 pool info endpoint (returns volume, fees, liquidity)
RAYDIUM_POOL_INFO_URL = "https://api.raydium.io/v2/main/pool"


class PoolScanner:
    """Background service that scans whitelist pools and emits HITL alerts."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self._scan_interval = int(os.getenv("POOL_SCAN_INTERVAL_SECONDS", "300"))
        self._min_improvement = float(os.getenv("MIN_APY_IMPROVEMENT_THRESHOLD", "5.0"))
        self._max_il = float(os.getenv("MAX_IL_TOLERANCE_PCT", "10.0"))
        self._autonomous = os.getenv("AUTONOMOUS_POOL_SWITCHING", "false").lower() == "true"
        self._gas_reserve = float(os.getenv("GAS_RESERVE_SOL", "0.02"))

        # Injected by defi_cog after bot is ready
        self.discord_alert_callback: Optional[callable] = None
        # Injected by defi_cog for auto-migration trigger
        self.migration_callback: Optional[callable] = None

        # Current best candidate waiting for human approval
        self._pending_migration: Optional[dict] = None
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run_forever(self) -> None:
        """Main loop — runs until CancelledError."""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "GarganDeFiBot/1.0"},
        )
        log.info("pool_scanner_started", interval_s=self._scan_interval)
        try:
            while True:
                await self._scan_cycle()
                await asyncio.sleep(self._scan_interval)
        except asyncio.CancelledError:
            log.info("pool_scanner_stopping")
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    # ── Scan cycle ────────────────────────────────────────────────────────────

    async def _scan_cycle(self) -> None:
        log.info("pool_scan_starting")
        active_pool_id = os.getenv("ACTIVE_POOL_ID", "")
        results: list[dict] = []

        for pool_id, meta in POOL_WHITELIST.items():
            try:
                data = await self._fetch_pool_data(pool_id)
                if data is None:
                    continue

                raw_apy = data.get("apy", 0.0) or 0.0
                fee_7d = data.get("fee7d", 0.0) or 0.0
                liquidity = data.get("liquidity", 1.0) or 1.0
                price_now = data.get("price", 1.0) or 1.0
                sol_price = await self._get_sol_price()

                metrics = PoolMetrics(
                    pool_id=pool_id,
                    pool_name=meta["name"],
                    raw_apy_pct=raw_apy,
                    fee_7d_usd=fee_7d,
                    liquidity_usd=liquidity,
                    price_now=price_now,
                    price_entry=price_now,   # will be accurate once we store entry price
                    sol_price_usd=sol_price,
                    pool_tier=meta.get("tier", "standard"),
                )

                capital_usd = 30 * sol_price  # ~$30 of SOL
                net = pool_net_apy(metrics, capital_usd)

                row_id = await self._db.insert_pool_evaluation(
                    pool_id=pool_id,
                    pool_name=meta["name"],
                    apy_pct=raw_apy,
                    fee_7d_usd=fee_7d,
                    il_pct=net["il_pct"],
                    net_apy_pct=net["net_apy_pct"],
                    recommended=False,
                    raw_json=json.dumps(data),
                )

                results.append({
                    "pool_id": pool_id,
                    "name": meta["name"],
                    "net_apy": net["net_apy_pct"],
                    "il_pct": net["il_pct"],
                    "raw_apy": raw_apy,
                    "db_id": row_id,
                })

                log.info(
                    "pool_evaluated",
                    pool=meta["name"],
                    raw_apy=round(raw_apy, 2),
                    net_apy=round(net["net_apy_pct"], 2),
                    il=round(net["il_pct"], 2),
                )

            except Exception as exc:
                log.error("pool_eval_error", pool=meta["name"], error=str(exc))
                continue

        await self._check_and_alert(active_pool_id, results)

    async def _check_and_alert(self, active_pool_id: str, results: list[dict]) -> None:
        if not results:
            return

        best = max(results, key=lambda r: r["net_apy"])
        current = next((r for r in results if r["pool_id"] == active_pool_id), None)

        if current is None:
            log.warning("active_pool_not_in_results", active_pool_id=active_pool_id)
            return

        current_net = current["net_apy"]
        best_net = best["net_apy"]

        profitable, reason = is_migration_profitable(
            current_net_apy=current_net,
            candidate_net_apy=best_net,
            min_improvement_pct=self._min_improvement,
            max_il_pct=self._max_il,
            candidate_il_pct=best["il_pct"],
        )

        if not profitable:
            log.info("no_migration_needed", reason=reason)
            return

        # Store pending migration proposal
        self._pending_migration = best
        log.info("migration_candidate_found", pool=best["name"], net_apy=round(best_net, 2))

        if self._autonomous:
            log.warning("autonomous_migration_triggered", pool=best["name"])
            if self.migration_callback:
                await self.migration_callback(best)
        else:
            # HITL: send Discord alert and wait for !approve_migration
            if self.discord_alert_callback:
                await self.discord_alert_callback(current, best, reason)

    # ── API helpers ───────────────────────────────────────────────────────────

    async def _fetch_pool_data(self, pool_id: str) -> Optional[dict]:
        """Fetch pool info from Raydium public API with retries."""
        url = f"{RAYDIUM_POOL_INFO_URL}/{pool_id}"
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(4),
                wait=wait_exponential(multiplier=1, min=2, max=30),
                retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
                reraise=True,
            ):
                with attempt:
                    async with self._session.get(url) as resp:
                        if resp.status == 404:
                            log.warning("pool_not_found_in_api", pool_id=pool_id)
                            return None
                        resp.raise_for_status()
                        return await resp.json()
        except Exception as exc:
            log.error("fetch_pool_data_failed", pool_id=pool_id, error=str(exc))
            return None

    async def _get_sol_price(self) -> float:
        """Fetch SOL/USD price from Jupiter price API."""
        url = "https://price.jup.ag/v4/price?ids=SOL"
        try:
            async with self._session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return float(data["data"]["SOL"]["price"])
        except Exception as exc:
            log.error("sol_price_fetch_failed", error=str(exc))
            return 150.0  # Fallback to conservative estimate

    # ── Public accessors ──────────────────────────────────────────────────────

    def get_pending_migration(self) -> Optional[dict]:
        return self._pending_migration

    def clear_pending_migration(self) -> None:
        self._pending_migration = None

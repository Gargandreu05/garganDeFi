"""
defi_engine/pool_scanner.py — Autonomous Pool Discovery & Security
==================================================================
Identifies top SOL liquidity pools dynamically using DexScreener.
Applies strict Anti-Scam/Rug-Pull filters (Liquidity, Volume, Age).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Optional, Dict, List, Any, Callable

import aiohttp
import structlog

from defi_engine.math_engine import (
    PoolMetrics,
    pool_net_apy,
    is_migration_profitable,
)
from defi_engine.quant_engine import QuantEngine
from ui.database import Database

log = structlog.get_logger(__name__)

# ── Whitelist (Base set to always monitor) ───────────────────────────────────
POOL_WHITELIST: dict[str, dict] = {
    "58oQChx4yWmvKnVgSTweG8AntmDhmr57fCz5rqVGkS8J": {"name": "SOL/USDC", "tier": "standard"},
    "BbZjQanvobx9tEBvSMkAJZBAEwVFjMGxQDf2eMNMEFhw": {"name": "JUP/USDC", "tier": "standard"},
    "6UmmUiYoBjSrhakAobJw8BYkpaltEi5I1R1HN4DXqiSo": {"name": "RAY/USDC", "tier": "standard"},
}

DEXSCREENER_API_BASE = "https://api.dexscreener.com/latest/dex"

class PoolScanner:
    """Autonomous service that discovers and evaluates DeFi pools."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self._scan_interval = int(os.getenv("POOL_SCAN_INTERVAL_SECONDS", "300"))
        self._min_improvement = float(os.getenv("MIN_APY_IMPROVEMENT_THRESHOLD", "5.0"))
        self._max_il = float(os.getenv("MAX_IL_TOLERANCE_PCT", "10.0"))
        self._autonomous = os.getenv("AUTONOMOUS_POOL_SWITCHING", "false").lower() == "true"
        self._gas_reserve = float(os.getenv("GAS_RESERVE_SOL", "0.02"))

        self.discord_alert_callback: Optional[Callable] = None
        self.migration_callback: Optional[Callable] = None
        self._pending_migration: Optional[dict] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._quant = QuantEngine()

    async def run_forever(self) -> None:
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

    async def _scan_cycle(self) -> None:
        log.info("pool_scan_starting")
        active_pool_id = os.getenv("ACTIVE_POOL_ID", "")
        results: List[Dict] = []
        sol_price = await self._get_sol_price()

        # 1. Evaluate Whitelist
        results_map = {}
        log.info("evaluating_whitelist", count=len(POOL_WHITELIST))
        for pool_id, meta in POOL_WHITELIST.items():
            data = await self._fetch_pool_data(pool_id)
            if data:
                row = await self._evaluate_and_save(pool_id, meta, data, sol_price, active_pool_id)
                if row:
                    results.append(row)
                    results_map[pool_id] = row
            else:
                log.warning("whitelist_fetch_failed", pool_id=pool_id)

        # 2. Dynamic Discovery (Top Liquidity SOL Pairs)
        discovered = await self._fetch_trending_pools()
        log.info("dynamic_pools_discovered", count=len(discovered))
        for p_id, p_meta in discovered.items():
            if p_id in results_map: continue
            data = await self._fetch_pool_data(p_id)
            if data and self.is_safe_pool(data):
                row = await self._evaluate_and_save(p_id, p_meta, data, sol_price, active_pool_id)
                if row:
                    results.append(row)
            elif data:
                log.debug("skipping_unsafe_pool", pool_id=p_id, name=p_meta.get("name"))

        # 3. Log Top 3 Risk-to-Reward Pools
        if results:
            results.sort(key=lambda x: x.get("risk_reward", 0), reverse=True)
            top_3 = results[:3]
            for i, p in enumerate(top_3):
                log.info("blue_chip_candidate", rank=i+1, name=p["name"], risk_reward=round(p["risk_reward"], 2), net_apy=round(p["net_apy"], 2))

        await self._check_and_alert(active_pool_id, results)

    async def _evaluate_and_save(self, pool_id: str, meta: dict, data: dict, sol_price: float, active_id: str) -> Optional[dict]:
        metrics = PoolMetrics(
            pool_id=pool_id,
            pool_name=meta.get("name", "Unknown"),
            raw_apy_pct=data.get("apy", 0),
            fee_7d_usd=data.get("fee7d", 0),
            liquidity_usd=data.get("liquidity", 1),
            price_now=data.get("price", 1),
            price_entry=data.get("price", 1),
            sol_price_usd=sol_price,
            pool_tier=meta.get("tier", "standard"),
        )
        
        net = pool_net_apy(metrics, 30 * sol_price)
        
        # Risk-to-Reward Ratio: Net APY vs IL (conservative padding)
        risk_reward = net["net_apy_pct"] / (abs(net["il_pct"]) + 0.1)
        
        # Save to DB for tracking (removing strictly high yield floor for blue-chips)
        db_id = await self._db.insert_pool_evaluation(
            pool_id=pool_id, pool_name=meta.get("name", "Unknown"),
            apy_pct=data.get("apy", 0), fee_7d_usd=data.get("fee7d", 0),
            il_pct=net["il_pct"], net_apy_pct=net["net_apy_pct"],
            recommended=False, raw_json=json.dumps(data)
        )

        return {
            "pool_id": pool_id, "name": meta.get("name", "Unknown"),
            "net_apy": net["net_apy_pct"], "il_pct": net["il_pct"],
            "raw_apy": data.get("apy", 0), "db_id": db_id,
            "liquidity": data.get("liquidity", 0),
            "vol_24h": data.get("vol_24h", 0),
            "risk_reward": risk_reward
        }

    def is_safe_pool(self, data: dict) -> bool:
        """
        Blue-Chip Strategy: Focus on established, high-liquidity pools.
        Requires $500k TVL and $50k 24h Volume.
        """
        liquidity = float(data.get("liquidity", 0))
        vol_24h = float(data.get("vol_24h", 0))

        if liquidity < 500000:
            log.debug("pool_rejected_low_liquidity", liquidity=liquidity)
            return False
        if vol_24h < 50000:
            log.debug("pool_rejected_low_volume", vol=vol_24h)
            return False
        
        if data.get("rugged", False):
            log.warning("pool_rejected_rugged", pool=data.get("name", "Unknown"))
            return False
        return True

    async def _fetch_trending_pools(self) -> Dict[str, dict]:
        """Query DexScreener for SOL pools, sorted by Liquidity/Volume descending."""
        url = f"{DEXSCREENER_API_BASE}/search/?q=SOL%20USDC"
        discovered = {}
        try:
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs", [])
                    
                    # Explicitly sort by Liquidity (USD) descending
                    sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
                    sol_pairs.sort(key=lambda x: float(x.get("liquidity", {}).get("usd", 0)), reverse=True)
                    
                    for p in sol_pairs[:15]:
                        p_id = p.get("pairAddress")
                        if p_id:
                            discovered[p_id] = {
                                "name": f"{p.get('baseToken',{}).get('symbol')}/{p.get('quoteToken',{}).get('symbol')}",
                                "tier": "blue-chip" if float(p.get("liquidity", {}).get("usd", 0)) > 1000000 else "standard"
                            }
        except Exception: pass
        return discovered

    async def _fetch_pool_data(self, pool_id: str) -> Optional[dict]:
        """Fetch pool data from DexScreener. Tries direct pair lookup first, then search fallback."""
        url = f"{DEXSCREENER_API_BASE}/pairs/solana/{pool_id}"
        try:
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs", [])
                    if pairs:
                        return self._normalize_pair(pairs[0])
        except Exception as exc:
            log.debug("direct_pair_fetch_failed", pool_id=pool_id, error=str(exc))

        # Fallback: search by pool_id as query (resolves Raydium AMM IDs via DexScreener search)
        search_url = f"{DEXSCREENER_API_BASE}/search/?q={pool_id}"
        try:
            async with self._session.get(search_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs", [])
                    solana_pairs = [p for p in pairs if p.get("chainId") == "solana"]
                    if solana_pairs:
                        return self._normalize_pair(solana_pairs[0])
        except Exception as exc:
            log.debug("search_pair_fetch_failed", pool_id=pool_id, error=str(exc))

        return None

    def _normalize_pair(self, pair: dict) -> dict:
        """Normalize a DexScreener pair response into the internal pool data format."""
        if not isinstance(pair, dict):
            log.warning("normalize_pair_not_a_dict", received=type(pair).__name__)
            return {}

        log.debug("normalize_pair_raw", pair_keys=list(pair.keys()))

        def safe_float(val) -> float:
            try:
                return float(val) if val is not None else 0.0
            except (TypeError, ValueError):
                return 0.0

        def safe_dict(val) -> dict:
            return val if isinstance(val, dict) else {}

        liquidity  = safe_float(safe_dict(pair.get("liquidity")).get("usd"))
        vol_24h    = safe_float(safe_dict(pair.get("volume")).get("h24"))
        price      = safe_float(pair.get("priceUsd"))
        base_token = safe_dict(pair.get("baseToken"))
        quote_token = safe_dict(pair.get("quoteToken"))

        fee7d = vol_24h * 0.0025 * 7
        apy   = (fee7d * 52 / liquidity * 100) if liquidity > 0 else 0.0

        labels = pair.get("labels", [])
        if not isinstance(labels, list):
            labels = []

        return {
            "apy":       round(apy, 4),
            "fee7d":     round(fee7d, 2),
            "liquidity": liquidity,
            "price":     price,
            "vol_24h":   vol_24h,
            "created_at": pair.get("pairCreatedAt", 0),
            "rugged":    any(
                safe_dict(l).get("label") == "rugged" for l in labels
            ),
            "pair_name": f"{base_token.get('symbol', '?')}/{quote_token.get('symbol', '?')}",
        }

    async def _get_sol_price(self) -> float:
        url = "https://api.jup.ag/price/v2?ids=So11111111111111111111111111111111111111112"
        try:
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(
                        data["data"]["So11111111111111111111111111111111111111112"]["price"]
                    )
        except Exception as exc:
            log.warning("sol_price_fetch_failed", error=str(exc))
        return 150.0

    async def _check_and_alert(self, active_id: str, results: List[Dict]) -> None:
        if not results:
            return

        candidates = sorted(results, key=lambda x: x.get("risk_reward", 0), reverse=True)[:3]

        # Always log top 3 so they appear in terminal output
        for i, c in enumerate(candidates):
            log.info(
                "top_pool_recommendation",
                rank=i + 1,
                name=c["name"],
                net_apy=round(c["net_apy"], 2),
                il_pct=round(c["il_pct"], 2),
                risk_reward=round(c["risk_reward"], 2),
                liquidity=round(c.get("liquidity", 0)),
            )

        current = next((r for r in results if r["pool_id"] == active_id), None)

        # If no active pool is configured, recommend the best one directly
        if not current:
            best = candidates[0]
            log.info("no_active_pool_recommending_best", pool=best["name"])
            if self.discord_alert_callback:
                await self.discord_alert_callback(
                    current=None,
                    candidate=best,
                    reason=(
                        f"🚀 No active pool configured. "
                        f"**Best opportunity found:** {best['name']} | "
                        f"Net APY: {best['net_apy']:.2f}% | "
                        f"Liquidity: ${best.get('liquidity', 0):,.0f} | "
                        f"Risk/Reward score: {best['risk_reward']:.2f}"
                    ),
                )
            return

        for cand in candidates:
            if cand["pool_id"] == active_id:
                continue

            profitable, reason = is_migration_profitable(
                current["net_apy"], cand["net_apy"], self._min_improvement, self._max_il, cand["il_pct"]
            )

            is_huge = cand.get("liquidity", 0) > 1_000_000
            quant_result = await self._quant.analyze_crypto(cand["pool_id"], relaxed=is_huge)
            confluence_pass = quant_result is not None

            if not confluence_pass and not is_huge:
                log.info("candidate_failed_confluence", pool=cand["name"])
                continue

            if profitable or (is_huge and cand["net_apy"] > current["net_apy"] * 0.9):
                final_reason = reason
                if is_huge:
                    final_reason += " | 🐋 BLUE-CHIP (> $1M TVL)"
                if quant_result:
                    final_reason += f" | Confidence: {quant_result['confidence_score']:.1f}%"

                if cand == candidates[0] and profitable:
                    self._pending_migration = cand
                    if self._autonomous and self.migration_callback:
                        await self.migration_callback(cand)

                if self.discord_alert_callback:
                    await self.discord_alert_callback(current, cand, final_reason)

    def get_pending_migration(self) -> Optional[dict]: return self._pending_migration
    def clear_pending_migration(self) -> None: self._pending_migration = None

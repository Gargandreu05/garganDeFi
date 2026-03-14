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
        url = f"{DEXSCREENER_API_BASE}/pairs/solana/{pool_id}"
        try:
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    pair = (await resp.json()).get("pairs", [{}])[0]
                    if not pair: return None
                    return {
                        "apy": float(pair.get("apr", 0) or 0),
                        "fee7d": float(pair.get("volume", {}).get("h24", 0)) * 0.0025 * 7,
                        "liquidity": float(pair.get("liquidity", {}).get("usd", 0)),
                        "price": float(pair.get("priceUsd", 0)),
                        "vol_24h": float(pair.get("volume", {}).get("h24", 0)),
                        "created_at": pair.get("pairCreatedAt", 0),
                        "rugged": any(l.get("label") == "rugged" for l in pair.get("labels", []))
                    }
        except Exception: pass
        return None

    async def _get_sol_price(self) -> float:
        url = "https://price.jup.ag/v4/price?ids=SOL"
        try:
            async with self._session.get(url) as resp:
                data = await resp.json()
                return float(data["data"]["SOL"]["price"])
        except Exception: return 150.0

    async def _check_and_alert(self, active_id: str, results: List[Dict]) -> None:
        """
        Evaluate candidates and send alerts.
        Now alerts for top 3 risk-to-reward pools and relaxes confluence for Blue-Chips.
        """
        if not results: return
        
        # Sort by Risk-to-Reward to find Top 3 "Blue-Chip" candidates
        candidates = sorted(results, key=lambda x: x.get("risk_reward", 0), reverse=True)[:3]
        
        current = next((r for r in results if r["pool_id"] == active_id), None)
        if not current: return

        for cand in candidates:
            # 1. Skip if already in this pool
            if cand["pool_id"] == active_id: continue

            # 2. Check Migration Profitability
            profitable, reason = is_migration_profitable(
                current["net_apy"], cand["net_apy"], self._min_improvement, self._max_il, cand["il_pct"]
            )
            
            # 3. Technical Confluence (Relaxed if TVL > $1M)
            is_huge = cand.get("liquidity", 0) > 1000000
            quant_result = await self._quant.analyze_crypto(cand["pool_id"], relaxed=is_huge)
            
            confluence_pass = quant_result is not None
            
            # THE RELAXATION RULE: 
            # If the pool is huge, we lower the bar to ensure the user sees the analysis.
            if not confluence_pass and not is_huge:
                log.info("candidate_failed_confluence", pool=cand["name"], liquidity=cand.get("liquidity"))
                continue
            elif not confluence_pass and is_huge:
                log.info("blue_chip_low_confluence_but_showing", pool=cand["name"])

            # 4. Alert / Propose
            if profitable or (is_huge and cand["net_apy"] > current["net_apy"] * 0.9):
                final_reason = reason
                if is_huge:
                    final_reason += " | 🐋 BLUE-CHIP (> $1M)"
                if quant_result:
                    final_reason += f" | Technical Confidence: {quant_result['confidence_score']}%"

                # Only set the absolute best as the "pending" one for buttons/auto
                if cand == candidates[0] and profitable:
                    self._pending_migration = cand
                    if self._autonomous and self.migration_callback:
                        await self.migration_callback(cand)
                
                # Alert for all interesting high-liquidity detections
                if self.discord_alert_callback:
                    await self.discord_alert_callback(current, cand, final_reason)

    def get_pending_migration(self) -> Optional[dict]: return self._pending_migration
    def clear_pending_migration(self) -> None: self._pending_migration = None

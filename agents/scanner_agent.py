"""
agents/scanner_agent.py — Pool Discovery & Safety Filtering
=============================================================
Discovers and ranks candidate pools based on liquidity, volume, and risk-reward.
"""

from __future__ import annotations

import asyncio
import os
import structlog
from typing import List, Dict, Optional, Any

from defi_engine.math_engine import PoolMetrics, pool_net_apy

try:
    import core_math as _math
except ImportError:
    _math = None

log = structlog.get_logger(__name__)

# ── Whitelist (Base set to always monitor) ───────────────────────────────────
POOL_WHITELIST: dict[str, dict] = {
    "58oQChx4yWmvKnVgSTweG8AntmDhmr57fCz5rqVGkS8J": {"name": "SOL/USDC", "tier": "standard"},
    "BbZjQanvobx9tEBvSMkAJZBAEwVFjMGxQDf2eMNMEFhw": {"name": "JUP/USDC", "tier": "standard"},
    "6UmmUiYoBjSrhakAobJw8BYkpaltEi5I1R1HN4DXqiSo": {"name": "RAY/USDC", "tier": "standard"},
}

class ScannerAgent:
    """
    Discovers, filters, and ranks DeFi pools.
    """

    def __init__(self, market_agent: Any, db: Any) -> None:
        self._market_agent = market_agent
        self._db = db
        # Loading configurations
        self._min_liquidity = float(os.getenv("MIN_LIQUIDITY_USD", "500000"))
        self._min_vol_24h = float(os.getenv("MIN_VOL_USD", "50000"))

    async def discover_pools(self) -> List[dict]:
        """
        Main entry point to get ranked candidates.
        Fetches whitelist + trending pools, filters of unsafe, and scores.
        """
        log.info("discover_pools_starting")
        sol_price = await self._market_agent.get_sol_price_usd()
        candidates = []
        
        # 0. Check only stable config
        only_stable = os.getenv("ONLY_STABLE_FARMING", "false").lower() == "true"

        # 1. Process Whitelist
        for pool_id, meta in POOL_WHITELIST.items():
            if only_stable and not self._is_stable_or_calculated(meta.get("name", "")):
                 log.debug("whitelist_skipped_not_stable", pool=meta.get("name"))
                 continue
                 
            pool_data = await self._market_agent.get_pool_data(pool_id)
            if pool_data:
                scored = await self._score_pool(pool_id, pool_data, sol_price, meta.get("tier", "standard"))
                log.debug("whitelist_pool_scored_result", pool=meta.get("name"), exists=bool(scored))
                if scored:
                    candidates.append(scored)
            else:
                log.debug("whitelist_pool_data_empty", pool_id=pool_id)

        # 2. Process Trending / Top Pools
        top_pools = await self._fetch_trending_pools()
        for pool_id, meta in top_pools.items():
            if only_stable and not self._is_stable_or_calculated(meta.get("name", "")):
                 continue
            if any(c["pool_id"] == pool_id for c in candidates):
                 continue  # Skip already added (whitelist)

            pool_data = await self._market_agent.get_pool_data(pool_id)
            if pool_data:
                safe = await self._is_safe(pool_data)
                log.debug("trending_pool_safety_check", pool=meta.get("name"), safe=safe)
                if safe:
                    scored = await self._score_pool(pool_id, pool_data, sol_price, meta.get("tier", "standard"))
                    if scored:
                        candidates.append(scored)

        # 3. Sort Candidates by score descending
        candidates.sort(key=lambda x: x.get("risk_reward", 0), reverse=True)
        
        # 4. Phase 2.3: Verify Shortlist with RugCheck
        verified = []
        for c in candidates[:5]:
             base_mint = c.get("base_token")
             if base_mint:
                  is_rug_safe = await self._verify_rug_score(base_mint)
                  if not is_rug_safe:
                       log.warning("rugcheck_failed", pool_name=c.get("name"), mint=base_mint)
                       continue
             verified.append(c)

        log.info("discover_pools_completed", count=len(verified))
        return verified

    async def _is_safe(self, data: dict) -> bool:
        """Apply filters: liquidity, volume, not rugged, rugcheck."""
        liquidity = float(data.get("liquidity_usd", 0))
        vol_24h = float(data.get("vol_24h", 0))

        log.debug("is_safe_metrics", liquidity=liquidity, min_liq=self._min_liquidity, vol_24h=vol_24h, min_vol=self._min_vol_24h)

        if liquidity < self._min_liquidity:
            return False
        if vol_24h < self._min_vol_24h:
            return False
        if data.get("rugged", False):
            return False

        # Phase 2.3: Rugcheck Auditor (Removed from _is_safe and moved to shortlist for performance)
        return True

    async def _score_pool(self, pool_id: str, data: dict, sol_price: float, tier: str) -> Optional[dict]:
        """Calculate net APY and risk-reward score for a pool."""
        try:
            metrics = PoolMetrics(
                pool_id=pool_id,
                pool_name=data.get("name", "Unknown"),
                raw_apy_pct=data.get("apy", 0),
                fee_7d_usd=data.get("vol_24h", 0) * 0.0025 * 7,  # rough 24h multiplier estimate
                liquidity_usd=data.get("liquidity_usd", 1),
                price_now=1.0,  # placeholder or fetched
                price_entry=1.0,
                sol_price_usd=sol_price,
                pool_tier=tier,
            )

            # Calculate Net APY
            if _math and hasattr(_math, 'calculate_net_apy'):
                # C++ logic scaffold fallback: not strictly available yet, but uses structure
                # net_apy = _math.calculate_net_apy(...)
                net = pool_net_apy(metrics, 30 * sol_price)
            else:
                net = pool_net_apy(metrics, 30 * sol_price)

            net_apy = net["net_apy_pct"]
            il_pct = net["il_pct"]

            # Base Risk-Reward: net_apy / (il_pct + factor)
            risk_reward = net_apy / (abs(il_pct) + 0.1)

            # Phase 2.2: RELIABILITY BOOST (TVL & Volume modifier)
            # Favors larger pools which are less volatile and more secure (reliable)
            import math
            liquidity = float(data.get("liquidity_usd", 100000))
            vol_24h = float(data.get("vol_24h", 10000))
            
            # log10(100k) is 5. 1.0 multiplier at 100k TVL, 1.2 at 1M TVL, etc.
            tvl_boost = math.log10(max(liquidity, 100000)) / 5.0 
            vol_boost = math.log10(max(vol_24h, 10000)) / 4.0
            
            risk_reward = risk_reward * tvl_boost * vol_boost

            # Save evaluation status to DB
            await self._db.insert_pool_evaluation(
                pool_id=pool_id,
                pool_name=data.get("name", "Unknown"),
                apy_pct=data.get("apy", 0),
                fee_7d_usd=data.get("vol_24h", 0) * 0.0025 * 7,
                il_pct=il_pct,
                net_apy_pct=net_apy,
                recommended=False,
                raw_json=""
            )

            return {
                "pool_id": pool_id,
                "name": data.get("name", "Unknown"),
                "net_apy": net_apy,
                "il_pct": il_pct,
                "risk_reward": risk_reward,
                "liquidity": data.get("liquidity_usd", 0),
                "vol_24h": data.get("vol_24h", 0),
                "quote_mint": data.get("quote_token")
            }

        except Exception as exc:
            log.error("score_pool_failed", pool_id=pool_id, error=str(exc))
            return None

    async def _fetch_trending_pools(self) -> Dict[str, dict]:
        """Query DexScreener for top SOL pools."""
        # This mirrors pool_scanner.py _fetch_trending_pools
        # Using market_agent session or initiating lookup.
        # But market_agent does single pool lookup.
        # ScannerAgent needs general list. 
        # I'll create a session inside ScannerAgent to do the list query if needed,
        # OR market_agent should have get_trending_pools?
        # Specification says "Pulls from DexScreener boosted + top SOL pools"
        # I will fetch from direct search endpoint in ScannerAgent or add it to MarketAgent.
        # Prompt says: ScannerAgent "Pulls from DexScreener boosted... Uses market_agent for price data".
        # This implies ScannerAgent can do it.
        # I'll create a local full list fetch.
        
        discovered = {}
        try:
            import aiohttp
            tags_raw = os.getenv("SEARCH_TAGS", "SOL,USDC")
            queries = [q.strip() for q in tags_raw.split(",") if q.strip()]
            
            if not hasattr(self, "_jupiter_mints") or not self._jupiter_mints:
                self._jupiter_mints = await self._fetch_jupiter_strict_mints()

            async with aiohttp.ClientSession() as session:
                all_pairs = []
                for q in queries:
                     url = f"https://api.dexscreener.com/latest/dex/search/?q={q}"
                     async with session.get(url) as resp:
                          if resp.status == 200:
                               data = await resp.json()
                               all_pairs.extend(data.get("pairs", []) or [])
                               
                seen_pairs = set()
                pairs = []
                for p in all_pairs:
                     p_id = p.get("pairAddress")
                     if p_id and p_id not in seen_pairs:
                          seen_pairs.add(p_id)
                          pairs.append(p)

                filtered = []
                for p in pairs:
                     base_mint = p.get("baseToken", {}).get("address")
                     quote_mint = p.get("quoteToken", {}).get("address")
                     if self._jupiter_mints:
                          if base_mint not in self._jupiter_mints and quote_mint not in self._jupiter_mints:
                               continue
                     filtered.append(p)

                sol_pairs = [p for p in filtered if p.get("chainId") == "solana" and p.get("dexId", "").lower() in ["raydium", "meteora"] and "CLMM" not in p.get("labels", [])]
                sol_pairs.sort(key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0), reverse=True)

                for p in sol_pairs[:20]:
                    p_id = p.get("pairAddress")
                    if p_id:
                        discovered[p_id] = {
                            "name": f"{p.get('baseToken', {}).get('symbol', '?')}/{p.get('quoteToken', {}).get('symbol', '?')}",
                            "tier": "blue-chip" if float(p.get("liquidity", {}).get("usd", 0) or 0) > 1000000 else "standard",
                            "base_token": p.get("baseToken", {}).get("address"),
                            "quote_token": p.get("quoteToken", {}).get("address"),
                            "liquidity": float(p.get("liquidity", {}).get("usd", 0) or 0),
                            "volume_24h": float(p.get("volume", {}).get("h24", 0) or 0),
                            "dex_url": p.get("url", ""),
                            "dex_id": p.get("dexId", "")
                        }
        except Exception as exc:
            log.warning("fetch_trending_pools_failed", error=str(exc))

        return discovered

    def _is_stable_or_calculated(self, pool_name: str) -> bool:
        """
        Check if pool is stablecoin (USDC/USDT) or correlated LST (SOL/mSOL).
        """
        name = pool_name.upper()
        # Stablecoins
        stables = ["USDC", "USDT"]
        # If BOTH tokens are in stables list
        parts = name.split("/")
        if len(parts) == 2:
            if parts[0] in stables and parts[1] in stables:
                return True
        
        # Liquid Staking
        lsts = ["MSOL", "BSOL", "JITOSOL", "STSOL", "INF", "SOL"]
        if len(parts) == 2:
            if parts[0] in lsts and parts[1] in lsts:
                 return True
                 
        return False

    async def _verify_rug_score(self, mint: str) -> bool:
        """
        Consult RugCheck.xyz API for safety report.
        """
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                url = f"https://api.rugcheck.xyz/v1/tokens/{mint}/report"
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        score = data.get("score", 0)
                        log.info("rugcheck_score_fetched", mint=mint, score=score)
                        
                        if score > 1000:
                             return False
                        
                        risks = data.get("risks", [])
                        for risk in risks:
                             if risk.get("level") == "danger" or "freeze" in risk.get("name", "").lower():
                                  return False
                                  
                        return True
        except Exception as exc:
             log.warning("rugcheck_lookup_failed", mint=mint, error=str(exc))
             
              
        return True

    async def _fetch_jupiter_strict_mints(self) -> set[str]:
        """Fetch strict list of token mints from Jupiter for safety verification."""
        try:
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession() as session:
                url = "https://token.jup.ag/strict"
                async with session.get(url, timeout=timeout) as resp:
                     if resp.status == 200:
                          data = await resp.json()
                          mints = {t.get("address") for t in data if t.get("address")}
                          log.info("jupiter_strict_list_fetched", count=len(mints))
                          return mints
        except Exception as exc:
            log.warning("jupiter_strict_list_fetch_failed", error=str(exc))
        return set()

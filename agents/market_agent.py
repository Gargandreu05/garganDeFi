"""
agents/market_agent.py — Market Data and Wallet Snapshots
===========================================================
Fetches and caches current market prices and wallet balances.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Optional, Dict, Any

import aiohttp
import structlog
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
from solana.rpc.types import TokenAccountOpts

log = structlog.get_logger(__name__)

RAYDIUM_POOL_ACCOUNTS_URL = "https://api.raydium.io/v2/sdk/liquidity/mainnet.json"

class MarketAgent:
    """
    Fetches market prices, pool data, and wallet snapshot.
    caches SOL price for 60s.
    """

    def __init__(self) -> None:
        self._sol_price_cache: Optional[float] = None
        self._sol_price_time: float = 0
        self._raydium_list_cache: Optional[dict] = None
        self._raydium_list_time: float = 0
        self._session: Optional[aiohttp.ClientSession] = None

    async def get_sol_price_usd(self) -> float:
        """
        Get current SOL price in USD via Jupiter Quote 1 SOL -> USDC.
        Caches result for 60s.
        """
        now = time.time()
        if self._sol_price_cache and (now - self._sol_price_time) < 60:
            return self._sol_price_cache

        log.debug("fetching_sol_price")
        if not self._session:
            self._session = aiohttp.ClientSession()

        try:
            # Jupiter Quote: 1 SOL -> USDC (EPj...1v)
            url = "https://api.jup.ag/swap/v1/quote"
            params = {
                "inputMint": "So11111111111111111111111111111111111111112",
                "outputMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "amount": "1000000000",  # 1 SOL
                "slippageBps": "50"
            }
            async with self._session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    out_amount = int(data.get("outAmount", 0))
                    price = out_amount / 1_000_000  # USDC has 6 decimals
                    self._sol_price_cache = price
                    self._sol_price_time = now
                    return price
        except Exception as exc:
            log.warning("sol_price_jupiter_failed", error=str(exc))

        # Fallback to general price v2 API if quote fails
        try:
            url = "https://api.jup.ag/price/v2?ids=So11111111111111111111111111111111111111112"
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = float(data["data"]["So11111111111111111111111111111111111111112"]["price"])
                    self._sol_price_cache = price
                    self._sol_price_time = now
                    return price
        except Exception as exc:
            log.error("sol_price_fallback_failed", error=str(exc))

        return self._sol_price_cache or 150.0  # Safe fallback

    async def get_pool_data(self, pool_id: str) -> Optional[dict]:
        """
        Fetch pool data from DexScreener and supplement with Raydium list for lp_mint.
        Returns {apy, liquidity_usd, vol_24h, base_token, quote_token, lp_mint}
        """
        log.debug("fetching_pool_data", pool_id=pool_id)
        if not self._session:
            self._session = aiohttp.ClientSession()

        # DexScreener single pair endpoint can be flaky with null pairs, so we use search layout
        dex_url = f"https://api.dexscreener.com/latest/dex/search/?q={pool_id}"
        dex_data = None

        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with self._session.get(dex_url, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs", [])
                    if pairs:
                        # Find the exact match by pairAddress just in case
                        for p in pairs:
                             if p.get("pairAddress") == pool_id:
                                  dex_data = p
                                  break
                        if not dex_data:
                             dex_data = pairs[0]  # Fallback
        except Exception as exc:
            log.warning("dexscreener_fetch_failed", pool_id=pool_id, error=str(exc))

        if not dex_data:
            return None

        # Extract DexScreener data
        liquidity = float(dex_data.get("liquidity", {}).get("usd", 0) or 0)
        vol_24h = float(dex_data.get("volume", {}).get("h24", 0) or 0)
        base_token = dex_data.get("baseToken", {}).get("address")
        quote_token = dex_data.get("quoteToken", {}).get("address")

        # Compute APY estimate (DexScreener doesn't provide it directly in same way always)
        fee7d = vol_24h * 0.0025 * 7
        apy = (fee7d * 52 / liquidity * 100) if liquidity > 0 else 0.0

        lp_mint = ""
        # Supplement with Raydium list for lp_mint
        raydium_pool = await self._get_raydium_pool_item(pool_id)
        if raydium_pool:
            lp_mint = raydium_pool.get("lpMint", "")

        return {
            "pool_id": pool_id,
            "name": f"{dex_data.get('baseToken', {}).get('symbol', '?')}/{dex_data.get('quoteToken', {}).get('symbol', '?')}",
            "apy": round(apy, 4),
            "liquidity_usd": liquidity,
            "vol_24h": vol_24h,
            "base_token": base_token,
            "quote_token": quote_token,
            "lp_mint": lp_mint
        }

    async def get_wallet_snapshot(self, rpc: AsyncClient, keypair: Any) -> dict:
        """
        Get wallet SOL balance and parsed token accounts.
        Returns {sol_balance, tokens: [{mint, amount_raw, amount_ui}]}
        """
        log.debug("fetching_wallet_snapshot", pubkey=str(keypair.pubkey()))
        
        try:
            sol_resp = await rpc.get_balance(keypair.pubkey())
            sol_balance = sol_resp.value / 1_000_000_000
        except Exception as exc:
            log.error("snapshot_sol_balance_failed", error=str(exc))
            sol_balance = 0.0

        tokens = []
        try:
            # Use the specified json_parsed method with opt filter
            opts = TokenAccountOpts(program_id=Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"))
            resp = await rpc.get_token_accounts_by_owner_json_parsed(
                keypair.pubkey(),
                opts
            )
            
            if resp.value:
                for acc in resp.value:
                    try:
                        info = acc.account.data.parsed["info"]
                        mint = info["mint"]
                        amount = info["tokenAmount"]
                        
                        tokens.append({
                            "mint": mint,
                            "amount_raw": int(amount["amount"]),
                            "amount_ui": float(amount["uiAmount"] or 0.0)
                        })
                    except (KeyError, ValueError):
                        continue
                        
        except Exception as exc:
            log.error("snapshot_token_accounts_failed", error=str(exc))

        return {
            "sol_balance": sol_balance,
            "tokens": tokens
        }

    async def _get_raydium_pool_item(self, pool_id: str) -> Optional[dict]:
        """Fetch and cache Raydium pool list."""
        now = time.time()
        if self._raydium_list_cache and (now - self._raydium_list_time) < 600:
            for pool in self._raydium_list_cache.get("official", []) + self._raydium_list_cache.get("unOfficial", []):
                if pool.get("id") == pool_id:
                    return pool
            return None

        log.debug("fetching_raydium_list")
        try:
            async with self._session.get(RAYDIUM_POOL_ACCOUNTS_URL) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._raydium_list_cache = data
                    self._raydium_list_time = now
                    for pool in data.get("official", []) + data.get("unOfficial", []):
                        if pool.get("id") == pool_id:
                            return pool
        except Exception as exc:
            log.warning("raydium_list_fetch_failed", error=str(exc))

        return None

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

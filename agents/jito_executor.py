"""
agents/jito_executor.py — Jito MEV Protection Wrapper
======================================================
Subclasses RaydiumExecutor and JupiterZap to inject Jito Tips and send via Jito Bundles.
"""

from __future__ import annotations

import asyncio
import base64
import os
import structlog
from typing import List, Optional

import aiohttp
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.instruction import Instruction
from solders.message import MessageV0
from solders.transaction import VersionedTransaction
from solders.hash import Hash
import solders.system_program as sp

from defi_engine.execution import RaydiumExecutor, RAYDIUM_AMM_PROGRAM_ID
from defi_engine.jupiter_zap import JupiterZap

log = structlog.get_logger(__name__)

JITO_TIP_ACCOUNTS = [
    "96g6nqu7Gi6m6pbeX959Deq7uK6DdPup6fVAAhgAsgV",
    "HF677UT9kwGr96mBdySgze369WKfVry767beGVe97M6G",
    "Cw8CFyM9Fko9hp79EshS6A96uB6v7W7hx7SZhFrYVHLZ",
    "ADaUM5AAtU89v9vns659PZ7spPaDx9ndLneZBe8mS7ve"
]
JITO_BUNDLE_URL = "https://mainnet.block-engine.jito.wtf/api/v1/bundles"

def get_jito_tip_ix(payer: Pubkey, amount_lamports: int = 100000) -> Instruction:
    """Create systemic transfer instruction for Jito Tip."""
    # Pick first tip account
    tip_acc = Pubkey.from_string(JITO_TIP_ACCOUNTS[0])
    return sp.transfer(
        sp.TransferParams(
            from_pubkey=payer,
            to_pubkey=tip_acc,
            lamports=amount_lamports
        )
    )

async def send_jito_bundle(transactions: List[VersionedTransaction], session: aiohttp.ClientSession) -> Optional[str]:
    """Submit VersionedTransactions array as a bundle to Jito."""
    encoded_txs = [base64.b64encode(bytes(tx)).decode('utf-8') for tx in transactions]
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendBundle",
        "params": [encoded_txs]
    }

    try:
        async with session.post(JITO_BUNDLE_URL, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                bundle_id = data.get("result")
                log.info("jito_bundle_submitted", bundle_id=bundle_id)
                return bundle_id
            else:
                text = await resp.text()
                log.error("jito_bundle_failed_http", status=resp.status, response=text)
    except Exception as exc:
        log.error("jito_bundle_error", error=str(exc))
    return None

class JitoRaydiumExecutor(RaydiumExecutor):
    """Overrides RaydiumExecutor to append Jito Tips."""

    async def _build_and_send_add_liquidity(
        self,
        pool_accounts: dict,
        base_amount_lamports: int,
        quote_amount: int,
        slippage_bps: int,
    ) -> str:
        """Override to append Jito tip instruction before building VersionedTransaction."""
        import struct

        slip_factor = 1 + slippage_bps / 10_000
        max_base = int(base_amount_lamports * slip_factor)
        max_quote = int(quote_amount * slip_factor)

        data = struct.pack("<BQQ", 3, max_base, max_quote)
        keys = self._build_add_liquidity_keys(pool_accounts)
        instruction = Instruction(
            program_id=RAYDIUM_AMM_PROGRAM_ID,
            accounts=keys,
            data=bytes(data),
        )

        log.info("jito_injecting_tip_raydium_add")
        tip_ix = get_jito_tip_ix(self._keypair.pubkey())

        blockhash = await self._get_blockhash()
        msg = MessageV0.try_compile(
            payer=self._keypair.pubkey(),
            instructions=[instruction, tip_ix],  # INJECT TIP
            address_lookup_table_accounts=[],
            recent_blockhash=Hash.from_string(blockhash),
        )
        tx = VersionedTransaction(msg, [self._keypair])

        # Submit via Jito
        bundle_id = await send_jito_bundle([tx], self._session)
        if bundle_id:
            # wait confirmation logic on signature (which is first tx sig)
            sig = str(tx.signatures[0])
            await self._send_and_confirm(tx) # Fallback confirmation check
            return sig
        else:
             # Fallback standard submit if bundle failing
             log.warning("jito_bundle_failed_falling_back_standard")
             return await self._send_and_confirm(tx)

class JitoJupiterZap(JupiterZap):
    """Overrides JupiterZap to submit transaction bundles to Jito."""

    async def _execute_swap(self, quote: dict) -> str:
        """Overrides execution to inject Jito tip before serializing if needed, or simply send raw_tx bundle."""
        url = f"https://api.jup.ag/swap/v1/swap"
        # Jupiter has a known tip method or we can extract instruction.
        # But Jupiter allows passing a signed tx directly or building it.
        # It's safer to POST with wrapAndUnwrapSol, get serialized tx, then prepend tip in Message.
        
        payload = {
            "quoteResponse": quote,
            "userPublicKey": str(self._keypair.pubkey()),
            "wrapAndUnwrapSol": True,
            "prioritizationFeeLamports": "auto",
            "dynamicComputeUnitLimit": True,
        }

        async with self._session.post(url, json=payload) as resp:
             resp.raise_for_status()
             swap_data = await resp.json()

        raw_tx_bytes = base64.b64decode(swap_data["swapTransaction"])
        tx = VersionedTransaction.from_bytes(raw_tx_bytes)

        # To add instructions to an ALREADY COMPILED VersionedTransaction:
        # We must de-serialize it back into instructions, add tip, and re-compile!
        # This can be tricky with Address Lookup Tables.
        # Jupiter v6 almost always uses lookup tables.
        # Alternatively, Jupiter has a parameter `prioritizationFeeLamports` but it goes to validators generally.
        # Easier alternative for Jupiter: we can't easily add instructions to compiled ALT-based VersionedTransaction without fetching ALT data.
        # So we can just submit the ALREADY COMPILED tx as a bundle single-item to Jito if high fee,
        # OR just submit standardly if too locked up.
        # However, sending as a Jito Bundle single item STILL protects from MEV if accepted without tip?
        # No, Jito requires tip.
        # Standard: Submit the raw_tx_bytes bundle directly to Jito with tip item before it?
        # No, you can make a separate transaction that is just a tip and place it FIRST in the bundle on top of Jupiter!
        # Bundle: [ Tip_Tx, Jupiter_Tx ]
        # This is a standard and safe way. Jito executes atomically!
        log.info("jito_jupiter_zap_bundle_creating_tip_tx")
        
        # Build Tip_Tx
        tip_ix = get_jito_tip_ix(self._keypair.pubkey())
        blockhash = await self._rpc.get_latest_blockhash()
        # Create message for Tip_Tx
        msg_tip = MessageV0.try_compile(
            payer=self._keypair.pubkey(),
            instructions=[tip_ix],
            address_lookup_table_accounts=[],
            recent_blockhash=Hash.from_string(str(blockhash.value.blockhash)),
        )
        tip_tx = VersionedTransaction(msg_tip, [self._keypair])

        # Jupiter Signed Tx
        signed_jup_tx = VersionedTransaction(tx.message, [self._keypair])

        bundle_id = await send_jito_bundle([tip_tx, signed_jup_tx], self._session)
        if bundle_id:
            sig = str(signed_jup_tx.signatures[0])
            await self._confirm_transaction(sig)
            return sig
        else:
             log.warning("jito_jupiter_bundle_failed_falling_back")
             return await super()._execute_swap(quote) # Standard execution fallback

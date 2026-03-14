"""
defi_engine/jupiter_zap.py — SOL → Token Swap via Jupiter Aggregator v6
=========================================================================
Single-asset "zap" logic:
  1. Reserve GAS_RESERVE_SOL
  2. Call Jupiter /quote  → get best route
  3. Call Jupiter /swap   → build & submit transaction
  4. Confirm the transaction on-chain

Uses ONLY `solders` + `solana-py` — no third-party web3 abstractions.
"""

from __future__ import annotations

import asyncio
import base64
import os
from typing import Optional

import aiohttp
import structlog
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from defi_engine.math_engine import compute_zap_amounts, LAMPORTS_PER_SOL

log = structlog.get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
WSOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_API_BASE = os.getenv("JUPITER_API_BASE", "https://api.jup.ag/swap/v1")
JUPITER_SLIPPAGE_BPS = int(os.getenv("JUPITER_SLIPPAGE_BPS", "50"))


def _load_keypair() -> Keypair:
    """Load keypair strictly from .env — raises if missing or invalid."""
    raw = os.getenv("WALLET_PRIVATE_KEY_BASE58", "").strip()
    if not raw:
        raise EnvironmentError(
            "WALLET_PRIVATE_KEY_BASE58 is not set in .env. "
            "Generate one with: python -c \"from solders.keypair import Keypair; "
            "k=Keypair(); print(k.to_base58_string())\""
        )
    try:
        return Keypair.from_base58_string(raw)
    except Exception as exc:
        raise EnvironmentError(f"Invalid WALLET_PRIVATE_KEY_BASE58: {exc}") from exc


class JupiterZap:
    """Handles single-asset zap from SOL to any SPL token via Jupiter v6."""

    def __init__(self, rpc_client: AsyncClient) -> None:
        self._rpc = rpc_client
        self._keypair: Optional[Keypair] = None
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def __aenter__(self) -> "JupiterZap":
        self._keypair = _load_keypair()
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "GarganDeFiBot/1.0"},
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Public API ────────────────────────────────────────────────────────────

    async def get_wallet_sol_balance(self) -> float:
        """Return SOL balance of the burner wallet (in SOL)."""
        if self._keypair is None:
            self._keypair = _load_keypair()
        try:
            resp = await self._rpc.get_balance(self._keypair.pubkey())
            lamports = resp.value
            return lamports / LAMPORTS_PER_SOL
        except Exception as exc:
            log.error("get_balance_failed", error=str(exc))
            raise

    async def zap_sol_to_token(
        self,
        quote_mint: str,
        gas_reserve_sol: float = 0.02,
    ) -> dict:
        """
        Full zap flow:
          1. Get current SOL balance.
          2. Compute how much SOL to swap (50% of deployable).
          3. Get Jupiter quote.
          4. Execute the swap.
          5. Return results dict.
        """
        if self._keypair is None:
            self._keypair = _load_keypair()
        if self._session is None:
            raise RuntimeError("JupiterZap must be used as an async context manager.")

        # 1. Balance check
        total_sol = await self.get_wallet_sol_balance()
        amounts = compute_zap_amounts(total_sol, gas_reserve_sol)

        if amounts["sol_to_swap"] <= 0:
            raise ValueError(
                f"Insufficient SOL balance. Total: {total_sol:.4f} SOL, "
                f"reserve: {gas_reserve_sol:.4f} SOL, deployable: {amounts['deployable']:.4f} SOL"
            )

        swap_lamports = int(amounts["sol_to_swap"] * LAMPORTS_PER_SOL)
        log.info(
            "zap_starting",
            total_sol=total_sol,
            sol_to_swap=amounts["sol_to_swap"],
            quote_mint=quote_mint,
        )

        # 2. Get quote
        quote = await self._get_quote(
            input_mint=WSOL_MINT,
            output_mint=quote_mint,
            amount_lamports=swap_lamports,
        )

        # 3. Execute swap
        tx_sig = await self._execute_swap(quote)

        log.info(
            "zap_completed",
            tx_signature=str(tx_sig),
            sol_swapped=amounts["sol_to_swap"],
            out_token_mint=quote_mint,
        )

        return {
            "tx_signature": str(tx_sig),
            "sol_swapped": amounts["sol_to_swap"],
            "sol_for_base": amounts["sol_for_base"],
            "gas_reserve": amounts["gas_reserve"],
            "quote_mint": quote_mint,
            "out_amount_raw": int(quote.get("outAmount", 0)),
        }

    async def zap_token_to_sol(
        self,
        token_mint: str,
        token_amount_raw: int,
    ) -> dict:
        """
        Swap any SPL token back to SOL via Jupiter.
        Used when exiting a position.
        
        Args:
            token_mint: The mint address of the token to sell.
            token_amount_raw: Amount in raw token units (not UI amount).
        
        Returns:
            dict with tx_signature and sol_received estimate.
        """
        if self._keypair is None:
            self._keypair = _load_keypair()
        if self._session is None:
            raise RuntimeError("JupiterZap must be used as an async context manager.")

        log.info(
            "zap_token_to_sol_starting",
            token_mint=token_mint,
            amount_raw=token_amount_raw,
        )

        # Get quote: token → wSOL
        quote = await self._get_quote(
            input_mint=token_mint,
            output_mint=WSOL_MINT,
            amount_lamports=token_amount_raw,
        )

        sol_out_lamports = int(quote.get("outAmount", 0))
        sol_out = sol_out_lamports / LAMPORTS_PER_SOL

        # Execute swap
        tx_sig = await self._execute_swap(quote)

        log.info(
            "zap_token_to_sol_completed",
            tx_signature=tx_sig,
            sol_received=sol_out,
            token_mint=token_mint,
        )

        return {
            "tx_signature": tx_sig,
            "sol_received": sol_out,
            "token_mint": token_mint,
            "token_amount_raw": token_amount_raw,
        }

    # ── Jupiter API internals ─────────────────────────────────────────────────

    async def _get_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount_lamports: int,
    ) -> dict:
        """Fetch the best swap route from Jupiter /quote endpoint."""
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_lamports),
            "slippageBps": str(JUPITER_SLIPPAGE_BPS),
            "onlyDirectRoutes": "false",
            "asLegacyTransaction": "false",
        }
        url = f"{JUPITER_API_BASE}/quote"

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=1, min=2, max=20),
            retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
            reraise=True,
        ):
            with attempt:
                async with self._session.get(url, params=params) as resp:
                    resp.raise_for_status()
                    quote = await resp.json()
                    log.debug(
                        "jupiter_quote_received",
                        in_amount=amount_lamports,
                        out_amount=quote.get("outAmount"),
                        price_impact=quote.get("priceImpactPct"),
                    )
                    return quote

    async def _execute_swap(self, quote: dict) -> str:
        """
        POST to Jupiter /swap to get the serialized transaction,
        sign it with the burner wallet, and submit to the RPC.
        Returns the transaction signature string.
        """
        url = f"{JUPITER_API_BASE}/swap"
        payload = {
            "quoteResponse": quote,
            "userPublicKey": str(self._keypair.pubkey()),
            "wrapAndUnwrapSol": True,
            "prioritizationFeeLamports": "auto",
            "dynamicComputeUnitLimit": True,
        }

        swap_data = None
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=15),
            retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
            reraise=True,
        ):
            with attempt:
                async with self._session.post(url, json=payload) as resp:
                    resp.raise_for_status()
                    swap_data = await resp.json()

        # Decode the transaction
        raw_tx_bytes = base64.b64decode(swap_data["swapTransaction"])

        # Deserialize, sign and reserialize correctly with solders
        tx = VersionedTransaction.from_bytes(raw_tx_bytes)
        signed_tx = VersionedTransaction(tx.message, [self._keypair])

        # Submit via Solana RPC using TxOpts (not a dict)
        try:
            result = await self._rpc.send_raw_transaction(
                bytes(signed_tx),
                opts=TxOpts(skip_preflight=False, preflight_commitment="confirmed"),
            )
            sig = result.value
        except Exception as exc:
            log.error("send_transaction_failed", error=str(exc))
            raise

        log.info("transaction_submitted", signature=str(sig))
        await self._confirm_transaction(str(sig))
        return str(sig)

    async def _confirm_transaction(self, signature: str, timeout_s: int = 300) -> None:
        """Poll until a transaction is confirmed or timeout expires.
        On timeout, logs a warning but does NOT raise — tx likely landed on-chain."""
        deadline = asyncio.get_event_loop().time() + timeout_s
        log.info("awaiting_confirmation", signature=signature, timeout_s=timeout_s)

        while asyncio.get_event_loop().time() < deadline:
            try:
                resp = await self._rpc.get_signature_statuses([signature])
                status = resp.value[0]
                if status is not None:
                    if status.err:
                        raise RuntimeError(
                            f"Transaction {signature} failed on-chain: {status.err}"
                        )
                    if status.confirmation_status in ("confirmed", "finalized"):
                        log.info("transaction_confirmed", signature=signature,
                                 status=status.confirmation_status)
                        return
            except RuntimeError:
                raise  # Re-raise on-chain failures immediately
            except Exception as exc:
                log.warning("confirm_poll_error", error=str(exc))

            await asyncio.sleep(5)

        # Timeout — warn but don't crash. The tx likely landed.
        log.warning(
            "confirmation_timeout_non_fatal",
            signature=signature,
            timeout_s=timeout_s,
            note="Transaction may still have succeeded. Check explorer manually.",
        )

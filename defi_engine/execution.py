"""
defi_engine/execution.py — Raydium AMM Deposit & Withdraw
==========================================================
Handles:
  • Adding liquidity to a Raydium AMM pool (after Jupiter zap)
  • Removing liquidity and redeeming LP tokens

Uses ONLY `solders` + `solana-py`. All token account management is explicit.
Private key is loaded strictly from .env via _load_keypair().

NOTE: Raydium's on-chain program requires knowing the pool's full account set.
      We fetch this from Raydium's public API to avoid hard-coded addresses.
"""

from __future__ import annotations

import asyncio
import os
from typing import Optional

import aiohttp
import structlog
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
from solders.message import MessageV0
from solders.transaction import VersionedTransaction
from solders.hash import Hash
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from defi_engine.math_engine import LAMPORTS_PER_SOL

log = structlog.get_logger(__name__)

# ── Raydium AMM v4 Program ID (mainnet) ───────────────────────────────────────
RAYDIUM_AMM_PROGRAM_ID = Pubkey.from_string("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8")
# SPL Token Program
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
# Associated Token Program
ATA_PROGRAM_ID = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe8bXa")
# System Program
SYSTEM_PROGRAM_ID = Pubkey.from_string("11111111111111111111111111111111")

RAYDIUM_POOL_ACCOUNTS_URL = "https://api.raydium.io/v2/ammV3/ammPool"


def _load_keypair() -> Keypair:
    raw = os.getenv("WALLET_PRIVATE_KEY_BASE58", "").strip()
    if not raw:
        raise EnvironmentError("WALLET_PRIVATE_KEY_BASE58 not set in .env")
    try:
        return Keypair.from_base58_string(raw)
    except Exception as exc:
        raise EnvironmentError(f"Invalid private key: {exc}") from exc


def _pda(seeds: list[bytes], program_id: Pubkey) -> Pubkey:
    """Find a Program Derived Address (PDA)."""
    addr, _bump = Pubkey.find_program_address(seeds, program_id)
    return addr


class RaydiumExecutor:
    """
    Executes Raydium AMM add/remove liquidity transactions.
    
    All methods are coroutines. Use as an async context manager to 
    ensure the aiohttp session is properly closed.
    """

    def __init__(self, rpc_client: AsyncClient) -> None:
        self._rpc = rpc_client
        self._keypair: Optional[Keypair] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "RaydiumExecutor":
        self._keypair = _load_keypair()
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "GarganDeFiBot/1.0"},
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Public: Add Liquidity ─────────────────────────────────────────────────

    async def add_liquidity(
        self,
        pool_id: str,
        base_amount_lamports: int,
        quote_amount: int,
        slippage_bps: int = 50,
    ) -> str:
        """
        Add liquidity to a Raydium AMM pool.
        
        Args:
            pool_id: Raydium pool public key string.
            base_amount_lamports: Amount of base token (e.g., wSOL) in lamports.
            quote_amount: Amount of quote token (e.g., USDC) in raw units.
            slippage_bps: Slippage tolerance in basis points.
        
        Returns:
            Transaction signature string.
        """
        log.info(
            "add_liquidity_starting",
            pool_id=pool_id,
            base_lamports=base_amount_lamports,
            quote_amount=quote_amount,
        )

        pool_accounts = await self._fetch_pool_accounts(pool_id)
        if pool_accounts is None:
            raise RuntimeError(f"Could not fetch pool accounts for pool {pool_id}")

        # Build and send the addLiquidity transaction
        tx_sig = await self._build_and_send_add_liquidity(
            pool_accounts=pool_accounts,
            base_amount_lamports=base_amount_lamports,
            quote_amount=quote_amount,
            slippage_bps=slippage_bps,
        )
        log.info("add_liquidity_success", signature=tx_sig, pool_id=pool_id)
        return tx_sig

    # ── Public: Remove Liquidity ──────────────────────────────────────────────

    async def remove_liquidity(
        self,
        pool_id: str,
        lp_amount: int,
    ) -> str:
        """
        Remove all or a portion of liquidity from a Raydium AMM pool.
        
        Args:
            pool_id: Raydium pool public key string.
            lp_amount: LP token amount to burn (raw units).
        
        Returns:
            Transaction signature string.
        """
        log.info(
            "remove_liquidity_starting",
            pool_id=pool_id,
            lp_amount=lp_amount,
        )

        pool_accounts = await self._fetch_pool_accounts(pool_id)
        if pool_accounts is None:
            raise RuntimeError(f"Could not fetch pool accounts for pool {pool_id}")

        tx_sig = await self._build_and_send_remove_liquidity(
            pool_accounts=pool_accounts,
            lp_amount=lp_amount,
        )
        log.info("remove_liquidity_success", signature=tx_sig, pool_id=pool_id)
        return tx_sig

    async def get_lp_token_balance(self, lp_mint: str) -> int:
        """Return LP token balance in raw units for the burner wallet."""
        if self._keypair is None:
            self._keypair = _load_keypair()
        lp_mint_pubkey = Pubkey.from_string(lp_mint)
        # Derive the Associated Token Account
        ata = _pda(
            [
                bytes(self._keypair.pubkey()),
                bytes(TOKEN_PROGRAM_ID),
                bytes(lp_mint_pubkey),
            ],
            ATA_PROGRAM_ID,
        )
        try:
            resp = await self._rpc.get_token_account_balance(ata)
            return int(resp.value.amount)
        except Exception as exc:
            log.warning("get_lp_balance_failed", error=str(exc))
            return 0

    # ── Internals: Pool Data ──────────────────────────────────────────────────

    async def _fetch_pool_accounts(self, pool_id: str) -> Optional[dict]:
        """Fetch full pool account addresses from Raydium API."""
        url = f"{RAYDIUM_POOL_ACCOUNTS_URL}/{pool_id}"
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(4),
                wait=wait_exponential(multiplier=1, min=2, max=30),
                retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
                reraise=True,
            ):
                with attempt:
                    async with self._session.get(url) as resp:
                        resp.raise_for_status()
                        return await resp.json()
        except Exception as exc:
            log.error("fetch_pool_accounts_failed", pool_id=pool_id, error=str(exc))
            return None

    # ── Internals: Transaction Builders ──────────────────────────────────────

    async def _get_blockhash(self) -> str:
        resp = await self._rpc.get_latest_blockhash()
        return str(resp.value.blockhash)

    async def _send_and_confirm(self, tx: VersionedTransaction, timeout_s: int = 90) -> str:
        """Submit a signed VersionedTransaction and wait for confirmation."""
        result = await self._rpc.send_raw_transaction(bytes(tx))
        sig = str(result.value)

        deadline = asyncio.get_event_loop().time() + timeout_s
        while asyncio.get_event_loop().time() < deadline:
            try:
                resp = await self._rpc.get_signature_statuses([sig])
                status = resp.value[0]
                if status is not None:
                    if status.err:
                        raise RuntimeError(f"Tx {sig} failed: {status.err}")
                    if status.confirmation_status in ("confirmed", "finalized"):
                        log.info("tx_confirmed", signature=sig)
                        return sig
            except RuntimeError:
                raise
            except Exception as exc:
                log.warning("confirm_poll_error", error=str(exc))
            await asyncio.sleep(4)

        raise TimeoutError(f"Transaction {sig} not confirmed within {timeout_s}s")

    async def _build_and_send_add_liquidity(
        self,
        pool_accounts: dict,
        base_amount_lamports: int,
        quote_amount: int,
        slippage_bps: int,
    ) -> str:
        """
        Build the addLiquidity instruction for Raydium AMM v4.

        Raydium AMM AddLiquidity instruction layout (discriminant 3):
          [0]  u8  = 3  (addLiquidity discriminant)
          [1:9] u64 = max base token amount
          [9:17] u64 = max quote token amount
          [17:25] u64 = base side amount (this side determines the ratio)
          
        See: https://github.com/raydium-io/raydium-amm/blob/master/program/src/instruction.rs
        """
        import struct

        # Apply slippage to create max amounts
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

        blockhash = await self._get_blockhash()
        msg = MessageV0.try_compile(
            payer=self._keypair.pubkey(),
            instructions=[instruction],
            address_lookup_table_accounts=[],
            recent_blockhash=Hash.from_string(blockhash),
        )
        tx = VersionedTransaction(msg, [self._keypair])
        return await self._send_and_confirm(tx)

    async def _build_and_send_remove_liquidity(
        self,
        pool_accounts: dict,
        lp_amount: int,
    ) -> str:
        """Build the removeLiquidity instruction for Raydium AMM v4 (discriminant 4)."""
        import struct

        data = struct.pack("<BQ", 4, lp_amount)
        keys = self._build_remove_liquidity_keys(pool_accounts)
        instruction = Instruction(
            program_id=RAYDIUM_AMM_PROGRAM_ID,
            accounts=keys,
            data=bytes(data),
        )

        blockhash = await self._get_blockhash()
        msg = MessageV0.try_compile(
            payer=self._keypair.pubkey(),
            instructions=[instruction],
            address_lookup_table_accounts=[],
            recent_blockhash=Hash.from_string(blockhash),
        )
        tx = VersionedTransaction(msg, [self._keypair])
        return await self._send_and_confirm(tx)

    # ── Account key helpers ───────────────────────────────────────────────────

    def _build_add_liquidity_keys(self, p: dict) -> list[AccountMeta]:
        """
        Build the ordered account list for Raydium addLiquidity.
        Order matches the Raydium AMM v4 IDL.
        """
        kp = self._keypair
        is_signer_writable = lambda k: AccountMeta(pubkey=Pubkey.from_string(k), is_signer=False, is_writable=True)
        is_readonly = lambda k: AccountMeta(pubkey=Pubkey.from_string(k), is_signer=False, is_writable=False)

        return [
            AccountMeta(pubkey=TOKEN_PROGRAM_ID,      is_signer=False, is_writable=False),
            is_signer_writable(p["id"]),                # AMM pool
            is_readonly(p["authority"]),                # AMM authority (PDA)
            is_signer_writable(p["openOrders"]),        # AMM open orders
            is_signer_writable(p["lpMint"]),            # LP mint
            is_signer_writable(p["baseVault"]),         # Pool base vault
            is_signer_writable(p["quoteVault"]),        # Pool quote vault
            is_readonly(p.get("marketId", p["id"])),    # Serum market
            # User accounts
            AccountMeta(pubkey=kp.pubkey(), is_signer=True, is_writable=True),
            is_signer_writable(p.get("userBaseAta", p["baseVault"])),
            is_signer_writable(p.get("userQuoteAta", p["quoteVault"])),
            is_signer_writable(p.get("userLpAta", p["lpMint"])),
        ]

    def _build_remove_liquidity_keys(self, p: dict) -> list[AccountMeta]:
        kp = self._keypair
        is_signer_writable = lambda k: AccountMeta(pubkey=Pubkey.from_string(k), is_signer=False, is_writable=True)
        is_readonly = lambda k: AccountMeta(pubkey=Pubkey.from_string(k), is_signer=False, is_writable=False)

        return [
            AccountMeta(pubkey=TOKEN_PROGRAM_ID,      is_signer=False, is_writable=False),
            is_signer_writable(p["id"]),
            is_readonly(p["authority"]),
            is_signer_writable(p["openOrders"]),
            is_signer_writable(p["lpMint"]),
            is_signer_writable(p["baseVault"]),
            is_signer_writable(p["quoteVault"]),
            is_readonly(p.get("marketId", p["id"])),
            AccountMeta(pubkey=kp.pubkey(), is_signer=True, is_writable=True),
            is_signer_writable(p.get("userBaseAta", p["baseVault"])),
            is_signer_writable(p.get("userQuoteAta", p["quoteVault"])),
            is_signer_writable(p.get("userLpAta", p["lpMint"])),
        ]

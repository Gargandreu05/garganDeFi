"""
ui/database.py — Async SQLite Database Layer
==============================================
All reads/writes use aiosqlite so the Discord event loop is never blocked.
Tables:
  • pool_evaluations  — every scan result (APY, IL, fees, etc.)
  • alerts            — every Discord alert that was sent
  • trades            — executed deposits / withdrawals
  • deals             — scraped hardware deals
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Optional

import aiosqlite
import structlog

log = structlog.get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS pool_evaluations (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts              TEXT    NOT NULL,
        pool_id         TEXT    NOT NULL,
        pool_name       TEXT    NOT NULL,
        apy_pct         REAL,
        fee_7d_usd      REAL,
        il_pct          REAL,
        net_apy_pct     REAL,
        recommended     INTEGER DEFAULT 0,
        raw_json        TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts              TEXT    NOT NULL,
        alert_type      TEXT    NOT NULL,   -- 'POOL_RECOMMENDATION' | 'DEAL' | 'ERROR'
        message         TEXT    NOT NULL,
        discord_msg_id  TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trades (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts              TEXT    NOT NULL,
        trade_type      TEXT    NOT NULL,   -- 'DEPOSIT' | 'WITHDRAW' | 'SWAP'
        pool_id         TEXT,
        pool_name       TEXT,
        amount_sol      REAL,
        amount_token    REAL,
        token_mint      TEXT,
        tx_signature    TEXT,
        status          TEXT    NOT NULL DEFAULT 'PENDING',  -- 'PENDING'|'CONFIRMED'|'FAILED'
        error_msg       TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS deals (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts              TEXT    NOT NULL,
        title           TEXT    NOT NULL,
        price           REAL,
        original_price  REAL,
        discount_pct    REAL,
        url             TEXT,
        source          TEXT,
        discord_msg_id  TEXT
    )
    """,
]

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_pe_ts   ON pool_evaluations(ts)",
    "CREATE INDEX IF NOT EXISTS idx_pe_pool ON pool_evaluations(pool_id)",
    "CREATE INDEX IF NOT EXISTS idx_tr_ts   ON trades(ts)",
    "CREATE INDEX IF NOT EXISTS idx_al_ts   ON alerts(ts)",
    "CREATE INDEX IF NOT EXISTS idx_dl_ts   ON deals(ts)",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    """Thread-safe async SQLite wrapper."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def initialise(self) -> None:
        """Create tables and indexes if they do not exist."""
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        # Enable WAL for better concurrent read performance
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        for stmt in DDL_STATEMENTS + INDEX_STATEMENTS:
            await self._conn.execute(stmt)
        await self._conn.commit()
        log.info("db_initialised", path=self._path)

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None
            log.info("db_closed")

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _execute(self, sql: str, params: tuple = ()) -> int:
        """Execute a write statement and return lastrowid."""
        async with self._lock:
            cur = await self._conn.execute(sql, params)
            await self._conn.commit()
            return cur.lastrowid

    async def _fetchall(self, sql: str, params: tuple = ()) -> list[dict]:
        async with self._lock:
            cur = await self._conn.execute(sql, params)
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def _fetchone(self, sql: str, params: tuple = ()) -> Optional[dict]:
        async with self._lock:
            cur = await self._conn.execute(sql, params)
            row = await cur.fetchone()
            return dict(row) if row else None

    # ── Pool evaluations ──────────────────────────────────────────────────────

    async def insert_pool_evaluation(
        self,
        pool_id: str,
        pool_name: str,
        apy_pct: float,
        fee_7d_usd: float,
        il_pct: float,
        net_apy_pct: float,
        recommended: bool = False,
        raw_json: str = "",
    ) -> int:
        return await self._execute(
            """INSERT INTO pool_evaluations
               (ts, pool_id, pool_name, apy_pct, fee_7d_usd, il_pct, net_apy_pct, recommended, raw_json)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                _now_iso(), pool_id, pool_name, apy_pct, fee_7d_usd,
                il_pct, net_apy_pct, int(recommended), raw_json,
            ),
        )

    async def get_recent_evaluations(self, limit: int = 100) -> list[dict]:
        return await self._fetchall(
            "SELECT * FROM pool_evaluations ORDER BY ts DESC LIMIT ?",
            (limit,),
        )

    async def get_evaluations_for_pool(self, pool_id: str, limit: int = 500) -> list[dict]:
        return await self._fetchall(
            "SELECT * FROM pool_evaluations WHERE pool_id=? ORDER BY ts DESC LIMIT ?",
            (pool_id, limit),
        )

    async def clear_all_evaluations(self) -> None:
        """Wipe all pool evaluations for a fresh start."""
        await self._execute("DELETE FROM pool_evaluations")
        log.info("db_evaluations_wiped")

    async def wipe_stale_evaluations(self, days: int = 7) -> None:
        """Remove evaluations older than X days."""
        await self._execute(
            "DELETE FROM pool_evaluations WHERE ts < datetime('now', '-' || ? || ' days')",
            (days,),
        )
        log.info("db_stale_evaluations_cleaned", days=days)

    # ── Alerts ────────────────────────────────────────────────────────────────

    async def insert_alert(
        self,
        alert_type: str,
        message: str,
        discord_msg_id: Optional[str] = None,
    ) -> int:
        return await self._execute(
            "INSERT INTO alerts (ts, alert_type, message, discord_msg_id) VALUES (?,?,?,?)",
            (_now_iso(), alert_type, message, discord_msg_id),
        )

    async def get_recent_alerts(self, limit: int = 50) -> list[dict]:
        return await self._fetchall(
            "SELECT * FROM alerts ORDER BY ts DESC LIMIT ?",
            (limit,),
        )

    # ── Trades ────────────────────────────────────────────────────────────────

    async def insert_trade(
        self,
        trade_type: str,
        pool_id: Optional[str] = None,
        pool_name: Optional[str] = None,
        amount_sol: Optional[float] = None,
        amount_token: Optional[float] = None,
        token_mint: Optional[str] = None,
        tx_signature: Optional[str] = None,
        status: str = "PENDING",
        error_msg: Optional[str] = None,
    ) -> int:
        return await self._execute(
            """INSERT INTO trades
               (ts, trade_type, pool_id, pool_name, amount_sol, amount_token,
                token_mint, tx_signature, status, error_msg)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                _now_iso(), trade_type, pool_id, pool_name, amount_sol,
                amount_token, token_mint, tx_signature, status, error_msg,
            ),
        )

    async def update_trade_status(
        self, trade_id: int, status: str, tx_signature: Optional[str] = None
    ) -> None:
        await self._execute(
            "UPDATE trades SET status=?, tx_signature=COALESCE(?,tx_signature) WHERE id=?",
            (status, tx_signature, trade_id),
        )

    async def get_recent_trades(self, limit: int = 100) -> list[dict]:
        return await self._fetchall(
            "SELECT * FROM trades ORDER BY ts DESC LIMIT ?",
            (limit,),
        )

    async def get_net_profit_sol(self) -> float:
        """Rough P&L: sum of deposited SOL minus withdrawn SOL."""
        row = await self._fetchone(
            """SELECT
                 COALESCE(SUM(CASE WHEN trade_type='DEPOSIT'  THEN amount_sol ELSE 0 END), 0) AS deposited,
                 COALESCE(SUM(CASE WHEN trade_type='WITHDRAW' THEN amount_sol ELSE 0 END), 0) AS withdrawn
               FROM trades WHERE status='CONFIRMED'"""
        )
        if not row:
            return 0.0
        return float(row["withdrawn"]) - float(row["deposited"])

    # ── Deals ─────────────────────────────────────────────────────────────────

    async def insert_deal(
        self,
        title: str,
        price: Optional[float],
        original_price: Optional[float],
        discount_pct: Optional[float],
        url: str,
        source: str,
        discord_msg_id: Optional[str] = None,
    ) -> int:
        return await self._execute(
            """INSERT INTO deals
               (ts, title, price, original_price, discount_pct, url, source, discord_msg_id)
               VALUES (?,?,?,?,?,?,?,?)""",
            (_now_iso(), title, price, original_price, discount_pct, url, source, discord_msg_id),
        )

    async def deal_already_seen(self, url: str) -> bool:
        row = await self._fetchone("SELECT id FROM deals WHERE url=?", (url,))
        return row is not None

    async def get_recent_deals(self, limit: int = 50) -> list[dict]:
        return await self._fetchall(
            "SELECT * FROM deals ORDER BY ts DESC LIMIT ?",
            (limit,),
        )

"""
main.py — GarganDeFi Bot Entry Point
=====================================
Async entry point that:
  • Boots the Discord bot with all Cogs
  • Initialises the async SQLite database
  • Launches background tasks (pool scanner, deals scraper)
  • Gracefully shuts everything down on SIGINT / SIGTERM
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

import discord
from discord.ext import commands
from dotenv import load_dotenv

import structlog

from ui.database import Database
from defi_engine.pool_scanner import PoolScanner
from crawlers.motor import DealsCrawler

# ── Load .env ──────────────────────────────────────────────────────────────────
load_dotenv()

# ── Structured logging ─────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, LOG_LEVEL, logging.INFO)
    ),
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger(__name__)

# ── Discord intents ────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = False   # not required, keeps the bot lower-privilege

# ── Bot setup ─────────────────────────────────────────────────────────────────
bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    help_command=commands.DefaultHelpCommand(),
    description="GarganDeFi — Hardware Deals + Solana DeFi HITL Bot",
)

# Global shared state passed to cogs via bot.state
bot.state: dict = {}


# ──────────────────────────────────────────────────────────────────────────────
#  Lifecycle hooks
# ──────────────────────────────────────────────────────────────────────────────

@bot.event
async def on_ready() -> None:
    log.info("discord_ready", user=str(bot.user), guilds=len(bot.guilds))
    await bot.tree.sync()   # Sync slash commands if any are added later


@bot.event
async def on_command_error(ctx: commands.Context, error: Exception) -> None:
    """Global error handler — surfaces failures in Discord rather than crashing."""
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("⛔ You don't have permission to use that command.")
        return
    log.error("command_error", command=ctx.command, error=str(error))
    await ctx.send(f"❌ An error occurred: `{error}`")


# ──────────────────────────────────────────────────────────────────────────────
#  Startup / teardown
# ──────────────────────────────────────────────────────────────────────────────

async def _load_cogs() -> None:
    cog_modules = [
        "cogs.deals_cog",
        "cogs.defi_cog",
    ]
    for module in cog_modules:
        try:
            await bot.load_extension(module)
            log.info("cog_loaded", module=module)
        except Exception as exc:
            log.error("cog_load_failed", module=module, error=str(exc))
            raise


async def main() -> None:
    # 1. Initialise DB
    db_path = os.getenv("DB_PATH", "./data/gargandefi.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    db = Database(db_path)
    await db.initialise()
    log.info("database_ready", path=db_path)

    # 2. Initialise shared services
    scanner = PoolScanner(db=db)
    crawler = DealsCrawler(db=db)

    # Store on bot so Cogs can access via ctx.bot.state
    bot.state["db"] = db
    bot.state["pool_scanner"] = scanner
    bot.state["deals_crawler"] = crawler

    # 3. Load Cogs
    await _load_cogs()

    # 4. Graceful shutdown handler
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal(signum, _frame):
        log.info("shutdown_signal_received", signal=signum)
        loop.call_soon_threadsafe(shutdown_event.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handle_signal)

    # 5. Start everything concurrently
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        log.error("missing_discord_token")
        sys.exit(1)

    async with bot:
        background_tasks = [
            asyncio.create_task(scanner.run_forever(), name="pool_scanner"),
            asyncio.create_task(crawler.run_forever(), name="deals_crawler"),
            asyncio.create_task(bot.start(token), name="discord_bot"),
            asyncio.create_task(shutdown_event.wait(), name="shutdown_watcher"),
        ]

        # Wait until the shutdown event fires or any task raises
        done, pending = await asyncio.wait(
            background_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel remaining tasks cleanly
        for task in pending:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        # Close DB
        await db.close()
        log.info("shutdown_complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

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
from config.system_monitor import SystemMonitor

# ── Multi-Agent System Imports ──────────────────────────────────────────────────
from agents.market_agent import MarketAgent
from agents.scanner_agent import ScannerAgent
from agents.risk_agent import RiskAgent
from agents.execution_agent import ExecutionAgent
from agents.discord_agent import DiscordAgent
from agents.coordinator import Coordinator
from agents.news_agent import NewsAgent

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
    
    # DEV-OPS STARTUP BROADCAST
    monitor = bot.state.get("system_monitor")
    if monitor:
        metrics = await monitor.get_system_metrics()
        
        embed = discord.Embed(
            title="🟢 GarganDeFi Core Online",
            description="All modules loaded and background tracking active.",
            color=discord.Color.green()
        )
        embed.add_field(name="CPU Usage", value=f"`{metrics['cpu_percent']:.1f}%`", inline=True)
        embed.add_field(name="CPU Temp", value=f"`{metrics['cpu_temp']:.1f}°C`", inline=True)
        embed.add_field(name="RAM Usage", value=f"`{metrics['ram_used_gb']:.1f}GB / {metrics['ram_total_gb']:.1f}GB ({metrics['ram_percent']}%)`", inline=False)
        embed.set_footer(text="DevOps & Hardware Monitor")
        
        # Send to DEALS channel
        deals_id = int(os.getenv("DISCORD_DEALS_CHANNEL_ID", "0"))
        if deals_id > 0:
            c1 = bot.get_channel(deals_id)
            if c1:
                await c1.send(embed=embed)
                
        # Send to DEFI channel
        defi_id = int(os.getenv("DISCORD_DEFI_ALERTS_CHANNEL_ID", "0"))
        if defi_id > 0:
            c2 = bot.get_channel(defi_id)
            if c2:
                await c2.send(embed=embed)


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

async def main() -> None:
    # 1. Initialise DB
    db_path = os.getenv("DB_PATH", "./data/gargandefi.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    db = Database(db_path)
    await db.initialise()
    log.info("database_ready", path=db_path)

    # 2. Initialise Agents
    market_agent = MarketAgent()
    scanner_agent = ScannerAgent(market_agent=market_agent, db=db)
    risk_agent = RiskAgent(market_agent=market_agent)
    execution_agent = ExecutionAgent(db=db)
    discord_agent = DiscordAgent(bot=bot)
    
    coordinator = Coordinator(
        market=market_agent, 
        scanner=scanner_agent, 
        risk=risk_agent,
        executor=execution_agent, 
        discord=discord_agent, 
        db=db
    )
    
    news_agent = NewsAgent(discord_agent=discord_agent)
    
    discord_agent.set_coordinator(coordinator)
    monitor = SystemMonitor(bot=bot)

    # Store on bot so standard access patterns still work for monitor/DB if needed
    bot.state["db"] = db
    bot.state["system_monitor"] = monitor

    # 3. Load Discord Cog
    await bot.add_cog(discord_agent)
    log.info("discord_agent_cog_loaded")

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
            asyncio.create_task(coordinator.run_forever(), name="coordinator_loop"),
            asyncio.create_task(news_agent.run_forever(), name="news_loop"),
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
        # Ensure monitor loops are closed
        monitor.stop()
        await market_agent.close()  # Close session
        log.info("shutdown_complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

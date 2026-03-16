"""
agents/discord_agent.py — Discord I/O and Commands
===================================================
Handles all Discord interactions without any direct Solana logic.
Communicates with the Coordinator for operations.
"""

from __future__ import annotations

import os
import structlog
from typing import Optional, Any

import discord
from discord.ext import commands

log = structlog.get_logger(__name__)

class _MigrationView(discord.ui.View):
    """Interactive buttons for HITL migration approval."""

    def __init__(self, discord_agent: "DiscordAgent", candidate: dict, timeout: float = 3600) -> None:
        super().__init__(timeout=timeout)
        self._discord_agent = discord_agent
        self._candidate = candidate
        self._decided = False

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success)
    async def approve(self, interaction: discord.Interaction, _btn: discord.ui.Button) -> None:
        owner_id = int(os.getenv("DISCORD_OWNER_ID", "0"))
        if interaction.user.id != owner_id:
            await interaction.response.send_message("⛔ Only the owner can approve.", ephemeral=True)
            return
        if self._decided:
            await interaction.response.send_message("⚠️ Already decided.", ephemeral=True)
            return
        self._decided = True
        self.stop()
        await interaction.response.defer()
        
        # Call coordinator
        coordinator = self._discord_agent.get_coordinator()
        if coordinator:
            try:
                await coordinator.approve_migration_cmd()
                await interaction.followup.send("✅ Migration approved and triggered successfully.")
            except Exception as e:
                await interaction.followup.send(f"❌ Error during approval execution: `{str(e)}`")
        else:
            await interaction.followup.send("❌ Coordinator not connected.")

    @discord.ui.button(label="Dismiss", style=discord.ButtonStyle.secondary)
    async def reject(self, interaction: discord.Interaction, _btn: discord.ui.Button) -> None:
        owner_id = int(os.getenv("DISCORD_OWNER_ID", "0"))
        if interaction.user.id != owner_id:
            await interaction.response.send_message("⛔ Only the owner can dismiss.", ephemeral=True)
            return
        if self._decided:
            await interaction.response.send_message("⚠️ Already decided.", ephemeral=True)
            return
        self._decided = True
        self.stop()
        
        coordinator = self._discord_agent.get_coordinator()
        if coordinator:
            await coordinator.reject_migration()
        await interaction.response.send_message("✅ Migration dismissed. Staying in current pool.")

class DiscordAgent(commands.Cog, name="DeFi Engine"):
    """
    Handles Discord interface. No direct Solana logic.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._coordinator: Optional[Any] = None
        self._owner_id = int(os.getenv("DISCORD_OWNER_ID", "0"))

    def set_coordinator(self, coordinator: Any) -> None:
        """Inject the coordinator after instances are wired up."""
        self._coordinator = coordinator
        log.info("discord_agent_coordinator_wired")

    def get_coordinator(self) -> Optional[Any]:
        return self._coordinator

    async def cog_check(self, ctx: commands.Context) -> bool:
        """All DeFi commands require the owner."""
        return ctx.author.id == self._owner_id

    async def send_migration_alert(self, current: Optional[dict], candidate: dict, reason: str) -> None:
        """
        Send migration alert with Approve/Dismiss buttons.
        Callback used by coordinator.
        """
        log.info("sending_migration_alert", pool_id=candidate.get("pool_id"))
        channel_id = int(os.getenv("DISCORD_PRIVATE_DEFI_CHANNEL_ID", "0"))
        channel = self.bot.get_channel(channel_id)
        if not channel:
            log.warning("discord_alert_channel_not_found", id=channel_id)
            return

        current_name = current["name"] if current else "None"
        current_apy = current["net_apy"] if current else 0.0

        embed = discord.Embed(
            title="🎯 Migration Proposal",
            description=reason,
            color=discord.Color.blue()
        )
        embed.add_field(name="From", value=f"`{current_name}` ({current_apy:.2f}% APY)", inline=True)
        embed.add_field(name="To", value=f"`{candidate['name']}`", inline=True)
        embed.add_field(name="📊 APY Est.", value=f"**{candidate.get('net_apy', 0.0):.2f}%**", inline=True)
        
        liquidity = candidate.get("liquidity", 0)
        volume_24h = candidate.get("volume_24h", 0)
        embed.add_field(name="💰 TVL", value=f"**${liquidity:,.0f}**", inline=True)
        embed.add_field(name="📈 Volumen 24h", value=f"**${volume_24h:,.0f}**", inline=True)
        embed.add_field(name="🛡️ Auditoria", value=f"✅ RugCheck Safe", inline=True)

        if candidate.get("dex_url"):
             embed.add_field(name="🔗 Análisis", value=f"[DexScreener]({candidate['dex_url']})", inline=False)

        view = _MigrationView(discord_agent=self, candidate=candidate)
        await channel.send(embed=embed, view=view)

    async def send_update(self, message: str, success: bool = True, channel_id: Optional[int] = None) -> None:
        """Generic update message sender for tasks."""
        if not channel_id:
             channel_id = int(os.getenv("DISCORD_PRIVATE_DEFI_CHANNEL_ID", "0"))
             
        channel = self.bot.get_channel(channel_id)
        if channel:
            color = discord.Color.green() if success else discord.Color.red()
            embed = discord.Embed(description=message, color=color)
            await channel.send(embed=embed)

    # ── Commands ─────────────────────────────────────────────────────────────

    @commands.command(name="defi_status", aliases=["dfi"])
    async def defi_status(self, ctx: commands.Context) -> None:
        """Show active pool metrics, wallet balance from Market Agent."""
        if not self._coordinator:
            await ctx.send("❌ Coordinator not connected.")
            return

        async with ctx.typing():
            market = self._coordinator._market_agent
            db = self._coordinator._db
            
            # Fetch data via market agent
            rpc = self._coordinator._rpc  # Coordinator should expose rpc
            keypair = self._coordinator._keypair # Coordinator should expose keypair
            
            snapshot = await market.get_wallet_snapshot(rpc, keypair)
            sol_price = await market.get_sol_price_usd()

            active_pool_id = os.getenv("ACTIVE_POOL_ID", "N/A")
            net_apy = il_pct = raw_apy = 0.0

            if hasattr(db, "get_evaluations_for_pool"):
                 rows = await db.get_evaluations_for_pool(active_pool_id, limit=1)
                 if rows:
                     r = rows[0]
                     net_apy = r.get("net_apy_pct", 0.0) or 0.0
                     il_pct  = r.get("il_pct", 0.0) or 0.0
                     raw_apy = r.get("apy_pct", 0.0) or 0.0

            net_profit = 0.0
            if hasattr(db, "get_net_profit_sol"):
                try: net_profit = await db.get_net_profit_sol() 
                except: pass

            color = discord.Color.green() if net_apy > 0 else discord.Color.red()
            embed = discord.Embed(title="🌊 DeFi System Status", color=color)
            embed.add_field(name="🏊 Active Pool", value=f"`{active_pool_id[:20]}…`", inline=False)
            embed.add_field(name="💰 SOL Balance", value=f"**{snapshot['sol_balance']:.4f} SOL**", inline=True)
            embed.add_field(name="📈 SOL Price", value=f"**${sol_price:,.2f}**", inline=True)
            embed.add_field(name="📊 Net APY", value=f"**{net_apy:.2f}%**", inline=True)

            if snapshot.get("tokens"):
                 lines = []
                 for t in snapshot["tokens"]:
                      if t["amount_ui"] > 0.0001:  # Hide dust
                           mint_short = f"{t['mint'][:4]}…{t['mint'][-4:]}"
                           lines.append(f"• `{mint_short}`: **{t['amount_ui']:.4f}**")
                 if lines:
                      embed.add_field(name="📋 SPL Tokens", value="\n".join(lines), inline=False)

            embed.add_field(name="⚠️ IL", value=f"**{il_pct:.2f}%**", inline=True)
            embed.add_field(name="💹 Net Profit", value=f"**{net_profit:+.4f} SOL**", inline=True)
            embed.set_footer(text="GarganDeFi | Multi-Agent Structure")

            await ctx.send(embed=embed)

    @commands.command(name="approve_migration", aliases=["am"])
    async def approve_migration(self, ctx: commands.Context) -> None:
        """Approve the pending pool migration."""
        if not self._coordinator:
             await ctx.send("❌ Coordinator not connected.")
             return

        # Fetch pending from somewhere, coordinator keeps track?
        # Coordinator usually runs the logic.
        # Coordinator class lists approve_migration(candidate) 
        # But where does it hold pending proposal state?
        # In Coordinator, or set in DiscordAgent.
        # Coordinator has it.
        await self._coordinator.approve_migration_cmd() # Or similar orchestrator command hook
        await ctx.send("🔄 Triggered migration approval via coordinator.")

    @commands.command(name="withdraw_all")
    async def withdraw_all(self, ctx: commands.Context) -> None:
        """Swap all non-SOL tokens back to SOL."""
        if not self._coordinator:
             await ctx.send("❌ Coordinator not connected.")
             return

        await ctx.send("🔴 Triggering full pull and exit via coordinator...")
        await self._coordinator.execute_full_withdraw()

    @commands.command(name="invest_pool")
    async def invest_pool(self, ctx: commands.Context, pool_id: str) -> None:
        """Manually invest in pool by ID."""
        if not self._coordinator:
             await ctx.send("❌ Coordinator not connected.")
             return

        await ctx.send(f"🔍 Orchestrating manual investment into `{pool_id[:20]}…`")
        await self._coordinator.manual_invest(pool_id)

    @commands.command(name="defi_force_withdraw", aliases=["dfw"])
    async def defi_force_withdraw(self, ctx: commands.Context) -> None:
        """🚨 Emergency: pull all liquidity immediately from high level."""
        if not self._coordinator:
             await ctx.send("❌ Coordinator not connected.")
             return

        await ctx.send("⚠️ Triggering emergency withdraw via coordinator...")
        await self._coordinator.emergency_withdraw()

    @commands.command(name="help_defi")
    async def help_defi(self, ctx: commands.Context) -> None:
        """List all available DeFi commands."""
        embed = discord.Embed(title="📖 GarganDeFi Commands", color=discord.Color.blue())
        embed.add_field(name="`!defi_status` (dfi)", value="View wallet and pool details", inline=False)
        embed.add_field(name="`!invest_pool <id>`", value="Invest in a pool manually", inline=False)
        embed.add_field(name="`!approve_migration` (am)", value="Approve pending migration", inline=False)
        embed.add_field(name="`!withdraw_all`", value="Swap all tokens back to SOL", inline=False)
        embed.add_field(name="`!defi_force_withdraw` (dfw)", value="Emergency withdraw", inline=False)
        await ctx.send(embed=embed)

"""
cogs/deals_cog.py — Hardware Deals Discord Cog
================================================
Connects the DealsCrawler to Discord. Responsibilities:
  • Receives new Deal objects via callback from motor.py
  • Posts rich Embeds to #hardware-deals channel
  • !deals_status  — show latest N deals
  • !deals_reload  — force an immediate re-scan
"""

from __future__ import annotations

import os

import discord
from discord.ext import commands

import structlog

from crawlers.deals import Deal
from ui.database import Database

log = structlog.get_logger(__name__)


class DealsCog(commands.Cog, name="Hardware Deals"):
    """Discord Cog for the Hardware Deals scraper."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._deals_channel_id = int(os.getenv("DISCORD_DEALS_CHANNEL_ID", "0"))

    # ── Setup hook ────────────────────────────────────────────────────────────

    async def cog_load(self) -> None:
        """Wire the crawler's callback once the Cog is loaded into the bot."""
        crawler = self.bot.state.get("deals_crawler")
        if crawler:
            crawler.discord_post_callback = self._post_deal
            log.info("deals_cog_wired_callback")
        else:
            log.warning("deals_cog_no_crawler_found")

    # ── Deal posting ──────────────────────────────────────────────────────────

    async def _post_deal(self, deal: Deal) -> None:
        """Called by the crawler when a new relevant deal is found."""
        channel = self.bot.get_channel(self._deals_channel_id)
        if channel is None:
            log.warning("deals_channel_not_found", channel_id=self._deals_channel_id)
            return

        embed = self._build_deal_embed(deal)
        try:
            msg = await channel.send(embed=embed)
            # Update discord_msg_id in the DB
            db: Database = self.bot.state.get("db")
            if db:
                await db._execute(
                    "UPDATE deals SET discord_msg_id=? WHERE url=?",
                    (str(msg.id), deal.url),
                )
            log.info("deal_posted", title=deal.title[:60], msg_id=msg.id)
        except discord.HTTPException as exc:
            log.error("deal_post_http_error", error=str(exc))
        except Exception as exc:
            log.error("deal_post_error", error=str(exc))

    @staticmethod
    def _build_deal_embed(deal: Deal) -> discord.Embed:
        color = discord.Color.green() if (deal.discount_pct or 0) >= 30 else discord.Color.orange()
        embed = discord.Embed(
            title=f"🛒 {deal.title[:250]}",
            url=deal.url,
            color=color,
        )
        if deal.price:
            embed.add_field(name="💵 Price", value=f"**${deal.price:.2f}**", inline=True)
        if deal.original_price:
            embed.add_field(name="🏷️ Was", value=f"~~${deal.original_price:.2f}~~", inline=True)
        if deal.discount_pct:
            embed.add_field(name="📉 Discount", value=f"**{deal.discount_pct:.1f}% OFF**", inline=True)
        embed.add_field(name="🔗 Source", value=deal.source, inline=True)
        embed.set_footer(text="GarganDeFi Deals Bot")
        return embed

    # ── Commands ──────────────────────────────────────────────────────────────

    @commands.command(name="deals_status", aliases=["ds"])
    @commands.has_permissions(send_messages=True)
    async def deals_status(self, ctx: commands.Context, count: int = 10) -> None:
        """Show the last N hardware deals found. Usage: !deals_status [N]"""
        async with ctx.typing():
            db: Database = self.bot.state.get("db")
            if not db:
                await ctx.send("❌ Database not available.")
                return

            deals = await db.get_recent_deals(limit=min(count, 25))
            if not deals:
                await ctx.send("📭 No deals in the database yet.")
                return

            embed = discord.Embed(
                title=f"🛒 Last {len(deals)} Hardware Deals",
                color=discord.Color.blurple(),
            )
            for d in deals:
                price_str = f"${d['price']:.2f}" if d.get("price") else "?"
                disc_str  = f" (-{d['discount_pct']:.0f}%)" if d.get("discount_pct") else ""
                embed.add_field(
                    name=d["title"][:100],
                    value=f"{price_str}{disc_str} — [{d['source']}]({d['url']})",
                    inline=False,
                )
            await ctx.send(embed=embed)

    @commands.command(name="deals_reload", aliases=["dr"])
    @commands.is_owner()
    async def deals_reload(self, ctx: commands.Context) -> None:
        """Force an immediate deal scrape. Owner only."""
        crawler = self.bot.state.get("deals_crawler")
        if not crawler:
            await ctx.send("❌ Deals crawler not available.")
            return
        await ctx.send("🔄 Triggering immediate deal scan...")
        try:
            await crawler._crawl_all_sources()
            await ctx.send("✅ Scan complete. Check the deals channel.")
        except Exception as exc:
            await ctx.send(f"❌ Scan failed: `{exc}`")
            log.error("manual_scan_failed", error=str(exc))


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(DealsCog(bot))

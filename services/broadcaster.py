"""
broadcaster.py — Freemium SaaS Broadcaster Module
===================================================
Handles public-facing content routing for the GarganDeFi bot.

Push to designated Discord channel IDs:
- Hardware deals (Public)
- Teaser Signals (Public / Free)
- Actionable Signals (VIP)

Stripe/roles are managed externally (e.g., Whop) so this bot only
needs to push payloads to the correct channels.
"""

import os
import discord
import structlog
from typing import Optional

log = structlog.get_logger(__name__)

async def send_hardware_deal(bot: discord.Client, deal: dict, affiliate_tag: str) -> bool:
    """Send a hardware deal to the public deals channel."""
    channel_id_str = os.getenv("DISCORD_PUBLIC_DEALS_ID", "0")
    if not channel_id_str.isdigit():
        return False
        
    channel_id = int(channel_id_str)
    channel = bot.get_channel(channel_id)
    if not channel:
        log.warning("public_deals_channel_not_found", channel_id=channel_id)
        return False

    url = deal.get('url', '')
    if affiliate_tag and '?' in url:
        url += f"&tag={affiliate_tag}"
    elif affiliate_tag:
        url += f"?tag={affiliate_tag}"

    embed = discord.Embed(
        title=f"🚨 New Hardware Deal: {deal.get('title', 'Unknown')}",
        description=f"**Discount:** {deal.get('discount_pct', 0)}%\n[Buy Now]({url})",
        color=discord.Color.green()
    )
    embed.add_field(name="Price", value=f"~~${deal.get('old_price', '0')}~~ **${deal.get('new_price', '0')}**", inline=False)
    embed.set_footer(text="GarganDeFi Hardware Deals")
    
    try:
        await channel.send(embed=embed)
        return True
    except discord.HTTPException as e:
        log.error("send_hardware_deal_failed", error=str(e))
        return False

async def send_teaser_signal(bot: discord.Client, alert: dict) -> bool:
    """Send a teaser signal to the public signals channel."""
    channel_id_str = os.getenv("DISCORD_PUBLIC_SIGNALS_ID", "0")
    if not channel_id_str.isdigit():
        return False
        
    channel_id = int(channel_id_str)
    channel = bot.get_channel(channel_id)
    if not channel:
        log.warning("public_signals_channel_not_found", channel_id=channel_id)
        return False

    direction = "Bullish" if alert.get('target_price', 0) > alert.get('current_price', 0) else "Bearish"
    ticker = alert.get('ticker', 'Unknown')
    conf = alert.get('confidence_score', 0.0)
    
    embed = discord.Embed(
        title=f"👀 Free Quant Teaser: {ticker}",
        description=f"**{direction}** setup on **${ticker}** detected.\n"
                    f"Confidence: **{conf:.1f}%**.\n\n"
                    "🔒 *Want the exact Entry, Stop-Loss, and Target Price?*\n"
                    "***Subscribe to VIP to get full actionable signals!***",
        color=discord.Color.dark_grey()
    )
    embed.set_footer(text="GarganDeFi Quant Engine — Free Tier")

    try:
        await channel.send(embed=embed)
        return True
    except discord.HTTPException as e:
        log.error("send_teaser_signal_failed", error=str(e))
        return False

async def send_vip_signal(bot: discord.Client, alert: dict, view: Optional[discord.ui.View] = None) -> bool:
    """Send an actionable signal to the VIP signals channel."""
    channel_id_str = os.getenv("DISCORD_VIP_SIGNALS_ID", "0")
    if not channel_id_str.isdigit():
        return False
        
    channel_id = int(channel_id_str)
    channel = bot.get_channel(channel_id)
    if not channel:
        log.warning("vip_signals_channel_not_found", channel_id=channel_id)
        return False

    ticker = alert.get('ticker', 'Unknown')
    embed = discord.Embed(
        title=f"💎 VIP Quant Actionable Signal: {ticker}",
        description=f"Confidence Score: **{alert.get('confidence_score', 0):.1f}%** (High Probability)",
        color=discord.Color.gold() if alert.get('asset_type') == "CRYPTO" else discord.Color.blue()
    )
    
    # Actionable data
    embed.add_field(name="Current Price", value=f"`${alert.get('current_price', 0):.4f}`", inline=True)
    embed.add_field(name="Target Price", value=f"`${alert.get('target_price', 0):.4f}`", inline=True)
    embed.add_field(name="Stop Loss", value=f"`${alert.get('stop_loss', 0):.4f}`", inline=True)
    
    # TA Logic
    embed.add_field(name="Tech Score", value=f"`{alert.get('tech_raw_score', 0):.2f}`", inline=True)
    embed.add_field(name="NLP Sentiment", value=f"`{alert.get('sentiment_score', 0):.2f}` (-1 to 1)", inline=True)
    embed.add_field(name="Bayesian Weight", value="`60% Tech / 40% NLP`", inline=True)

    inds = alert.get('indicators', {})
    if inds:
        embed.add_field(name="RSI (14)", value=f"`{inds.get('RSI', 'N/A')}`", inline=True)
        embed.add_field(name="MACD", value=f"`{inds.get('MACD', 'N/A')}` / Sig: `{inds.get('MACD_Signal', 'N/A')}`", inline=True)
        embed.add_field(name="Bollinger Band", value=f"`${inds.get('BB_Lower', 'N/A')}` - `${inds.get('BB_Upper', 'N/A')}`", inline=True)

    embed.set_footer(text="GarganDeFi Quant Engine — VIP Exclusive")

    try:
        if view:
            await channel.send(embed=embed, view=view)
        else:
            await channel.send(embed=embed)
        return True
    except discord.HTTPException as e:
        log.error("send_vip_signal_failed", error=str(e))
        return False

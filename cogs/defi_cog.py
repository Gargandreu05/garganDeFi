"""
cogs/defi_cog.py — DeFi HITL Discord Cog
==========================================
Commands:
  !defi_status          — Show active pool, balance, net APY, IL
  !defi_force_withdraw  — Emergency: pull all liquidity immediately  (owner only)
  !approve_migration    — Approve a pending pool migration (owner only)

HITL alert flow (AUTONOMOUS_POOL_SWITCHING=false):
  1. pool_scanner finds candidate pool → calls discord_alert_callback
  2. This cog sends a rich Embed to #defi-alerts with ✅/❌ buttons
  3. Owner clicks ✅ (or sends !approve_migration) → migration executes
  4. On success, .env is updated with new pool IDs via dotenv.set_key()
    → Survives a system reboot.

All DeFi operations are wrapped in try/except so a Solana RPC failure
cannot crash the Discord bot.
"""

from __future__ import annotations

import asyncio
import os
from typing import Optional

import aiohttp
import discord
from discord.ext import commands, tasks
from dotenv import set_key, find_dotenv

import structlog

from defi_engine.pool_scanner import PoolScanner
from defi_engine.jupiter_zap import JupiterZap
from defi_engine.execution import RaydiumExecutor
from defi_engine.math_engine import compute_zap_amounts, LAMPORTS_PER_SOL
from defi_engine.quant_engine import QuantEngine
from ui.database import Database
from ui.graph_maker import GraphMaker
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

log = structlog.get_logger(__name__)

# Attempt to load the C++ optimized math engine, fallback to Python version if unavailable
try:
    import core_math
    C_MATH_AVAILABLE = True
    log.info("core_math C++ module loaded successfully.")
except ImportError:
    C_MATH_AVAILABLE = False
    log.warning("core_math C++ module unavailable. Falling back to Python math engine.")


def _rpc_client() -> AsyncClient:
    url = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    return AsyncClient(url)


class _MigrationView(discord.ui.View):
    """Interactive buttons for HITL migration approval."""

    def __init__(self, cog: "DeFiCog", candidate: dict, timeout: float = 3600) -> None:
        super().__init__(timeout=timeout)
        self._cog = cog
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
        await self._cog._execute_migration(self._candidate, interaction.channel)

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
        scanner: PoolScanner = self._cog.bot.state.get("pool_scanner")
        if scanner:
            scanner.clear_pending_migration()
        await interaction.response.send_message("✅ Migration dismissed. Staying in current pool.")

class _TradeIdeaView(discord.ui.View):
    """Interactive buttons for Quant trade approvals."""

    def __init__(self, cog: "DeFiCog", alert: dict, timeout: float = 3600) -> None:
        super().__init__(timeout=timeout)
        self._cog = cog
        self._alert = alert
        self._decided = False

    @discord.ui.button(label="Approve Trade", style=discord.ButtonStyle.success)
    async def approve(self, interaction: discord.Interaction, _btn: discord.ui.Button) -> None:
        owner_id = int(os.getenv("DISCORD_OWNER_ID", "0"))
        if interaction.user.id != owner_id:
            await interaction.response.send_message("⛔ Only the owner can approve trades.", ephemeral=True)
            return
        if self._decided:
            await interaction.response.send_message("⚠️ Already decided.", ephemeral=True)
            return
        self._decided = True
        self.stop()
        await interaction.response.defer()
        await interaction.followup.send(f"✅ Trade approved for {self._alert['ticker']}. (Execution logic pending)")

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.secondary)
    async def decline(self, interaction: discord.Interaction, _btn: discord.ui.Button) -> None:
        owner_id = int(os.getenv("DISCORD_OWNER_ID", "0"))
        if interaction.user.id != owner_id:
            await interaction.response.send_message("⛔ Only the owner can decline trades.", ephemeral=True)
            return
        if self._decided:
            await interaction.response.send_message("⚠️ Already decided.", ephemeral=True)
            return
        self._decided = True
        self.stop()
        await interaction.response.send_message(f"❌ Trade for {self._alert['ticker']} declined.")

class DeFiCog(commands.Cog, name="DeFi Engine"):
    """Discord Cog for the DeFi HITL trading engine."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        # Multi-channel priority routing
        self._private_defi_channel_id = int(os.getenv("DISCORD_PRIVATE_DEFI_CHANNEL_ID", "0"))
        
        self._quant_engine = QuantEngine()
        self._graph_maker = GraphMaker()
        self._owner_id = int(os.getenv("DISCORD_OWNER_ID", "0"))
        self._top_tier_discovery.start()
        self._quant_screener.start()

    def cog_unload(self):
        self._top_tier_discovery.cancel()
        self._quant_screener.cancel()

    @tasks.loop(minutes=5)
    async def _top_tier_discovery(self) -> None:
        """Top-Tier Discovery logic: Query Raydium’s API for top 5 volume SOL pools."""
        try:
            active_pool_id = os.getenv("ACTIVE_POOL_ID")
            if not active_pool_id:
                return

            api_url = "https://api.raydium.io/v2/main/pool"
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, timeout=15) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()

            sol_pools = [p for p in data if p.get("baseMint") == "So11111111111111111111111111111111111111112" or p.get("quoteMint") == "So11111111111111111111111111111111111111112"]
            sol_pools.sort(key=lambda x: x.get("volume24h", 0), reverse=True)
            top_pools = sol_pools[:5]
            if not top_pools:
                return

            top_pair = top_pools[0]
            top_apy = top_pair.get("apr30d", 0) or top_pair.get("apr7d", 0) or top_pair.get("apr", 0) or 0
            
            active_pool = next((p for p in data if p.get("ammId") == active_pool_id), None)
            active_apy = 0
            if active_pool:
                active_apy = active_pool.get("apr30d", 0) or active_pool.get("apr7d", 0) or active_pool.get("apr", 0) or 0

            # Trigger condition: active drops 15% below top
            # Leverage C++ math engine if available for these heavy comparisons
            if C_MATH_AVAILABLE:
                diff = core_math.calculate_apy_differential(top_apy, active_apy)
            else:
                diff = top_apy - active_apy

            if active_apy < top_apy * 0.85:
                candidate = {
                    "pool_id": top_pair.get("ammId"),
                    "name": top_pair.get("name", "Unknown Pool"),
                    "base": top_pair.get("baseMint"),
                    "quote": top_pair.get("quoteMint"),
                    "net_apy": top_apy,
                    "il_pct": 0.0
                }
                current = {
                    "pool_id": active_pool_id,
                    "name": active_pool.get("name", "Active Pool") if active_pool else "Active Pool",
                    "net_apy": active_apy,
                    "il_pct": 0.0
                }
                # Ensure we don't spam if a migration alert is pending
                scanner: PoolScanner = self.bot.state.get("pool_scanner")
                if scanner:
                    pending = scanner.get_pending_migration()
                    if pending and pending.get("pool_id") == candidate["pool_id"]:
                        return
                
                await self._send_migration_alert(
                    current=current, candidate=candidate, 
                    reason=f"Active APY ({active_apy:.2f}%) dropped 15%+ below Top Tier APY ({top_apy:.2f}%). Diff: {diff:.2f}%"
                )
        except Exception as exc:
            log.error("top_tier_discovery_failed", error=str(exc))

    @_top_tier_discovery.before_loop
    async def before_discovery(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=15)
    async def _quant_screener(self) -> None:
        """Quant Engine Screener logic: Fetch multi-asset indicators and score them."""
        try:
            alerts = await self._quant_engine.run_screener()
            if not alerts:
                return

            from services.broadcaster import send_teaser_signal, send_vip_signal

            for alert in alerts:
                # 1. Broadcast Freemium Teaser
                await send_teaser_signal(self.bot, alert)
                
                # 2. Broadcast Actionable Signal to VIP (and Owner can approve via View if crypto)
                view = None
                if alert['asset_type'] == "CRYPTO":
                    view = _TradeIdeaView(cog=self, alert=alert)
                
                await send_vip_signal(self.bot, alert, view=view)

        except Exception as exc:
            log.error("quant_screener_failed", error=str(exc))

    @_quant_screener.before_loop
    async def before_quant_screener(self):
        await self.bot.wait_until_ready()

    # ── Cog setup ─────────────────────────────────────────────────────────────

    async def cog_load(self) -> None:
        db: Database = self.bot.state.get("db")
        if db:
            # Satisfy the "Fresh Start" requirement: clear previous eval data
            await db.clear_all_evaluations()
            log.info("defi_cog_wiped_old_data_for_fresh_start")

        scanner: PoolScanner = self.bot.state.get("pool_scanner")
        if scanner:
            scanner.discord_alert_callback  = self._send_migration_alert
            scanner.migration_callback      = self._auto_migrate  # only used if AUTONOMOUS=true
            log.info("defi_cog_wired_scanner")
        else:
            log.warning("defi_cog_no_scanner_found")

    # ── Owner guard ───────────────────────────────────────────────────────────

    async def cog_check(self, ctx: commands.Context) -> bool:
        """All DeFi commands require the bot owner. Strict Private Wealth Manager boundary."""
        if not self._is_owner(ctx.author):
            await ctx.send("⛔ Only the owner can execute financial commands.")
            return False
        return True

    def _is_owner(self, ctx_or_user) -> bool:
        uid = ctx_or_user.id if hasattr(ctx_or_user, "id") else ctx_or_user
        return uid == self._owner_id

    # ── !report ───────────────────────────────────────────────────────────────
    
    @commands.command(name="report")
    async def report(self, ctx: commands.Context) -> None:
        """Generate a visual portfolio report (Owner only)."""
        if not self._is_owner(ctx.author):
            await ctx.send("⛔ Only the bot owner can fetch proprietary visual reports.")
            return
            
        async with ctx.typing():
            db: Database = self.bot.state.get("db")
            if not db:
                await ctx.send("❌ Database not connected.")
                return

            # Fetch 24h profit metrics
            net_profit = await db.get_net_profit_sol()
            
            # Generate the hacker-themed chart
            chart_buffer = await self._graph_maker.generate_portfolio_chart(db)
            
            if chart_buffer is None:
                await ctx.send("❌ Not enough data to generate visual report.")
                return
                
            # Create a discord file object to be uploaded as attachment
            file = discord.File(fp=chart_buffer, filename="portfolio_chart.png")

            # Build Embed
            embed = discord.Embed(
                title="📊 Executive Portfolio Report",
                description="Visual performance analysis powered by GarganDeFi Quant Engine.",
                color=0x00ffcc # Neon Cyan to match chart primary color
            )
            embed.add_field(name="Network", value="`Solana Mainnet-Beta`", inline=True)
            embed.add_field(name="Active Strategies", value="`Raydium LP` / `Quant Screen`", inline=True)
            embed.add_field(name="Net Profit (All Time)", value=f"`{net_profit:+.4f} SOL`", inline=False)
            
            # Embed the generated file as an image inside the Embed
            embed.set_image(url="attachment://portfolio_chart.png")
            embed.set_footer(text="Confidential Hacker Analytics — Restricted Access")

            await ctx.send(embed=embed, file=file)

    # ── !defi_status ──────────────────────────────────────────────────────────

    @commands.command(name="defi_status", aliases=["dfi"])
    async def defi_status(self, ctx: commands.Context) -> None:
        """Show active pool metrics, wallet balance, and recent trades."""
        async with ctx.typing():
            embed = await self._build_status_embed()
        await ctx.send(embed=embed)

    async def _build_status_embed(self) -> discord.Embed:
        db: Database = self.bot.state.get("db")
        pool_id   = os.getenv("ACTIVE_POOL_ID", "N/A")
        pool_name_env = "SOL/USDC"  # Default — scanner keeps this aligned

        # ── Wallet balance ────────────────────────────────────────────────────
        sol_balance = 0.0
        rpc = _rpc_client()
        try:
            async with JupiterZap(rpc) as zap:
                sol_balance = await zap.get_wallet_sol_balance()
        except Exception as exc:
            log.error("status_balance_fetch_failed", error=str(exc))
        finally:
            await rpc.close()

        # ── Latest pool evaluation ────────────────────────────────────────────
        net_apy = il_pct = raw_apy = 0.0
        if db:
            rows = await db.get_evaluations_for_pool(pool_id, limit=1)
            if rows:
                r = rows[0]
                net_apy = r.get("net_apy_pct", 0.0) or 0.0
                il_pct  = r.get("il_pct", 0.0) or 0.0
                raw_apy = r.get("apy_pct", 0.0) or 0.0

        # ── Net profit ────────────────────────────────────────────────────────
        net_profit = 0.0
        if db:
            try:
                net_profit = await db.get_net_profit_sol()
            except Exception:
                pass

        # ── Pending migration ─────────────────────────────────────────────────
        scanner: PoolScanner = self.bot.state.get("pool_scanner")
        pending = scanner.get_pending_migration() if scanner else None

        color = discord.Color.green() if net_apy > 0 else discord.Color.red()
        embed = discord.Embed(
            title="🌊 DeFi Engine Status",
            color=color,
        )
        embed.add_field(name="🏊 Active Pool",   value=f"`{pool_id[:20]}…`\n**{pool_name_env}**", inline=False)
        embed.add_field(name="💰 SOL Balance",   value=f"**{sol_balance:.4f} SOL**",               inline=True)
        embed.add_field(name="📈 Gross APY",     value=f"**{raw_apy:.2f}%**",                      inline=True)
        embed.add_field(name="📊 Net APY",       value=f"**{net_apy:.2f}%**",                      inline=True)
        embed.add_field(name="⚠️ IL",            value=f"**{il_pct:.2f}%**",                       inline=True)
        embed.add_field(name="💹 Net Profit",    value=f"**{net_profit:+.4f} SOL**",               inline=True)
        embed.add_field(
            name="🔄 HITL Mode",
            value="ON (manual approval)" if os.getenv("AUTONOMOUS_POOL_SWITCHING", "false").lower() != "true" else "OFF (autonomous)",
            inline=True,
        )
        if pending:
            embed.add_field(
                name="⏳ Pending Migration",
                value=f"→ **{pending['name']}** ({pending['net_apy']:.2f}% net APY)\nUse `!approve_migration` to execute.",
                inline=False,
            )
        embed.set_footer(text="GarganDeFi | Solana DeFi Bot")
        return embed

    # ── !defi_force_withdraw ──────────────────────────────────────────────────

    @commands.command(name="defi_force_withdraw", aliases=["dfw"])
    async def defi_force_withdraw(self, ctx: commands.Context) -> None:
        """🚨 Emergency: immediately remove ALL liquidity from the active pool. Owner only."""
        if not self._is_owner(ctx.author):
            await ctx.send("⛔ Only the bot owner can force a withdrawal.")
            return

        await ctx.send("⚠️ **EMERGENCY WITHDRAW INITIATED** — pulling all liquidity from active pool…")

        pool_id  = os.getenv("ACTIVE_POOL_ID", "")
        db: Database = self.bot.state.get("db")
        rpc = _rpc_client()
        trade_id = None

        try:
            async with RaydiumExecutor(rpc) as executor:
                # Find our LP token balance — use pool's lpMint
                # (For a real deployment, store lp_mint in .env or db)
                lp_mint = os.getenv("ACTIVE_LP_MINT", "")
                if not lp_mint:
                    await ctx.send("❌ `ACTIVE_LP_MINT` not set in .env. Cannot determine LP balance.")
                    return

                lp_balance = await executor.get_lp_token_balance(lp_mint)
                if lp_balance == 0:
                    await ctx.send("ℹ️ No LP tokens found — wallet may not be in any pool.")
                    return

                if db:
                    trade_id = await db.insert_trade(
                        trade_type="WITHDRAW",
                        pool_id=pool_id,
                        pool_name="Active Pool",
                        status="PENDING",
                    )

                sig = await executor.remove_liquidity(pool_id=pool_id, lp_amount=lp_balance)

                if db and trade_id:
                    await db.update_trade_status(trade_id, "CONFIRMED", sig)

                await ctx.send(
                    f"✅ **Withdrawal Complete!**\n"
                    f"LP Tokens redeemed: `{lp_balance}`\n"
                    f"TX: `{sig}`"
                )

        except Exception as exc:
            log.error("force_withdraw_failed", error=str(exc))
            if db and trade_id:
                await db.update_trade_status(trade_id, "FAILED")
            await ctx.send(f"❌ Withdrawal failed: `{exc}`")
        finally:
            await rpc.close()

    # ── !approve_migration ────────────────────────────────────────────────────

    @commands.command(name="approve_migration", aliases=["am"])
    async def approve_migration(self, ctx: commands.Context) -> None:
        """Approve the pending pool migration proposal. Owner only."""
        if not self._is_owner(ctx.author):
            await ctx.send("⛔ Only the bot owner can approve migrations.")
            return

        scanner: PoolScanner = self.bot.state.get("pool_scanner")
        candidate = scanner.get_pending_migration() if scanner else None

        if not candidate:
            await ctx.send("ℹ️ No pending migration proposal. Wait for the scanner to find a better pool.")
            return

        await ctx.send(
            f"🔄 Executing migration to **{candidate['name']}** "
            f"(Net APY: **{candidate['net_apy']:.2f}%**)…"
        )
        await self._execute_migration(candidate, ctx.channel)

    @commands.command(name="withdraw_all")
    async def withdraw_all(self, ctx: commands.Context) -> None:
        """Emergency or manual full exit: swap all non-SOL tokens back to SOL. Owner only."""
        if not self._is_owner(ctx.author):
            await ctx.send("⛔ Only the bot owner can withdraw.")
            return

        await ctx.send("🔴 **Manual withdrawal initiated.** Swapping all tokens back to SOL…")
        rpc = _rpc_client()
        db: Database = self.bot.state.get("db")

        try:
            async with JupiterZap(rpc) as zap:
                # Check SOL balance first
                sol_balance = await zap.get_wallet_sol_balance()
                await ctx.send(f"💰 Current SOL balance: `{sol_balance:.4f} SOL`")

                # Get all token accounts with balance
                from solana.rpc.types import TokenAccountOpts
                token_accounts_resp = await rpc.get_token_accounts_by_owner(
                    zap._keypair.pubkey(),
                    TokenAccountOpts(
                        program_id=Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
                        encoding="jsonParsed"
                    ),
                )
                
                token_accounts = token_accounts_resp.value
                if not token_accounts:
                    await ctx.send("ℹ️ No token accounts found. Wallet is already in SOL.")
                    return

                swapped_count = 0
                failed_count = 0
                for account in token_accounts:
                    try:
                        mint = str(account.account.data.parsed["info"]["mint"])
                        amount_raw = int(account.account.data.parsed["info"]["tokenAmount"]["amount"])
                        ui_amount = float(account.account.data.parsed["info"]["tokenAmount"]["uiAmount"] or 0)

                        # Skip dust (less than $0.01 equivalent) and WSOL
                        if amount_raw == 0 or mint == "So11111111111111111111111111111111111111112":
                            continue
                        if ui_amount < 0.01:
                            continue

                        await ctx.send(f"  🔀 Swapping `{ui_amount:.4f}` of `{mint[:20]}…` → SOL…")
                        
                        result = await zap.zap_token_to_sol(
                            token_mint=mint,
                            token_amount_raw=amount_raw,
                        )
                        
                        if db:
                            await db.insert_trade(
                                trade_type="WITHDRAW",
                                pool_id=os.getenv("ACTIVE_POOL_ID", ""),
                                pool_name="Manual Withdrawal",
                                amount_token=ui_amount,
                                token_mint=mint,
                                tx_signature=result["tx_signature"],
                                status="CONFIRMED",
                            )
                        
                        await ctx.send(
                            f"  ✅ Swapped → `{result['sol_received']:.4f} SOL`\n"
                            f"  TX: `{result['tx_signature'][:30]}…`"
                        )
                        swapped_count += 1

                    except Exception as exc:
                        log.error("withdraw_token_swap_failed", error=str(exc))
                        await ctx.send(f"  ⚠️ Failed to swap token: `{exc}`")
                        failed_count += 1
                        continue

                final_sol = await zap.get_wallet_sol_balance()
                if swapped_count == 0 and failed_count == 0:
                    await ctx.send("ℹ️ Nothing to withdraw — wallet already in SOL.")
                elif swapped_count == 0 and failed_count > 0:
                    await ctx.send(
                        f"⚠️ **Withdrawal incomplete** — {failed_count} token(s) failed to swap. "
                        f"Check logs and retry `!withdraw_all`."
                    )
                else:
                    await ctx.send(
                        f"✅ **Withdrawal complete!**\n"
                        f"Swapped `{swapped_count}` token(s) back to SOL.\n"
                        f"Final balance: `{final_sol:.4f} SOL`"
                    )

        except Exception as exc:
            log.critical("withdraw_all_failed", error=str(exc))
            await ctx.send(f"💥 Withdrawal failed: `{exc}`")
        finally:
            await rpc.close()

    @commands.command(name="invest_pool")
    async def invest_pool(self, ctx: commands.Context, pool_id: str) -> None:
        """
        Manually invest into a specific pool by its DexScreener pair address.
        Usage: !invest_pool <pair_address>
        Example: !invest_pool GipgW6bsrDKaJkaYFPEkrQVfCqTexP4aP4aDTP4WEsN6
        Owner only.
        """
        if not self._is_owner(ctx.author):
            await ctx.send("⛔ Only the bot owner can manually invest.")
            return

        await ctx.send(f"🔍 Looking up pool `{pool_id[:20]}…`")

        # Fetch pool data from DexScreener to validate it exists
        scanner: PoolScanner = self.bot.state.get("pool_scanner")
        pool_data = None
        pool_name = "Unknown Pool"
        try:
            import aiohttp as _aiohttp
            async with _aiohttp.ClientSession() as session:
                url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pool_id}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        pairs = data.get("pairs", [])
                        if pairs:
                            p = pairs[0]
                            base = p.get("baseToken", {}).get("symbol", "?")
                            quote = p.get("quoteToken", {}).get("symbol", "?")
                            pool_name = f"{base}/{quote}"
                            liq = float(p.get("liquidity", {}).get("usd", 0) or 0)
                            vol = float(p.get("volume", {}).get("h24", 0) or 0)
                            pool_data = p
                            await ctx.send(
                                f"✅ Pool found: **{pool_name}**\n"
                                f"💧 Liquidity: `${liq:,.0f}` | 📊 24h Vol: `${vol:,.0f}`\n"
                                f"Proceeding with investment…"
                            )
        except Exception as exc:
            await ctx.send(f"⚠️ Could not verify pool on DexScreener: `{exc}`\nProceeding anyway…")

        # Determine quote mint — default to USDC
        quote_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        if pool_data:
            quote_token = pool_data.get("quoteToken", {})
            quote_addr = quote_token.get("address", "")
            if quote_addr and quote_addr != "So11111111111111111111111111111111111111112":
                quote_mint = quote_addr

        candidate = {
            "pool_id": pool_id,
            "name": pool_name,
            "net_apy": 0.0,
            "il_pct": 0.0,
        }

        # Update env so the bot tracks this as the active pool
        from dotenv import set_key, find_dotenv
        dotenv_path = find_dotenv(usecwd=True) or ".env"
        set_key(dotenv_path, "ACTIVE_POOL_ID", pool_id)
        set_key(dotenv_path, "ACTIVE_POOL_QUOTE_MINT", quote_mint)
        os.environ["ACTIVE_POOL_ID"] = pool_id
        os.environ["ACTIVE_POOL_QUOTE_MINT"] = quote_mint

        await self._execute_migration(candidate, ctx.channel)

    # ── Migration execution ───────────────────────────────────────────────────

    async def _execute_migration(self, candidate: dict, channel: discord.abc.Messageable) -> None:
        """
        Full migration flow:
          1. Withdraw from current pool
          2. Jupiter Zap SOL → Quote Token (50/50)
          3. Deposit into new pool
          4. Update .env with new pool addresses (dotenv set_key)
        """
        db: Database = self.bot.state.get("db")
        old_pool_id  = os.getenv("ACTIVE_POOL_ID", "")
        new_pool_id  = candidate["pool_id"]
        new_pool_name = candidate["name"]
        quote_mint   = os.getenv("ACTIVE_POOL_QUOTE_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
        rpc = _rpc_client()

        try:
            # ── Step 1: Withdraw from old pool ─────────────────────────────
            await channel.send(f"**[1/4]** 🔴 Withdrawing from current pool `{old_pool_id[:20]}…`")
            withdraw_trade_id = None
            try:
                async with RaydiumExecutor(rpc) as executor:
                    lp_mint = os.getenv("ACTIVE_LP_MINT", "")
                    if lp_mint:
                        lp_balance = await executor.get_lp_token_balance(lp_mint)
                        if lp_balance > 0:
                            if db:
                                withdraw_trade_id = await db.insert_trade(
                                    trade_type="WITHDRAW",
                                    pool_id=old_pool_id,
                                    pool_name="Old Pool",
                                    status="PENDING",
                                )
                            sig = await executor.remove_liquidity(pool_id=old_pool_id, lp_amount=lp_balance)
                            if db and withdraw_trade_id:
                                await db.update_trade_status(withdraw_trade_id, "CONFIRMED", sig)
                            await channel.send(f"  ✅ Withdrawn. TX: `{sig[:30]}…`")
                        else:
                            await channel.send("  ℹ️ No LP tokens in current pool — skipping withdraw.")
                    else:
                        await channel.send("  ⚠️ `ACTIVE_LP_MINT` not set — skipping withdraw step.")
            except Exception as exc:
                log.error("migration_withdraw_failed", error=str(exc))
                await channel.send(f"  ⚠️ Withdraw failed: `{exc}` — continuing with available SOL.")

            # Small pause for settlement
            await asyncio.sleep(5)

            # ── Step 2: Smart Jupiter Zap (only if needed) ────────────────────────
            await channel.send("**[2/4]** 🔍 Checking wallet balances before swapping…")
            zap_result = None
            swap_trade_id = None
            try:
                async with JupiterZap(rpc) as zap:
                    total_sol = await zap.get_wallet_sol_balance()

                    # BUG 3 FIX: check MIN_INVEST_SOL
                    gas_reserve = float(os.getenv("GAS_RESERVE_SOL", "0.02"))
                    deployable = total_sol - gas_reserve
                    min_invest = float(os.getenv("MIN_INVEST_SOL", "0.05"))

                    if deployable < min_invest:
                        await channel.send(
                            f"❌ **Migration aborted**: Deployable balance (`{deployable:.4f} SOL`) is "
                            f"below the minimum investment threshold (`{min_invest:.2f} SOL`).\n"
                            f"Please add more SOL to your wallet and try again."
                        )
                        return

                    # Check existing USDC/quote token balance
                    existing_quote_raw = 0
                    existing_quote_ui = 0.0
                    try:
                        from solana.rpc.types import TokenAccountOpts
                        token_resp = await rpc.get_token_accounts_by_owner(
                            zap._keypair.pubkey(),
                            TokenAccountOpts(
                                program_id=Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
                                encoding="jsonParsed"
                            ),
                        )
                        for acc in (token_resp.value or []):
                            try:
                                info = acc.account.data.parsed["info"]
                                if info["mint"] == quote_mint:
                                    existing_quote_raw = int(info["tokenAmount"]["amount"])
                                    existing_quote_ui = float(info["tokenAmount"]["uiAmount"] or 0)
                                    break
                            except Exception:
                                continue
                    except Exception as exc:
                        log.warning("balance_check_failed", error=str(exc))

                    amounts = compute_zap_amounts(total_sol, gas_reserve)
                    sol_to_swap = amounts["sol_to_swap"]

                    # BUG 2 FIX: Real Jupiter quote for SOL/Quote price (Skip hardcoded estimate)
                    already_balanced = False
                    try:
                        price_quote = await zap._get_quote(
                            input_mint="So11111111111111111111111111111111111111112",
                            output_mint=quote_mint,
                            amount_lamports=LAMPORTS_PER_SOL  # 1 SOL
                        )
                        real_sol_price_raw = float(price_quote["outAmount"])
                        target_quote_raw = sol_to_swap * real_sol_price_raw
                        
                        # If existing quote covers >= 80% of what we'd swap, skip the swap
                        already_balanced = (
                            existing_quote_raw > 0 and
                            existing_quote_raw >= (target_quote_raw * 0.8)
                        )
                    except Exception as exc:
                        log.warning("real_price_fetch_failed", error=str(exc))
                        already_balanced = False

                    if already_balanced:
                        await channel.send(
                            f"  ✅ Wallet already balanced!\n"
                            f"  SOL: `{total_sol:.4f}` | Quote token: `{existing_quote_ui:.4f}`\n"
                            f"  Skipping swap — proceeding directly to deposit."
                        )
                        # Build a synthetic zap_result using existing balances
                        zap_result = {
                            "tx_signature": "SKIPPED_ALREADY_BALANCED",
                            "sol_swapped": 0.0,
                            "sol_for_base": amounts["sol_for_base"],
                            "gas_reserve": gas_reserve,
                            "quote_mint": quote_mint,
                            "out_amount_raw": existing_quote_raw,
                        }
                    else:
                        await channel.send(
                            f"  💱 Swapping `{sol_to_swap:.4f}` SOL → Quote Token via Jupiter…\n"
                            f"  (Current quote balance: `{existing_quote_ui:.4f}`)"
                        )
                        if db:
                            swap_trade_id = await db.insert_trade(
                                trade_type="SWAP",
                                pool_id=new_pool_id,
                                pool_name=new_pool_name,
                                status="PENDING",
                            )
                        zap_result = await zap.zap_sol_to_token(
                            quote_mint=quote_mint,
                            gas_reserve_sol=gas_reserve,
                        )
                        if db and swap_trade_id:
                            await db.update_trade_status(
                                swap_trade_id, "CONFIRMED", zap_result["tx_signature"]
                            )
                        await channel.send(
                            f"  ✅ Swap complete! SOL swapped: `{zap_result['sol_swapped']:.4f}`\n"
                            f"  TX: `{zap_result['tx_signature'][:30]}…`"
                        )

            except Exception as exc:
                log.error("migration_zap_failed", error=str(exc))
                if db and swap_trade_id:
                    await db.update_trade_status(swap_trade_id, "FAILED")
                await channel.send(
                    f"  ❌ Swap failed: `{exc}`\n"
                    f"  ⚠️ If you already have tokens in your wallet, use `!invest_pool <pool_id>` to deposit manually."
                )
                return

            await asyncio.sleep(5)

            # ── Step 3: Deposit into new pool ──────────────────────────────
            await channel.send(f"**[3/4]** 🟢 Adding liquidity to **{new_pool_name}**…")
            deposit_trade_id = None
            try:
                async with RaydiumExecutor(rpc) as executor:
                    base_lamports = int(zap_result["sol_for_base"] * LAMPORTS_PER_SOL)
                    quote_raw     = int(zap_result["out_amount_raw"])

                    if db:
                        deposit_trade_id = await db.insert_trade(
                            trade_type="DEPOSIT",
                            pool_id=new_pool_id,
                            pool_name=new_pool_name,
                            amount_sol=zap_result["sol_for_base"],
                            token_mint=quote_mint,
                            status="PENDING",
                        )

                    sig = await executor.add_liquidity(
                        pool_id=new_pool_id,
                        base_amount_lamports=base_lamports,
                        quote_amount=quote_raw,
                    )
                    if db and deposit_trade_id:
                        await db.update_trade_status(deposit_trade_id, "CONFIRMED", sig)

                    await channel.send(f"  ✅ Liquidity added! TX: `{sig[:30]}…`")

            except Exception as exc:
                log.error("migration_deposit_failed", error=str(exc))
                if db and deposit_trade_id:
                    await db.update_trade_status(deposit_trade_id, "FAILED")
                await channel.send(
                    f"  ❌ Deposit failed: `{exc}`\n"
                    "  Tokens remain in wallet — rerun `!approve_migration` or add liquidity manually."
                )
                return

            # ── Step 4: Update .env ────────────────────────────────────────
            await channel.send("**[4/4]** 💾 Persisting new pool config to `.env`…")
            try:
                from defi_engine.pool_scanner import POOL_WHITELIST
                pool_meta = POOL_WHITELIST.get(new_pool_id, {})

                dotenv_path = find_dotenv(usecwd=True) or ".env"
                set_key(dotenv_path, "ACTIVE_POOL_ID",         new_pool_id)
                set_key(dotenv_path, "ACTIVE_POOL_BASE_MINT",  pool_meta.get("base", ""))
                set_key(dotenv_path, "ACTIVE_POOL_QUOTE_MINT", pool_meta.get("quote", quote_mint))

                # In-process update (avoids requiring restart to reflect new env)
                os.environ["ACTIVE_POOL_ID"]         = new_pool_id
                os.environ["ACTIVE_POOL_BASE_MINT"]  = pool_meta.get("base", "")
                os.environ["ACTIVE_POOL_QUOTE_MINT"] = pool_meta.get("quote", quote_mint)

                await channel.send(f"  ✅ `.env` updated with `ACTIVE_POOL_ID={new_pool_id[:30]}…`")
            except Exception as exc:
                log.error("env_update_failed", error=str(exc))
                await channel.send(
                    f"  ⚠️ Could not update `.env`: `{exc}`\n"
                    "  **Manually set** `ACTIVE_POOL_ID` in `.env` before next reboot!"
                )

            # Clear pending migration
            scanner: PoolScanner = self.bot.state.get("pool_scanner")
            if scanner:
                scanner.clear_pending_migration()

            # Log alert
            if db:
                await db.insert_alert(
                    alert_type="POOL_MIGRATION",
                    message=f"Migrated to {new_pool_name} ({new_pool_id}). Net APY: {candidate['net_apy']:.2f}%",
                )

            await channel.send(
                f"🎉 **Migration to {new_pool_name} complete!**\n"
                f"Net APY: **{candidate['net_apy']:.2f}%**\n"
                f"Config saved. Bot will use new pool after reboot too. ✅"
            )

        except Exception as exc:
            log.critical("migration_unexpected_error", error=str(exc))
            await channel.send(f"💥 **UNEXPECTED ERROR** during migration: `{exc}`\nCheck logs immediately!")
        finally:
            await rpc.close()

    # ── Discord alert (from pool_scanner) ─────────────────────────────────────

    async def _send_migration_alert(
        self,
        current: Optional[dict],
        candidate: dict,
        reason: str,
    ) -> None:
        """Called by PoolScanner when a better pool is identified (HITL mode)."""
        channel = self.bot.get_channel(self._private_defi_channel_id)
        if channel is None:
            log.warning("private_defi_channel_not_found", channel_id=self._private_defi_channel_id)
            return

        # Data extraction
        liquidity = candidate.get("liquidity", 0)
        il_pct = candidate.get("il_pct", 0)
        vol_24h = candidate.get("vol_24h", 0)
        risk_reward = candidate.get("risk_reward", 0)
        
        # Calculations
        safety_score = min(100, int((liquidity / 1_000_000) * 30 + (100 - min(il_pct * 5, 50)) + 20))
        vol_liq_ratio = (vol_24h / liquidity * 100) if liquidity > 0 else 0.0

        # Safety Label
        if safety_score >= 80:
            safety_label = "🟢 Excellent"
        elif safety_score >= 60:
            safety_label = "🟡 Good"
        elif safety_score >= 40:
            safety_label = "🟠 Caution"
        else:
            safety_label = "🔴 Risky"

        # Risk/Reward Interpretation
        if risk_reward >= 100:
            rr_interpretation = "🔥 Exceptional — very high yield vs very low IL risk"
        elif risk_reward >= 50:
            rr_interpretation = "✅ Strong — good yield/risk balance"
        elif risk_reward >= 20:
            rr_interpretation = "⚠️ Moderate — acceptable but monitor IL"
        else:
            rr_interpretation = "🔴 Weak — high IL risk vs yield"

        title = "🚀 New Investment Opportunity" if current is None else "🔄 Pool Migration Alert"
        color = discord.Color.green() if safety_score >= 70 else discord.Color.gold()
        if "alert" in reason.lower() or "dropped" in reason.lower():
            color = discord.Color.red()

        embed = discord.Embed(
            title=title,
            description=f"**Reason:** {reason}",
            color=color,
        )
        embed.add_field(name="Pool", value=f"**{candidate['name']}**", inline=False)
        
        # Detailed Safety Score Breakdown
        embed.add_field(
            name="🛡️ Safety Score",
            value=(
                f"{safety_label} ({safety_score}/100)\n"
                f"💧 Liquidity: ${liquidity:,.0f}\n"
                f"📉 IL Risk: {il_pct:.2f}%\n"
                f"📊 Vol/Liq ratio: {vol_liq_ratio:.2f}%"
            ),
            inline=True
        )

        # Detailed Risk/Reward
        embed.add_field(
            name="⚖️ Risk/Reward",
            value=(
                f"Score: {risk_reward:.2f} (higher = better, avg blue-chip ~50)\n"
                f"{rr_interpretation}"
            ),
            inline=True
        )

        if current:
            net_gain = candidate['net_apy'] - current['net_apy']
            comparison = f"Current: {current['net_apy']:.2f}% → New: {candidate['net_apy']:.2f}% (+{net_gain:.2f}%)"
            embed.add_field(name="APY Comparison", value=comparison, inline=False)
        else:
            embed.add_field(name="Net APY", value=f"{candidate['net_apy']:.2f}%", inline=False)

        embed.set_footer(text="GarganDeFi | Click Approve to migrate")

        view = _MigrationView(cog=self, candidate=candidate)

        try:
            msg = await channel.send(embed=embed, view=view)
            db: Database = self.bot.state.get("db")
            if db:
                await db.insert_alert(
                    alert_type="POOL_RECOMMENDATION",
                    message=f"Candidate: {candidate['name']} | Net APY: {candidate['net_apy']:.2f}%",
                    discord_msg_id=str(msg.id),
                )
            log.info("migration_alert_sent", msg_id=msg.id, candidate=candidate["name"])
        except discord.HTTPException as exc:
            log.error("migration_alert_send_failed", error=str(exc))

    async def _auto_migrate(self, candidate: dict) -> None:
        """Called by scanner when AUTONOMOUS_POOL_SWITCHING=true. NOT the default."""
        log.warning("autonomous_migration_executing", pool=candidate["name"])
        channel = self.bot.get_channel(self._private_defi_channel_id)
        if channel:
            await channel.send(
                f"🤖 **AUTONOMOUS MODE**: Auto-migrating to **{candidate['name']}**…"
            )
        await self._execute_migration(candidate, channel)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(DeFiCog(bot))

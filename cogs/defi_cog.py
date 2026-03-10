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

import discord
from discord.ext import commands
from dotenv import set_key, find_dotenv

import structlog

from defi_engine.pool_scanner import PoolScanner
from defi_engine.jupiter_zap import JupiterZap
from defi_engine.execution import RaydiumExecutor
from defi_engine.math_engine import compute_zap_amounts, LAMPORTS_PER_SOL
from ui.database import Database
from solana.rpc.async_api import AsyncClient

log = structlog.get_logger(__name__)


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

    @discord.ui.button(label="✅ Approve Migration", style=discord.ButtonStyle.success)
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

    @discord.ui.button(label="❌ Reject", style=discord.ButtonStyle.danger)
    async def reject(self, interaction: discord.Interaction, _btn: discord.ui.Button) -> None:
        owner_id = int(os.getenv("DISCORD_OWNER_ID", "0"))
        if interaction.user.id != owner_id:
            await interaction.response.send_message("⛔ Only the owner can reject.", ephemeral=True)
            return
        if self._decided:
            await interaction.response.send_message("⚠️ Already decided.", ephemeral=True)
            return
        self._decided = True
        self.stop()
        scanner: PoolScanner = self._cog.bot.state.get("pool_scanner")
        if scanner:
            scanner.clear_pending_migration()
        await interaction.response.send_message("✅ Migration rejected. Staying in current pool.")


class DeFiCog(commands.Cog, name="DeFi Engine"):
    """Discord Cog for the DeFi HITL trading engine."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._defi_channel_id = int(os.getenv("DISCORD_DEFI_ALERTS_CHANNEL_ID", "0"))
        self._owner_id = int(os.getenv("DISCORD_OWNER_ID", "0"))

    # ── Cog setup ─────────────────────────────────────────────────────────────

    async def cog_load(self) -> None:
        scanner: PoolScanner = self.bot.state.get("pool_scanner")
        if scanner:
            scanner.discord_alert_callback  = self._send_migration_alert
            scanner.migration_callback      = self._auto_migrate  # only used if AUTONOMOUS=true
            log.info("defi_cog_wired_scanner")
        else:
            log.warning("defi_cog_no_scanner_found")

    # ── Owner guard ───────────────────────────────────────────────────────────

    async def cog_check(self, ctx: commands.Context) -> bool:
        """All DeFi commands require the bot owner unless overridden per-command."""
        return True  # Per-command checks below

    def _is_owner(self, ctx_or_user) -> bool:
        uid = ctx_or_user.id if hasattr(ctx_or_user, "id") else ctx_or_user
        return uid == self._owner_id

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

            # ── Step 2: Jupiter Zap ────────────────────────────────────────
            await channel.send(f"**[2/4]** 🔀 Swapping 50% SOL → Quote Token via Jupiter…")
            zap_result = None
            swap_trade_id = None
            try:
                async with JupiterZap(rpc) as zap:
                    if db:
                        swap_trade_id = await db.insert_trade(
                            trade_type="SWAP",
                            pool_id=new_pool_id,
                            pool_name=new_pool_name,
                            status="PENDING",
                        )
                    zap_result = await zap.zap_sol_to_token(
                        quote_mint=quote_mint,
                        gas_reserve_sol=float(os.getenv("GAS_RESERVE_SOL", "0.02")),
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
                await channel.send(f"  ❌ Swap failed: `{exc}`")
                await channel.send("❌ **Migration aborted at swap step.** Funds remain as SOL in wallet.")
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
        current: dict,
        candidate: dict,
        reason: str,
    ) -> None:
        """Called by PoolScanner when a better pool is identified (HITL mode)."""
        channel = self.bot.get_channel(self._defi_channel_id)
        if channel is None:
            log.warning("defi_alerts_channel_not_found", channel_id=self._defi_channel_id)
            return

        owner_mention = f"<@{self._owner_id}>" if self._owner_id else "Owner"
        embed = discord.Embed(
            title="🔔 Pool Migration Proposal",
            description=(
                f"{owner_mention} — the scanner found a better pool!\n\n"
                f"**Reason:** {reason}"
            ),
            color=discord.Color.gold(),
        )
        embed.add_field(
            name="📍 Current Pool",
            value=f"**{current['name']}**\nNet APY: `{current['net_apy']:.2f}%` | IL: `{current['il_pct']:.2f}%`",
            inline=True,
        )
        embed.add_field(
            name="✨ Candidate Pool",
            value=f"**{candidate['name']}**\nNet APY: `{candidate['net_apy']:.2f}%` | IL: `{candidate['il_pct']:.2f}%`",
            inline=True,
        )
        embed.set_footer(text="Click ✅ to migrate or ❌ to stay. This alert expires in 1 hour.")

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
        channel = self.bot.get_channel(self._defi_channel_id)
        if channel:
            await channel.send(
                f"🤖 **AUTONOMOUS MODE**: Auto-migrating to **{candidate['name']}**…"
            )
        await self._execute_migration(candidate, channel)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(DeFiCog(bot))

"""
config/system_monitor.py
========================
Enterprise-grade DevOps and Hardware Monitoring for Linux mini-PCs.
Provides continuous thermal tracking, auto-throttling of scrapers, 
and emergency shutdown capabilities.
"""

import os
import sys
import asyncio
import psutil
import structlog
import discord
from discord.ext import tasks, commands

log = structlog.get_logger(__name__)

# Thermal thresholds (Celsius)
WARNING_TEMP = 80.0
CRITICAL_TEMP = 90.0

class SystemMonitor:
    """Monitors CPU/RAM usage and thermal temperatures."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._defi_channel_id = int(os.getenv("DISCORD_DEFI_ALERTS_CHANNEL_ID", "0"))
        
        # State tracking
        self.is_throttled = False
        self.thermal_watchdog.start()

    def stop(self):
        """Cancel the background loop."""
        self.thermal_watchdog.cancel()

    async def get_cpu_temp(self) -> float:
        """
        Attempts to read CPU temperature natively in Linux (mini-PCs/Raspberry Pi)
        Falls back to psutil if available.
        """
        temp = 0.0
        # 1. Try native Linux thermal zone (most accurate for mini-PCs)
        try:
            val = await asyncio.to_thread(self._read_sys_temp)
            if val is not None:
                return val
        except Exception:
            pass

        # 2. Fallback to psutil
        try:
            temps = await asyncio.to_thread(psutil.sensors_temperatures)
            if not temps:
                return 0.0
            
            # Common sensor names for CPU Core temps
            for name in ['coretemp', 'k10temp', 'cpu_thermal', 'acpitz']:
                if name in temps:
                    return float(temps[name][0].current)
                    
            # If nothing specific is found, just grab the first available
            first_sensor = list(temps.values())[0]
            if first_sensor:
                return float(first_sensor[0].current)
        except Exception as e:
            log.warning("thermal_fallback_failed", error=str(e))
            
        return temp

    def _read_sys_temp(self) -> float | None:
        """Reads raw thermal zone value in Linux."""
        path = "/sys/class/thermal/thermal_zone0/temp"
        if os.path.exists(path):
            with open(path, "r") as f:
                # Value is in millidegrees Celsius
                return float(f.read().strip()) / 1000.0
        return None

    async def get_system_metrics(self) -> dict:
        """Fetch RAM, CPU %, and Temp asynchronously."""
        def fetchSync():
            cpu = psutil.cpu_percent(interval=None) # Non-blocking instantly
            mem = psutil.virtual_memory()
            return cpu, mem.percent, mem.used, mem.total
            
        cpu_pct, mem_pct, mem_used, mem_total = await asyncio.to_thread(fetchSync)
        temp = await self.get_cpu_temp()
        
        return {
            "cpu_percent": cpu_pct,
            "ram_percent": mem_pct,
            "ram_used_gb": mem_used / (1024**3),
            "ram_total_gb": mem_total / (1024**3),
            "cpu_temp": temp
        }

    @tasks.loop(seconds=60)
    async def thermal_watchdog(self) -> None:
        """Background task running every minute to monitor hardware health."""
        try:
            temp = await self.get_cpu_temp()
            # If temp is 0.0, sensors might be failing/unsupported, skip logic
            if temp <= 0.0:
                return

            log.debug("thermal_check", temp_c=temp)

            channel = self.bot.get_channel(self._defi_channel_id)

            # ── 1. Critical State: EMERGENCY SHUTDOWN ──
            if temp >= CRITICAL_TEMP:
                log.critical("thermal_critical_shutdown", temp=temp)
                if channel:
                    embed = discord.Embed(
                        title="🔥 CRITICAL SYSTEM FAILURE IMMINENT",
                        description=f"@everyone 🚨 **CPU at {temp:.1f}°C!** Initiating emergency shutdown to protect hardware.",
                        color=discord.Color.dark_red()
                    )
                    await channel.send(embed=embed)
                
                # Close discord bot connection cleanly
                await self.bot.close()
                # Kill Python Process to halt all background threads rapidly
                sys.exit(1)

            # ── 2. Warning State: THERMAL THROTTLING ──
            elif temp > WARNING_TEMP and not self.is_throttled:
                self.is_throttled = True
                log.warning("thermal_throttling_activated", temp=temp)
                
                # Pause non-essential background scrapers
                deals_crawler = self.bot.state.get("deals_crawler")
                if deals_crawler and hasattr(deals_crawler, "_loop"):
                    # Assuming deals_crawler has a standard loop or task we can pause.
                    # As a safe generic approach for crawlers running tasks.loop:
                    if deals_crawler._loop.is_running():
                        deals_crawler._loop.cancel()
                
                if channel:
                    embed = discord.Embed(
                        title="⚠️ Thermal Throttling Engaged",
                        description=f"CPU Temperature critically high (**{temp:.1f}°C**). Non-essential modules (Hardware scraper) have been suspended to reduce load.",
                        color=discord.Color.orange()
                    )
                    await channel.send(embed=embed)

            # ── 3. Recovery State: UNTHROTTLE ──
            elif temp < (WARNING_TEMP - 5.0) and self.is_throttled:
                # Add a 5-degree hysteresis buffer so it doesn't flap on/off around exactly 80C
                self.is_throttled = False
                log.info("thermal_throttling_deactivated", temp=temp)
                
                # Resume non-essential scrapers
                deals_crawler = self.bot.state.get("deals_crawler")
                if deals_crawler and hasattr(deals_crawler, "_loop"):
                    if not deals_crawler._loop.is_running():
                        deals_crawler._loop.start()

                if channel:
                    embed = discord.Embed(
                        title="✅ Thermal Recovery",
                        description=f"CPU Temperature stabilized (**{temp:.1f}°C**). All modules resuming normal operation.",
                        color=discord.Color.green()
                    )
                    await channel.send(embed=embed)

        except Exception as e:
            log.error("thermal_watchdog_error", error=str(e))

    @thermal_watchdog.before_loop
    async def before_watchdog(self):
        await self.bot.wait_until_ready()

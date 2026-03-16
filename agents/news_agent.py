"""
agents/news_agent.py — Sentinel de Noticias para Acciones y Criptos
==================================================================
Consulta noticias vía RSS (Google News) y utiliza una IA o lógica 
de sentimientos para alertar en Discord en un canal exclusivo.
"""

import os
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
import structlog
from typing import Optional, List, Dict
from datetime import datetime

log = structlog.get_logger(__name__)

class NewsAgent:
    def __init__(self, discord_agent: any) -> None:
        self._discord_agent = discord_agent
        self._seen_urls = set()
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    async def fetch_news(self, query: str) -> List[Dict]:
        """Fetch RSS from Google News for a query."""
        url = f"https://news.google.com/rss/search?q={query}&hl=es-ES&gl=ES&ceid=ES:es"
        news_items = []

        try:
            async with aiohttp.ClientSession(headers=self._headers) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        root = ET.fromstring(text)
                        
                        for item in root.findall(".//item")[:10]: # Top 10
                            title = item.find("title").text if item.find("title") is not None else ""
                            link = item.find("link").text if item.find("link") is not None else ""
                            pub_date = item.find("pubDate").text if item.find("pubDate") is not None else ""

                            if link not in self._seen_urls:
                                news_items.append({
                                    "title": title,
                                    "link": link,
                                    "pub_date": pub_date,
                                    "asset": query
                                })
                                self._seen_urls.add(link)
        except Exception as exc:
            log.warning("fetch_news_failed", query=query, error=str(exc))

        return news_items

    async def analyze_sentiment(self, title: str) -> str:
        """
        Hyper-fast sentiment helper.
        In the future, targets LLM analysis, currently uses keyword scoring rules.
        """
        positive = ["sube", "alcista", "crece", "ganancias", "rally", "moon", "aprueba", "comprar", "bullish", "superior", "récord"]
        negative = ["cae", "bajista", "pierde", "desplome", "crisis", "negativo", "hack", "dump", "investigación", "multa"]

        score = 0
        text_lower = title.lower()
        for w in positive:
            if w in text_lower: score += 1
        for w in negative:
            if w in text_lower: score -= 1

        if score > 0: return "BULLISH 🚀"
        if score < 0: return "BEARISH 📉"
        return "NEUTRAL ⚖️"

    async def process_markets(self) -> None:
        """Fetch and analyze news for preset assets."""
        assets = ["Solana", "Bitcoin", "NVIDIA", "MicroStrategy", "Apple"]
        log.info("news_agent_cycle_starting", assets=assets)

        for asset in assets:
            items = await self.fetch_news(asset)
            for item in items:
                sentiment = await self.analyze_sentiment(item["title"])
                
                # Only alert on BULLISH news that forces Prices Up, as per user requirement!
                if sentiment == "BULLISH 🚀":
                     log.info("bullish_news_found", asset=asset, title=item["title"])
                     
                     # Update alert on Discord agent
                     # Since it has its own target channel, we send update text
                     message = (
                          f"📰 **Noticia Alcista Detectada** | `{asset}`\n"
                          f"📌 **Titular**: {item['title']}\n"
                          f"🤖 **Sentimiento**: {sentiment}\n"
                          f"🔗 [Leer Más]({item['link']})"
                     )
                     # Send update to discord
                     # Uses send_update which ships to PRIVATE_DEFI_CHANNEL 
                     # For separation, we can add channel support or direct trigger
                     if hasattr(self._discord_agent, "send_update"):
                          news_channel = int(os.getenv("DISCORD_NEWS_CHANNEL_ID", "0"))
                          if news_channel > 0:
                               await self._discord_agent.send_update(message, channel_id=news_channel)
                          else:
                               await self._discord_agent.send_update(message)
            
            # rate limit
            await asyncio.sleep(2)

    async def run_forever(self) -> None:
        """Continuous polling loop for news updates."""
        log.info("news_agent_loop_started")
        while True:
            try:
                await self.process_markets()
            except Exception as e:
                log.error("news_agent_iteration_failed", error=str(e))
                
            interval = float(os.getenv("NEWS_SCAN_INTERVAL_SECONDS", "3600"))
            await asyncio.sleep(interval)

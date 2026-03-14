"""
crawlers/motor.py — Async Hardware Deals Scraper Engine
========================================================
Scrapes multiple deal aggregators concurrently:
  • Slickdeals   (RSS + HTML)
  • Reddit r/buildapcsales  (JSON API)
  • TechBargains (HTML / BeautifulSoup)

Runs on a configurable interval (DEALS_SCAN_INTERVAL_SECONDS).
Fires the discord_post_callback when new relevant deals are found.
Deduplicates via the database (URL-based).
"""

from __future__ import annotations

import asyncio
import os
from typing import Callable, Coroutine, Optional

import aiohttp
import structlog
from bs4 import BeautifulSoup
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from crawlers.deals import (
    Deal,
    DealFilter,
    parse_slickdeals_item,
    parse_reddit_item,
    parse_techbargains_item,
)
from ui.database import Database

from fake_useragent import UserAgent

log = structlog.get_logger(__name__)

ua = UserAgent()

def get_headers():
    return {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }


# ── Source URLs ───────────────────────────────────────────────────────────────
SLICKDEALS_RSS = "https://slickdeals.net/newsearch.php?src=SearchBarV2&q=gpu&searcharea=deals&searchin=first_word&rss=1"
REDDIT_HOT    = "https://www.reddit.com/r/buildapcsales/hot.json?limit=25"
TECHBARGAINS  = "https://www.techbargains.com/deals/computers"


class DealsCrawler:
    """
    Background service that polls multiple deal sources and emits new deals
    via a Discord callback.
    """

    def __init__(self, db: Database) -> None:
        self._db = db
        self._filter = DealFilter()
        self._interval = int(os.getenv("DEALS_SCAN_INTERVAL_SECONDS", "600"))
        self._session: Optional[aiohttp.ClientSession] = None
        # Set by deals_cog after bot is ready
        self.discord_post_callback: Optional[Callable[[Deal], Coroutine]] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run_forever(self) -> None:
        """Main loop — scrapes all sources then sleeps."""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=45),
        )
        log.info("deals_crawler_started", interval_s=self._interval)
        try:
            while True:
                await self._crawl_all_sources()
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            log.info("deals_crawler_stopping")
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    # ── Crawl all sources ─────────────────────────────────────────────────────

    async def _crawl_all_sources(self) -> None:
        log.info("deals_crawl_starting")
        tasks = [
            self._crawl_slickdeals(),
            self._crawl_reddit(),
            self._crawl_techbargains(),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                log.error("crawler_source_failed", source_index=i, error=str(result))

    # ── Slickdeals ────────────────────────────────────────────────────────────

    async def _crawl_slickdeals(self) -> None:
        try:
            xml = await self._fetch_text(SLICKDEALS_RSS)
            if not xml:
                return

            soup = BeautifulSoup(xml, "lxml-xml")
            items = soup.find_all("item")
            deals: list[Deal] = []

            for item in items:
                title = (item.find("title") or {}).get_text(strip=True)
                link  = (item.find("link")  or {}).get_text(strip=True)
                desc  = (item.find("description") or {}).get_text(strip=True)
                # Try to parse price from description
                import re
                price_m = re.search(r"\$\s*([\d,]+(?:\.\d{1,2})?)", desc or "")
                raw: dict = {
                    "title": title,
                    "url": link,
                    "price": price_m.group(1).replace(",", "") if price_m else "",
                }
                deal = parse_slickdeals_item(raw)
                if deal:
                    deals.append(deal)

            await self._process_deals(deals, source="Slickdeals")

        except Exception as exc:
            log.error("slickdeals_crawl_error", error=str(exc))

    # ── Reddit ────────────────────────────────────────────────────────────────

    async def _crawl_reddit(self) -> None:
        try:
            data = await self._fetch_json(REDDIT_HOT)
            if not data:
                return

            children = data.get("data", {}).get("children", [])
            deals = []
            for child in children:
                deal = parse_reddit_item(child)
                if deal:
                    deals.append(deal)

            await self._process_deals(deals, source="Reddit")

        except Exception as exc:
            log.error("reddit_crawl_error", error=str(exc))

    # ── TechBargains ──────────────────────────────────────────────────────────

    async def _crawl_techbargains(self) -> None:
        try:
            html = await self._fetch_text(TECHBARGAINS)
            if not html:
                return

            soup = BeautifulSoup(html, "lxml")
            # TechBargains deal items — selector may need update if site changes
            containers = soup.select(".dealItem, .deal-item, article.deal")
            deals = []
            for div in containers:
                deal = parse_techbargains_item(div)
                if deal:
                    deals.append(deal)

            await self._process_deals(deals, source="TechBargains")

        except Exception as exc:
            log.error("techbargains_crawl_error", error=str(exc))

    # ── Deal processing pipeline ──────────────────────────────────────────────

    async def _process_deals(self, deals: list[Deal], source: str) -> None:
        """Filter, deduplicate, persist, and post new deals."""
        relevant = self._filter.filter_many(deals)
        log.info("deals_filtered", source=source, total=len(deals), relevant=len(relevant))

        for deal in relevant:
            try:
                if await self._db.deal_already_seen(deal.url):
                    log.debug("deal_already_seen", url=deal.url[:80])
                    continue

                # Persist first
                await self._db.insert_deal(
                    title=deal.title,
                    price=deal.price,
                    original_price=deal.original_price,
                    discount_pct=deal.discount_pct,
                    url=deal.url,
                    source=deal.source,
                )

                # Post to Discord
                if self.discord_post_callback:
                    try:
                        await self.discord_post_callback(deal)
                    except Exception as cb_exc:
                        log.error("discord_deal_post_failed", error=str(cb_exc))

            except Exception as exc:
                log.error("deal_processing_error", error=str(exc))

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    async def _fetch_text(self, url: str) -> Optional[str]:
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=2, max=20),
                retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
                reraise=True,
            ):
                with attempt:
                    async with self._session.get(url, headers=get_headers()) as resp:
                        resp.raise_for_status()
                        return await resp.text()
        except Exception as exc:
            log.error("fetch_text_failed", url=url[:80], error=str(exc))
            return None

    async def _fetch_json(self, url: str) -> Optional[dict]:
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=2, max=20),
                retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
                reraise=True,
            ):
                with attempt:
                    async with self._session.get(url, headers=get_headers()) as resp:
                        resp.raise_for_status()
                        return await resp.json()
        except Exception as exc:
            log.error("fetch_json_failed", url=url[:80], error=str(exc))
            return None

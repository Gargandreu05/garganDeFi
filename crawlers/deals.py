"""
crawlers/deals.py — Deal Parsing & Filtering Logic
====================================================
Responsible for:
  • Normalising raw HTML/JSON from scraped sources into Deal dataclass
  • Keyword matching and minimum-discount filtering
  • Deduplication via the database
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Optional

import structlog

log = structlog.get_logger(__name__)

# ── Config from .env ──────────────────────────────────────────────────────────
def _load_keywords() -> list[str]:
    raw = os.getenv(
        "DEALS_KEYWORDS",
        "GPU,RTX,RX 7900,RX 6800,Arc B580,mini-PC,NUC,Ryzen,Core Ultra",
    )
    return [kw.strip().lower() for kw in raw.split(",") if kw.strip()]


def _load_min_discount() -> float:
    return float(os.getenv("DEALS_MIN_DISCOUNT_PCT", "20"))


# ── Data model ────────────────────────────────────────────────────────────────
@dataclass
class Deal:
    title: str
    price: Optional[float]
    original_price: Optional[float]
    url: str
    source: str
    discount_pct: Optional[float] = field(init=False, default=None)

    def __post_init__(self) -> None:
        if self.price and self.original_price and self.original_price > 0:
            self.discount_pct = (
                (self.original_price - self.price) / self.original_price
            ) * 100.0

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "price": self.price,
            "original_price": self.original_price,
            "discount_pct": self.discount_pct,
            "url": self.url,
            "source": self.source,
        }

    def discord_embed_fields(self) -> list[dict]:
        fields = [{"name": "Source", "value": self.source, "inline": True}]
        if self.price:
            fields.append({"name": "Price", "value": f"${self.price:.2f}", "inline": True})
        if self.discount_pct:
            fields.append(
                {"name": "Discount", "value": f"{self.discount_pct:.1f}%", "inline": True}
            )
        return fields


# ── Filtering logic ───────────────────────────────────────────────────────────
class DealFilter:
    """Stateless filter that checks deals against keywords and discount threshold."""

    def __init__(self) -> None:
        self._keywords = _load_keywords()
        self._min_discount = _load_min_discount()

    def reload(self) -> None:
        """Re-read .env settings so changes take effect without restart."""
        self._keywords = _load_keywords()
        self._min_discount = _load_min_discount()

    def is_relevant(self, deal: Deal) -> bool:
        """Return True if the deal matches at least one keyword AND meets discount threshold."""
        title_lower = deal.title.lower()

        keyword_match = any(kw in title_lower for kw in self._keywords)
        if not keyword_match:
            log.debug("deal_filtered_keyword", title=deal.title[:60])
            return False

        if deal.discount_pct is not None and deal.discount_pct < self._min_discount:
            log.debug(
                "deal_filtered_discount",
                title=deal.title[:60],
                discount=deal.discount_pct,
                min_required=self._min_discount,
            )
            return False

        return True

    def filter_many(self, deals: list[Deal]) -> list[Deal]:
        return [d for d in deals if self.is_relevant(d)]


# ── Parsers ───────────────────────────────────────────────────────────────────

def _parse_price(text: str) -> Optional[float]:
    """Extract the first number with optional decimal from a price string."""
    if not text:
        return None
    match = re.search(r"[\$€£]?\s*([\d,]+(?:\.\d{1,2})?)", text.replace(",", ""))
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def parse_slickdeals_item(item: dict) -> Optional[Deal]:
    """
    Convert a Slickdeals API or scraped dict into a Deal.
    Expected keys: title, price, frontpagePrice, url
    """
    try:
        title = item.get("title", "").strip()
        if not title:
            return None
        price = _parse_price(str(item.get("price", "")))
        original = _parse_price(str(item.get("frontpagePrice", item.get("originalPrice", ""))))
        url = item.get("url") or item.get("dealUrl", "")
        return Deal(title=title, price=price, original_price=original, url=url, source="Slickdeals")
    except Exception as exc:
        log.warning("parse_slickdeals_failed", error=str(exc))
        return None


def parse_reddit_item(item: dict) -> Optional[Deal]:
    """
    Convert a Reddit r/buildapcsales post dict into a Deal.
    Expects Reddit API post structure (data.children[*].data).
    """
    try:
        data = item.get("data", item)
        title = data.get("title", "").strip()
        url = data.get("url") or f"https://reddit.com{data.get('permalink', '')}"
        # Try to extract price from title: [GPU] ASUS RTX 3080 - $499
        price_match = re.search(r"\$\s*([\d,]+(?:\.\d{1,2})?)", title)
        price = float(price_match.group(1).replace(",", "")) if price_match else None
        return Deal(title=title, price=price, original_price=None, url=url, source="Reddit")
    except Exception as exc:
        log.warning("parse_reddit_failed", error=str(exc))
        return None


def parse_techbargains_item(soup_div) -> Optional[Deal]:
    """Parse a BeautifulSoup div from TechBargains."""
    try:
        from bs4 import Tag
        if not isinstance(soup_div, Tag):
            return None
        title_el = soup_div.select_one(".deal-title a, .title a")
        price_el = soup_div.select_one(".price, .deal-price")
        original_el = soup_div.select_one(".oldprice, .was-price")
        link_el = soup_div.select_one("a[href]")

        title = title_el.get_text(strip=True) if title_el else ""
        price = _parse_price(price_el.get_text(strip=True)) if price_el else None
        original = _parse_price(original_el.get_text(strip=True)) if original_el else None
        url = link_el["href"] if link_el else ""

        if not title:
            return None
        return Deal(title=title, price=price, original_price=original, url=url, source="TechBargains")
    except Exception as exc:
        log.warning("parse_techbargains_failed", error=str(exc))
        return None

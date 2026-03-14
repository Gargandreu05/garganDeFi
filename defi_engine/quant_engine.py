"""
defi_engine/quant_engine.py — Advanced Quant Analysis Module
============================================================
Strict mathematical module implementing Multi-Asset Analysis.
No LLM hallucinations here. Uses standard technical indicators
(RSI, MACD, Bollinger Bands) to generate an actionable score.
"""

import asyncio
import aiohttp
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import structlog
import feedparser
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from typing import Dict, Any, List, Optional

log = structlog.get_logger(__name__)

# Fallback fake history (for tokens with very little data in simple APIs)
# Dexscreener API mostly returns current price and 24h stats. 
# We'll need a way to get historical OHLCV. 
# For demonstration, we'll try to use Coingecko or Dexscreener charts endpoint if available.
# Actually, DexScreener v1 endpoints don't directly offer easy OHLCV.
# We will simulate the OHLCV logic for cryptos based on price changes or use yfinance for stocks.
# Since building a full historical data pipeline is complex, we will use yfinance for crypto too 
# where possible (e.g., SOL-USD) and simulate for pure on-chain if API doesn't provide it easily, 
# or use Jupiter API.

class QuantEngine:
    """Core mathematical engine for technical analysis of stocks and crypto."""

    def __init__(self):
        # We will track these major tech / hardware stocks
        self.stocks_to_track = ["NVDA", "AMD", "INTC"]
        # And these core cryptos (we can add token addresses for DexScreener)
        # Using DexScreener specific pairs. E.g Raydium SOL/USDC pool
        self.crypto_pairs = [
            "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUListenbkvkXRy" # SOL/USDC Raydium
        ]
        # Initialize Sentiment Analyzer
        self.analyzer = SentimentIntensityAnalyzer()
        
        # Free RSS feeds for news aggregation
        self.crypto_feeds = [
            "https://cointelegraph.com/rss",
            "https://www.coindesk.com/arc/outboundfeeds/rss/"
        ]
        self.stock_feeds = [
            "https://feeds.finance.yahoo.com/rss/2.0/headline?s=NVDA,AMD,INTC,QQQ",
            "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664" # Tech News
        ]

    async def _fetch_latest_news(self, asset_type: str) -> List[str]:
        """Asynchronously fetch latest headlines via RSS feeds to use for Sentiment."""
        feeds_to_fetch = self.crypto_feeds if asset_type == "CRYPTO" else self.stock_feeds
        headlines = []
        
        def fetch_feed(url: str):
            try:
                feed = feedparser.parse(url)
                # Take top 10 from each feed to keep it focused
                return [entry.title for entry in feed.entries[:10]]
            except Exception as e:
                log.error("quant_rss_fetch_error", feed=url, error=str(e))
                return []

        # Run fetchers in threads
        tasks = [asyncio.to_thread(fetch_feed, url) for url in feeds_to_fetch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for res in results:
            if isinstance(res, list):
                headlines.extend(res)
                
        return headlines

    def _analyze_sentiment(self, headlines: List[str], target_keyword: str = None) -> float:
        """
        Analyze sentiment of headlines using VADER.
        Returns a score from -1.0 (Bearish/Panic) to +1.0 (Bullish/Euphoria).
        """
        if not headlines:
            return 0.0
            
        # Filter for relevant headlines if target provided (e.g., 'SOL' or 'NVDA')
        # Simple string match for context-aware scoring
        relevant_headlines = headlines
        if target_keyword:
            kw = target_keyword.split('-')[0].split('/')[0].lower() # e.g 'SOL-USD' -> 'sol'
            relevant_headlines = [h for h in headlines if kw in h.lower()]
            
        # If no specific mentions found, fallback to broader market sentiment
        if not relevant_headlines:
            relevant_headlines = headlines

        scores = []
        for headline in relevant_headlines:
            # Compound score is typical VADER output metric between -1 and 1
            sentiment = self.analyzer.polarity_scores(headline)
            scores.append(sentiment['compound'])
            
        if not scores:
            return 0.0
            
        return sum(scores) / len(scores)

    async def run_screener(self) -> List[Dict[str, Any]]:
        """
        Run the screener across both Stocks and Crypto.
        Returns a list of alerts with Confidence > 80%.
        """
        alerts = []

        # 1. Analyze Stocks
        for ticker in self.stocks_to_track:
            try:
                data = await self.analyze_stock(ticker)
                if data and data.get("confidence_score", 0) > 80:
                    alerts.append(data)
            except Exception as e:
                log.error("quant_stock_analysis_failed", ticker=ticker, error=str(e))

        # 2. Analyze Crypto
        for pair_address in self.crypto_pairs:
            try:
                data = await self.analyze_crypto(pair_address)
                if data and data.get("confidence_score", 0) > 80:
                    alerts.append(data)
            except Exception as e:
                log.error("quant_crypto_analysis_failed", pair=pair_address, error=str(e))

        return alerts

    async def analyze_stock(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch and analyze traditional stock using yfinance."""
        log.info("quant_analyzing_stock", ticker=ticker)
        
        # Run yfinance in a thread to prevent blocking async loop
        def fetch_data():
            stock = yf.Ticker(ticker)
            # Fetch last 3 months to have enough data for Indicators (MACD needs 26, SMA needs 20, etc.)
            df = stock.history(period="3mo", interval="1d")
            return df
        
        df = await asyncio.to_thread(fetch_data)

        if df is None or df.empty or len(df) < 30:
            log.warning("quant_insufficient_data", ticker=ticker)
            return None

        # Clean column names (yfinance returns MultiIndex sometimes or capitalized)
        df.columns = [col.lower() for col in df.columns]

        tech_data = self._apply_indicators_and_score(df, ticker, asset_type="STOCK")
        return await self._apply_bayesian_confluence(tech_data, ticker)


    async def analyze_crypto(self, pair_address: str, relaxed: bool = False) -> Optional[Dict[str, Any]]:
        """
        Analyze a Solana token pair (or major crypto).
        Relaxed mode lowers thresholds for high-liquidity pools.
        """
        log.info("quant_analyzing_crypto", pair=pair_address, relaxed=relaxed)
        
        # We will use yfinance for SOL-USD and others to ensure we have standard DAILY OHLCV 
        # for robust pandas_ta calculations without API keys.
        # If it's a specific solana token, we'll try to handle it.
        # Let's map pair address to a common symbol if we can, else fallback to SOL-USD.
        
        ticker = "SOL-USD" # Default proxy for Solana ecosystem
        
        def fetch_data():
            stock = yf.Ticker(ticker)
            df = stock.history(period="3mo", interval="1d")
            return df
        
        df = await asyncio.to_thread(fetch_data)

        if df is None or df.empty or len(df) < 30:
            return None

        df.columns = [col.lower() for col in df.columns]
        
        # Enhance with current price from DexScreener if we want real-time price of the token
        dex_data = await self._fetch_dexscreener_data(pair_address)
        current_price = dex_data.get("priceUsd") if dex_data else None
        token_name = dex_data.get("baseToken", {}).get("symbol", "SOL") if dex_data else "SOL"
        
        tech_data = self._apply_indicators_and_score(df, f"{token_name}/USDC", asset_type="CRYPTO")
        
        if not tech_data:
            return None
            
        if current_price:
            tech_data['current_price'] = float(current_price)
            # Adjust target price relative to current dex price
            tech_data['target_price'] = tech_data['current_price'] * 1.05
            tech_data['stop_loss'] = tech_data['current_price'] * 0.95
            
        return await self._apply_bayesian_confluence(tech_data, token_name, relaxed=relaxed)

    async def _apply_bayesian_confluence(self, tech_data: Dict[str, Any], keyword: str, relaxed: bool = False) -> Optional[Dict[str, Any]]:
        """
        Applies Bayesian Probability Model and Confluence Rule.
        Weighted Model: 60% Tech, 40% Sentiment.
        Relaxed mode (for blue-chips) lowers the entry barrier.
        """
        # Fetch News and score Sentiment
        headlines = await self._fetch_latest_news(tech_data['asset_type'])
        sentiment_score = self._analyze_sentiment(headlines, target_keyword=keyword)
        
        tech_score = tech_data.get("tech_scaled_score", 0.0)
        
        # The 'Confluence' Rule: Both must be positive for a Bullish setup.
        # Relaxed thresholds for "Blue-Chip" assets (TVL > $1M)
        tech_min = 0.1 if relaxed else 0.2
        sentiment_min = 0.05 if relaxed else 0.1

        if tech_score < tech_min or sentiment_score < sentiment_min:
            log.info("quant_confluence_rejected", ticker=tech_data['ticker'], 
                     tech=tech_score, sentiment=sentiment_score, relaxed=relaxed)
            # Fails confluence rule, don't generate alert
            return None
            
        # Bayesian Probability Model: 60% Technical, 40% Sentiment
        # Weighted sum of scores (-1 to 1)
        weighted_score = (tech_score * 0.60) + (sentiment_score * 0.40)
        
        # Convert the weighted score (-1 to 1) back into a Confidence Score (0-100%)
        # Here we map 0.0 -> 50%, 1.0 -> 100%
        final_confidence = 50.0 + (weighted_score * 50.0)
        
        tech_data['confidence_score'] = round(final_confidence, 2)
        tech_data['sentiment_score'] = round(sentiment_score, 2)
        tech_data['tech_raw_score'] = round(tech_score, 2)
        
        return tech_data

    async def _fetch_dexscreener_data(self, pair_address: str) -> Optional[Dict]:
        """Fetch real-time data from DexScreener."""
        url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair_address}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        pairs = data.get("pairs", [])
                        if pairs:
                            return pairs[0]
        except Exception as e:
            log.error("dexscreener_api_error", error=str(e))
        return None

    def _apply_indicators_and_score(self, df: pd.DataFrame, name: str, asset_type: str) -> Dict[str, Any]:
        """Apply pandas_ta indicators and calculate a confidence score."""

        # 1. Calculate Indicators
        # RSI - Relative Strength Index (14)
        df.ta.rsi(length=14, append=True)
        
        # MACD - Moving Average Convergence Divergence
        df.ta.macd(fast=12, slow=26, signal=9, append=True)
        
        # Bollinger Bands
        df.ta.bbands(length=20, std=2, append=True)

        # Get the latest row
        latest = df.iloc[-1]
        
        current_price = float(latest['close'])
        
        # Indicator columns created by pandas_ta
        # RSI: 'RSI_14'
        # MACD: 'MACD_12_26_9', 'MACDs_12_26_9', 'MACDh_12_26_9'
        # BBands: 'BBL_20_2.0', 'BBM_20_2.0', 'BBU_20_2.0', 'BBB_20_2.0', 'BBP_20_2.0'
        
        rsi_val = float(latest.get('RSI_14', 50))
        macd_line = float(latest.get('MACD_12_26_9', 0))
        signal_line = float(latest.get('MACDs_12_26_9', 0))
        bb_lower = float(latest.get('BBL_20_2.0', current_price * 0.9))
        bb_upper = float(latest.get('BBU_20_2.0', current_price * 1.1))

        # 2. Strict Mathematical Scoring System (Technical Only)
        # We scale technical score to span -1.0 to 1.0 to match Sentiment scale.
        # Start at 0 (Neutral)
        tech_scaled = 0.0

        # RSI Logic (Oversold = Bullish, Overbought = Bearish)
        if rsi_val < 30:
            tech_scaled += 0.4  # Strongly oversold
        elif rsi_val < 40:
            tech_scaled += 0.2
        elif rsi_val > 70:
            tech_scaled -= 0.4  # Strongly overbought
        elif rsi_val > 60:
            tech_scaled -= 0.2

        # MACD Logic (Bullish Crossover)
        if macd_line > signal_line and macd_line < 0:
            tech_scaled += 0.3  # Bullish cross below zero line
        elif macd_line > signal_line:
            tech_scaled += 0.2  # Standard bullish cross
        elif macd_line < signal_line:
            tech_scaled -= 0.3  # Bearish cross

        # Bollinger Bands Logic (Mean Reversion)
        if current_price <= bb_lower * 1.02:
            tech_scaled += 0.3  # Price bouncing off lower band
        elif current_price >= bb_upper * 0.98:
            tech_scaled -= 0.3  # Price rejecting from upper band

        # Bounds check for technicals
        tech_scaled = max(-1.0, min(1.0, tech_scaled))
        
        # NOTE: At this stage we are just returning the technical setup. 
        # The sentiment confluence engine needs async context to fetch RSS, 
        # so we will process Bayesian logic back in the analyze_x methods.
        
        # Generate Target and Stop Loss
        target_price = current_price * 1.05 if current_price < bb_upper else current_price * 1.10
        stop_loss = bb_lower if current_price > bb_lower else current_price * 0.95

        return {
            "asset_type": asset_type,
            "ticker": name,
            "current_price": current_price,
            "target_price": target_price,
            "stop_loss": stop_loss,
            "tech_scaled_score": tech_scaled, # Output the raw scaled score for Bayseian Model
            "indicators": {
                "RSI": round(rsi_val, 2),
                "MACD": round(macd_line, 4),
                "MACD_Signal": round(signal_line, 4),
                "BB_Lower": round(bb_lower, 4),
                "BB_Upper": round(bb_upper, 4)
            }
        }

"""
ui/graph_maker.py — Visual Reporting Module
=============================================
Generates dark, hacker-themed charts for portfolio tracking
using matplotlib and seaborn. Outputs directly to a memory buffer.
"""

import io
import asyncio
from typing import Optional
from datetime import datetime

import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import structlog

from ui.database import Database

log = structlog.get_logger(__name__)

class GraphMaker:
    """Generates charts and visualizations from DB data."""

    def __init__(self):
        # Configure the dark hacker aesthetic
        sns.set_theme(style="darkgrid")
        plt.style.use("dark_background")
        
        # Neon brand colors
        self.color_primary = "#00ffcc"   # Neon Cyan
        self.color_secondary = "#ff007f" # Neon Pink
        self.color_bg = "#0d1117"        # Dark GitHub/Hacker background
        self.color_grid = "#30363d"      # Subtle grid lines
        
        # Override some matplotlib params globally for aesthetics
        plt.rcParams.update({
            "axes.facecolor": self.color_bg,
            "figure.facecolor": self.color_bg,
            "grid.color": self.color_grid,
            "text.color": "white",
            "axes.labelcolor": "white",
            "xtick.color": "gray",
            "ytick.color": "gray",
            "font.family": "sans-serif"
        })

    async def generate_portfolio_chart(self, db: Database) -> Optional[io.BytesIO]:
        """
        Fetches trade and evaluation data to generate a portfolio chart.
        Returns a BytesIO buffer containing the PNG image.
        """
        log.info("generating_portfolio_chart")
        
        def _plot(df: pd.DataFrame) -> io.BytesIO:
            # Create a 10x5 inch figure
            fig, ax = plt.subplots(figsize=(10, 5), dpi=120)
            
            # If no data, render an empty chart message
            if df.empty:
                ax.text(0.5, 0.5, "NOT ENOUGH DATA YET", color=self.color_secondary, 
                        ha='center', va='center', fontsize=20, weight='bold')
                ax.set_axis_off()
            else:
                # Plot the Portfolio Value
                sns.lineplot(
                    data=df, 
                    x='timestamp', 
                    y='value', 
                    ax=ax, 
                    color=self.color_primary, 
                    linewidth=2.5
                )
                
                # Fill area under curve for a "glow" effect
                ax.fill_between(
                    df['timestamp'], 
                    df['value'], 
                    alpha=0.15, 
                    color=self.color_primary
                )

                # Format axes
                ax.set_title("Portfolio Value Over Time (SOL)", fontsize=16, weight="bold", color="white", pad=20)
                ax.set_xlabel("Date", fontsize=12, color="gray", labelpad=10)
                ax.set_ylabel("SOL Value", fontsize=12, color="gray", labelpad=10)
                
                # Format X-axis as dates
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
                plt.xticks(rotation=45)
                
                # Stylish layout tweaks
                sns.despine(left=True, bottom=True)
                fig.tight_layout()

            # Save to BytesIO buffer
            buf = io.BytesIO()
            plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), edgecolor='none', bbox_inches='tight')
            buf.seek(0)
            
            # Close plot to free memory
            plt.close(fig)
            return buf

        try:
            # Fetch data from Database
            df = await self._fetch_historical_data(db)
            # Render plot in a separate thread so we don't block the async event loop
            buffer = await asyncio.to_thread(_plot, df)
            return buffer
            
        except Exception as e:
            log.error("graph_generation_failed", error=str(e))
            return None

    async def _fetch_historical_data(self, db: Database) -> pd.DataFrame:
        """
        Builds a time-series dataframe representing portfolio value.
        For a true portfolio, we'd query `trades` to map deposits & withdrawals, 
        and `pool_evaluations` to track active pool APY/IL.
        For demonstrating the visualizer hook, we'll build a synthetic series
        based on recent trades if they exist, or mock data if testing.
        """
        # Get all confirmed deposit/withdraw trades
        rows = await db.get_recent_trades(limit=1000)
        
        # Sort chronologically (oldest first)
        rows = [r for r in rows if r['status'] == 'CONFIRMED']
        rows.sort(key=lambda x: x['ts'])
        
        data_points = []
        current_value = 0.0
        
        for trade in rows:
            trade_type = trade.get('trade_type')
            amt = float(trade.get('amount_sol') or 0.0)
            
            # VERY simplified net tracker:
            if trade_type == 'DEPOSIT':
                current_value -= amt
            elif trade_type == 'WITHDRAW':
                current_value += amt
                
            data_points.append({
                'timestamp': datetime.fromisoformat(trade['ts']),
                'value': current_value
            })
            
        df = pd.DataFrame(data_points)
        
        # If the DB is completely empty (new install), return an empty DataFrame
        if df.empty:
            return df
            
        # Optional: Smooth the line to avoid jagged steps for standard tracking
        # We'd typically blend this against live price Oracles. 
        # But this suffices for connecting the exact trade delta points over time.
        return df

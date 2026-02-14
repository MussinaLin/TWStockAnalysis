from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class AppConfig:
    extra_stocks: list[str]
    macd_fast: int
    macd_slow: int
    macd_signal: int
    bb_period: int
    alpha_rsi_min: float
    alpha_rsi_max: float
    alpha_macd_hist_min: float
    alpha_insti_days_short: int
    alpha_insti_days_long: int
    bb_narrow_short_days: int
    bb_narrow_long_days: int
    bb_percent_b_min: float
    vol_breakout_ratio: float
    # Sell alert settings
    sell_insti_days_short: int
    sell_insti_days_long: int
    sell_high_black_ratio: float
    sell_price_high_days: int
    sell_volume_shrink_ratio: float
    sell_bb_percent_b_max: float
    sell_rsi_overbought: float
    sell_margin_surge_ratio: float
    sell_other_cond_min: int

    @classmethod
    def from_env(cls) -> "AppConfig":
        raw_stocks = os.getenv("STOCKS", "")
        extra_stocks = [
            item.strip()
            for item in raw_stocks.split(",")
            if item.strip()
        ]
        return cls(
            extra_stocks=extra_stocks,
            macd_fast=int(os.getenv("MACD_FAST", "12")),
            macd_slow=int(os.getenv("MACD_SLOW", "26")),
            macd_signal=int(os.getenv("MACD_SIGNAL", "9")),
            bb_period=int(os.getenv("BB_PERIOD", "20")),
            alpha_rsi_min=float(os.getenv("ALPHA_RSI_MIN", "40")),
            alpha_rsi_max=float(os.getenv("ALPHA_RSI_MAX", "70")),
            alpha_macd_hist_min=float(os.getenv("ALPHA_MACD_HIST_MIN", "0")),
            alpha_insti_days_short=int(os.getenv("ALPHA_INSTI_DAYS_SHORT", "20")),
            alpha_insti_days_long=int(os.getenv("ALPHA_INSTI_DAYS_LONG", "30")),
            bb_narrow_short_days=int(os.getenv("BB_NARROW_SHORT_DAYS", "5")),
            bb_narrow_long_days=int(os.getenv("BB_NARROW_LONG_DAYS", "20")),
            bb_percent_b_min=float(os.getenv("BB_PERCENT_B_MIN", "0.75")),
            vol_breakout_ratio=float(os.getenv("VOL_BREAKOUT_RATIO", "1.5")),
            # Sell alert settings
            sell_insti_days_short=int(os.getenv("SELL_INSTI_DAYS_SHORT", "15")),
            sell_insti_days_long=int(os.getenv("SELL_INSTI_DAYS_LONG", "30")),
            sell_high_black_ratio=float(os.getenv("SELL_HIGH_BLACK_RATIO", "0.05")),
            sell_price_high_days=int(os.getenv("SELL_PRICE_HIGH_DAYS", "10")),
            sell_volume_shrink_ratio=float(os.getenv("SELL_VOLUME_SHRINK_RATIO", "0.7")),
            sell_bb_percent_b_max=float(os.getenv("SELL_BB_PERCENT_B_MAX", "0.5")),
            sell_rsi_overbought=float(os.getenv("SELL_RSI_OVERBOUGHT", "80")),
            sell_margin_surge_ratio=float(os.getenv("SELL_MARGIN_SURGE_RATIO", "0.05")),
            sell_other_cond_min=int(os.getenv("SELL_OTHER_COND_MIN", "2")),
        )

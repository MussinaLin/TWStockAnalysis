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
        )

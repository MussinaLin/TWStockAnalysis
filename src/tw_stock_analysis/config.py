from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class AppConfig:
    tpex_daily_quotes_url_template: str | None
    tpex_3insti_url_template: str | None
    extra_stocks: list[str]
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
            tpex_daily_quotes_url_template=os.getenv("TPEX_DAILY_QUOTES_URL_TEMPLATE"),
            tpex_3insti_url_template=os.getenv("TPEX_3INSTI_URL_TEMPLATE"),
            extra_stocks=extra_stocks,
            alpha_rsi_min=float(os.getenv("ALPHA_RSI_MIN", "40")),
            alpha_rsi_max=float(os.getenv("ALPHA_RSI_MAX", "70")),
            alpha_macd_hist_min=float(os.getenv("ALPHA_MACD_HIST_MIN", "0")),
            alpha_insti_days_short=int(os.getenv("ALPHA_INSTI_DAYS_SHORT", "20")),
            alpha_insti_days_long=int(os.getenv("ALPHA_INSTI_DAYS_LONG", "30")),
        )

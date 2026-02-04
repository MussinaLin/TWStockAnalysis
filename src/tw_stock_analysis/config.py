from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class AppConfig:
    tpex_daily_quotes_url_template: str | None
    tpex_3insti_url_template: str | None
    extra_stocks: list[str]

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
        )

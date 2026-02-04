from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class AppConfig:
    tpex_daily_quotes_url_template: str | None
    tpex_3insti_url_template: str | None

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            tpex_daily_quotes_url_template=os.getenv("TPEX_DAILY_QUOTES_URL_TEMPLATE"),
            tpex_3insti_url_template=os.getenv("TPEX_3INSTI_URL_TEMPLATE"),
        )

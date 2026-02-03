from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd


def load_history(path: Path) -> dict[str, pd.Series]:
    if not path.exists():
        return {}

    history: dict[str, list[tuple[dt.date, float]]] = {}
    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        try:
            sheet_date = dt.date.fromisoformat(sheet)
        except ValueError:
            continue
        df = xls.parse(sheet)
        if "symbol" not in df.columns or "close" not in df.columns:
            continue
        for _, row in df.iterrows():
            symbol = str(row["symbol"]).strip()
            close = row["close"]
            if pd.isna(close):
                continue
            history.setdefault(symbol, []).append((sheet_date, float(close)))

    result: dict[str, pd.Series] = {}
    for symbol, rows in history.items():
        rows = sorted(rows, key=lambda item: item[0])
        dates = [item[0] for item in rows]
        closes = [item[1] for item in rows]
        result[symbol] = pd.Series(closes, index=pd.to_datetime(dates))
    return result


def get_sheet_names(path: Path) -> set[str]:
    if not path.exists():
        return set()
    xls = pd.ExcelFile(path)
    return set(xls.sheet_names)


def write_daily_sheet(path: Path, date: dt.date, df: pd.DataFrame) -> None:
    sheet_name = date.isoformat()
    if path.exists():
        with pd.ExcelWriter(
            path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace",
        ) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

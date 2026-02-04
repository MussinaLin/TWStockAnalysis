from __future__ import annotations

import datetime as dt
import io
import os
import re
import urllib3
from typing import Any

import pandas as pd
import requests

HOLDINGS_URL = "https://www.tsit.com.tw/ETF/Home/ETFSeriesDetail/00987A"
TWSE_STOCK_DAY_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TWSE_T86_URL = "https://www.twse.com.tw/fund/T86"
TPEX_DAILY_QUOTES_URL = (
    "https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/"
    "stk_quote_result.php?l=zh-tw&o=data"
)
TPEX_3INSTI_URL = (
    "https://www.tpex.org.tw/web/stock/3insti/daily_trade/"
    "3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&o=data"
)
TPEX_DAILY_QUOTES_URL_TEMPLATE = os.getenv("TPEX_DAILY_QUOTES_URL_TEMPLATE")
TPEX_3INSTI_URL_TEMPLATE = os.getenv("TPEX_3INSTI_URL_TEMPLATE")


class DataUnavailableError(RuntimeError):
    pass


def _clean_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if text in {"--", "---", "", "None"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _clean_int(value: Any) -> int | None:
    number = _clean_number(value)
    if number is None:
        return None
    return int(round(number))


def _roc_to_date(roc_date: str) -> dt.date | None:
    match = re.match(r"^(\d{2,3})/(\d{1,2})/(\d{1,2})$", roc_date.strip())
    if not match:
        return None
    year = int(match.group(1)) + 1911
    month = int(match.group(2))
    day = int(match.group(3))
    return dt.date(year, month, day)


def _date_to_roc(date: dt.date) -> str:
    return f"{date.year - 1911}/{date.month:02d}/{date.day:02d}"


def _format_template(template: str, date: dt.date) -> str:
    return template.format(date=date.isoformat(), roc=_date_to_roc(date))


def fetch_00987a_holdings(session: requests.Session) -> pd.DataFrame:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(HOLDINGS_URL, timeout=30, verify=False)
    response.raise_for_status()

    try:
        tables = pd.read_html(io.StringIO(response.text))
    except ValueError as exc:
        raise DataUnavailableError("成份股頁面解析失敗，未找到表格。") from exc
    target = None
    for table in tables:
        flat_cols = [
            " ".join(map(str, col)).strip() if isinstance(col, tuple) else str(col)
            for col in table.columns
        ]
        if any("代號" in col for col in flat_cols) and any("名稱" in col for col in flat_cols):
            target = table.copy()
            target.columns = flat_cols
            break
        if any("成分" in col or "股票" in col for col in flat_cols):
            target = table.copy()
            target.columns = flat_cols
            break

    if target is None:
        raise DataUnavailableError("無法找到 00987A 成份股表格。")

    rows = []
    symbol_col = next((c for c in target.columns if "代號" in str(c)), None)
    name_col = next((c for c in target.columns if "名稱" in str(c)), None)
    if symbol_col and name_col:
        for _, row in target.iterrows():
            symbol_raw = str(row.get(symbol_col, "")).strip()
            name = str(row.get(name_col, "")).strip()
            match = re.search(r"(\d{4,6})", symbol_raw)
            symbol = match.group(1) if match else symbol_raw
            if not symbol:
                continue
            name = re.sub(r"\s+", " ", name)
            rows.append({"symbol": symbol, "name": name})
    else:
        first_col = target.columns[0]
        for raw in target[first_col].astype(str).tolist():
            match = re.search(r"(\d{4,6})", raw)
            if not match:
                continue
            symbol = match.group(1)
            name = re.sub(r"\s*\d{4,6}\s*", " ", raw).strip()
            name = re.sub(r"\s+", " ", name)
            rows.append({"symbol": symbol, "name": name})

    if not rows:
        raise DataUnavailableError("成份股資料解析失敗。")

    return pd.DataFrame(rows).drop_duplicates(subset=["symbol"]).reset_index(drop=True)


def fetch_twse_stock_day(
    session: requests.Session,
    stock_no: str,
    date: dt.date,
) -> pd.DataFrame:
    month_start = date.replace(day=1)
    params = {
        "response": "json",
        "date": month_start.strftime("%Y%m%d"),
        "stockNo": stock_no,
    }
    response = session.get(TWSE_STOCK_DAY_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if payload.get("stat") != "OK":
        raise DataUnavailableError(payload.get("stat") or "TWSE STOCK_DAY 回傳異常")

    data = payload.get("data") or []
    fields = payload.get("fields") or []
    if not data or not fields:
        raise DataUnavailableError("TWSE STOCK_DAY 無資料")

    df = pd.DataFrame(data, columns=fields)
    return df


def find_twse_open_close(df: pd.DataFrame, date: dt.date) -> tuple[float | None, float | None]:
    if "日期" not in df.columns:
        return None, None

    df = df.copy()
    df["_gregorian"] = df["日期"].map(_roc_to_date)
    row = df.loc[df["_gregorian"] == date]
    if row.empty:
        return None, None

    open_price = _clean_number(row.iloc[0].get("開盤價"))
    close_price = _clean_number(row.iloc[0].get("收盤價"))
    return open_price, close_price


def fetch_twse_t86(session: requests.Session, date: dt.date) -> pd.DataFrame:
    params = {
        "response": "json",
        "date": date.strftime("%Y%m%d"),
        "selectType": "ALL",
    }
    response = session.get(TWSE_T86_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if payload.get("stat") != "OK":
        raise DataUnavailableError(payload.get("stat") or "TWSE T86 回傳異常")

    fields = payload.get("fields") or []
    data = payload.get("data") or []
    if not data or not fields:
        raise DataUnavailableError("TWSE T86 無資料")

    df = pd.DataFrame(data, columns=fields)
    return df


def fetch_tpex_daily_quotes(session: requests.Session, date: dt.date | None = None) -> pd.DataFrame:
    if date is not None:
        if not TPEX_DAILY_QUOTES_URL_TEMPLATE:
            raise DataUnavailableError(
                "未設定 TPEX_DAILY_QUOTES_URL_TEMPLATE，無法回補指定日期上櫃行情。"
            )
        url = _format_template(TPEX_DAILY_QUOTES_URL_TEMPLATE, date)
    else:
        url = TPEX_DAILY_QUOTES_URL

    response = session.get(url, timeout=30)
    response.raise_for_status()

    content = response.content
    for encoding in ("utf-8-sig", "cp950"):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            text = ""
    if not text:
        raise DataUnavailableError("TPEX 日行情解碼失敗")

    df = pd.read_csv(io.StringIO(text))
    return df


def fetch_tpex_3insti(session: requests.Session, date: dt.date | None = None) -> pd.DataFrame:
    if date is not None:
        if not TPEX_3INSTI_URL_TEMPLATE:
            raise DataUnavailableError(
                "未設定 TPEX_3INSTI_URL_TEMPLATE，無法回補指定日期上櫃三大法人。"
            )
        url = _format_template(TPEX_3INSTI_URL_TEMPLATE, date)
    else:
        url = TPEX_3INSTI_URL

    response = session.get(url, timeout=30)
    response.raise_for_status()

    content = response.content
    for encoding in ("utf-8-sig", "cp950"):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            text = ""
    if not text:
        raise DataUnavailableError("TPEX 三大法人解碼失敗")

    df = pd.read_csv(io.StringIO(text))
    return df

from __future__ import annotations

import datetime as dt
import io
import re
import urllib3
from typing import Any

import pandas as pd
import requests

HOLDINGS_URL = "https://www.tsit.com.tw/ETF/Home/ETFSeriesDetail/00987A"
TWSE_STOCK_DAY_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TWSE_T86_URL = "https://www.twse.com.tw/fund/T86"
TWSE_STOCK_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TWSE_MI_INDEX_URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
TPEX_DAILY_QUOTES_URL = (
    "https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/"
    "stk_quote_result.php?l=zh-tw&o=data"
)
TPEX_3INSTI_URL = (
    "https://www.tpex.org.tw/web/stock/3insti/daily_trade/"
    "3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&o=data"
)
TPEX_DAILY_QUOTES_V2_URL = (
    "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"
)
TPEX_3INSTI_V2_URL = (
    "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"
)
TWSE_COMPANY_BASIC_URL = "https://dts.twse.com.tw/opendata/t187ap03_L.csv"
TPEX_COMPANY_BASIC_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"


class DataUnavailableError(RuntimeError):
    pass


def _parse_roc_date(value: str) -> dt.date | None:
    match = re.match(r"^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$", value.strip())
    if not match:
        return None
    year = int(match.group(1)) + 1911
    month = int(match.group(2))
    day = int(match.group(3))
    try:
        return dt.date(year, month, day)
    except ValueError:
        return None


def _parse_date_any(value: str) -> dt.date | None:
    text = value.strip()
    if not text:
        return None

    match = re.match(r"^(\d{4})(\d{2})(\d{2})$", text)
    if match:
        try:
            return dt.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None

    match = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$", text)
    if match:
        try:
            return dt.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None

    roc_date = _parse_roc_date(text)
    if roc_date:
        return roc_date

    return None


def _extract_first_date(text: str) -> dt.date | None:
    patterns = [
        r"(?<!\d)(\d{4}[/-]\d{1,2}[/-]\d{1,2})(?!\d)",
        r"(?<!\d)(\d{2,3}[/-]\d{1,2}[/-]\d{1,2})(?!\d)",
        r"(?<!\d)(\d{8})(?!\d)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            date_value = _parse_date_any(match.group(1))
            if date_value:
                return date_value
    return None


def _clean_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
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
    if pd.isna(number):
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


def _extract_twse_table(payload: dict[str, Any]) -> pd.DataFrame:
    tables = payload.get("tables")
    if isinstance(tables, list):
        for table in tables:
            fields = table.get("fields")
            data = table.get("data")
            if not isinstance(fields, list) or not isinstance(data, list):
                continue
            joined = "".join(map(str, fields))
            if ("證券代號" in joined or "代號" in joined) and ("開盤" in joined) and ("收盤" in joined):
                return pd.DataFrame(data, columns=fields)

    for key, fields in payload.items():
        if not key.startswith("fields"):
            continue
        if not isinstance(fields, list):
            continue
        suffix = key.replace("fields", "")
        data_key = f"data{suffix}"
        data = payload.get(data_key)
        if not isinstance(data, list):
            continue
        joined = "".join(map(str, fields))
        if ("證券代號" in joined or "代號" in joined) and ("開盤" in joined) and ("收盤" in joined):
            return pd.DataFrame(data, columns=fields)

    raise DataUnavailableError("TWSE MI_INDEX 無法找到行情表格。")


def _read_tpex_csv(text: str) -> pd.DataFrame:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        raise DataUnavailableError("TPEX 回傳內容為空。")

    joined = "\n".join(lines)
    lower = joined.lower()
    if "<html" in lower or "<!doctype" in lower:
        raise DataUnavailableError("TPEX 回傳非 CSV（可能為網頁內容）。")
    if "查無資料" in joined or "沒有資料" in joined:
        raise DataUnavailableError("TPEX 查無資料。")

    header_idx = None
    for idx, line in enumerate(lines):
        if "," not in line:
            continue
        if line.lstrip().startswith(("註", "說明")):
            continue
        if ("代號" in line and "名稱" in line) or ("證券代號" in line and "收盤" in line):
            header_idx = idx
            break

    if header_idx is None:
        raise DataUnavailableError("TPEX CSV 解析失敗，未找到表頭。")

    csv_text = "\n".join(lines[header_idx:])
    return pd.read_csv(io.StringIO(csv_text))


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
            if not match:
                continue
            symbol = match.group(1)
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
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TWSE_STOCK_DAY_URL, params=params, timeout=30, verify=False)
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


def find_twse_ohlcv(
    df: pd.DataFrame, date: dt.date
) -> tuple[float | None, float | None, float | None, float | None, int | None]:
    if "日期" not in df.columns:
        return None, None, None, None, None

    df = df.copy()
    df["_gregorian"] = df["日期"].map(_roc_to_date)
    row = df.loc[df["_gregorian"] == date]
    if row.empty:
        return None, None, None, None, None

    open_price = _clean_number(row.iloc[0].get("開盤價"))
    high_price = _clean_number(row.iloc[0].get("最高價"))
    low_price = _clean_number(row.iloc[0].get("最低價"))
    close_price = _clean_number(row.iloc[0].get("收盤價"))
    volume = _clean_int(row.iloc[0].get("成交股數")) or _clean_int(row.iloc[0].get("成交量"))
    return open_price, high_price, low_price, close_price, volume


def fetch_twse_t86(session: requests.Session, date: dt.date) -> pd.DataFrame:
    params = {
        "response": "json",
        "date": date.strftime("%Y%m%d"),
        "selectType": "ALL",
    }
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TWSE_T86_URL, params=params, timeout=30, verify=False)
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


def fetch_twse_stock_day_all(session: requests.Session) -> tuple[pd.DataFrame, dt.date | None]:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TWSE_STOCK_DAY_ALL_URL, timeout=30, verify=False)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise DataUnavailableError("TWSE STOCK_DAY_ALL 回傳格式異常")
    if not payload:
        raise DataUnavailableError("TWSE STOCK_DAY_ALL 無資料")
    data_date = None
    sample = payload[0]
    if isinstance(sample, dict):
        for key in ("Date", "date", "日期"):
            if key in sample:
                data_date = _parse_date_any(str(sample.get(key, "")))
                if data_date:
                    break
        if data_date is None:
            for key, value in sample.items():
                if "date" in str(key).lower() or "日期" in str(key):
                    data_date = _parse_date_any(str(value))
                    if data_date:
                        break
    return pd.DataFrame(payload), data_date


def fetch_twse_mi_index(session: requests.Session, date: dt.date) -> tuple[pd.DataFrame, dt.date | None]:
    params = {
        "response": "json",
        "date": date.strftime("%Y%m%d"),
        "type": "ALLBUT0999",
    }
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TWSE_MI_INDEX_URL, params=params, timeout=30, verify=False)
    response.raise_for_status()
    payload = response.json()
    if payload.get("stat") not in {None, "OK"}:
        raise DataUnavailableError(payload.get("stat") or "TWSE MI_INDEX 回傳異常")

    if not isinstance(payload, dict):
        raise DataUnavailableError("TWSE MI_INDEX 回傳格式異常")

    data_date = None
    for key in ("date", "Date", "reportDate", "dataDate", "REPORTDATE", "DATADATE"):
        if key in payload:
            data_date = _parse_date_any(str(payload.get(key, "")))
            if data_date:
                break
    if data_date is None:
        for key, value in payload.items():
            if "date" in str(key).lower():
                data_date = _parse_date_any(str(value))
                if data_date:
                    break

    return _extract_twse_table(payload), data_date


def fetch_tpex_daily_quotes(
    session: requests.Session,
    date: dt.date | None = None,
    template: str | None = None,
) -> tuple[pd.DataFrame, dt.date | None]:
    if date is not None:
        if not template:
            raise DataUnavailableError(
                "未設定 TPEX_DAILY_QUOTES_URL_TEMPLATE，無法回補指定日期上櫃行情。"
            )
        url = _format_template(template, date)
    else:
        url = TPEX_DAILY_QUOTES_URL

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(url, timeout=30, verify=False)
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

    data_date = _extract_first_date(text)
    df = _read_tpex_csv(text)
    return df, data_date


def fetch_tpex_3insti(
    session: requests.Session,
    date: dt.date | None = None,
    template: str | None = None,
) -> tuple[pd.DataFrame, dt.date | None]:
    if date is not None:
        if not template:
            raise DataUnavailableError(
                "未設定 TPEX_3INSTI_URL_TEMPLATE，無法回補指定日期上櫃三大法人。"
            )
        url = _format_template(template, date)
    else:
        url = TPEX_3INSTI_URL

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(url, timeout=30, verify=False)
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

    data_date = _extract_first_date(text)
    df = _read_tpex_csv(text)
    return df, data_date


def _extract_tpex_v2_table(
    payload: dict, title_keyword: str = "上櫃股票"
) -> pd.DataFrame:
    """Extract DataFrame from new TPEX JSON API (tables/fields/data format)."""
    tables = payload.get("tables", [])
    for table in tables:
        if not isinstance(table, dict):
            continue
        title = table.get("title", "")
        fields = table.get("fields", [])
        data = table.get("data", [])
        if title_keyword in title and fields and data:
            return pd.DataFrame(data, columns=fields)
    raise DataUnavailableError(f"TPEX V2 找不到包含「{title_keyword}」的表格。")


def fetch_tpex_daily_quotes_v2(
    session: requests.Session,
    date: dt.date,
) -> tuple[pd.DataFrame, dt.date | None]:
    """Fetch TPEX daily quotes using the new API that supports historical queries."""
    roc = _date_to_roc(date)
    params = {"date": roc, "response": "json"}
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TPEX_DAILY_QUOTES_V2_URL, params=params, timeout=30, verify=False)
    response.raise_for_status()
    payload = response.json()

    if payload.get("stat") not in {None, "ok", "OK"}:
        raise DataUnavailableError(payload.get("stat") or "TPEX V2 行情回傳異常")

    data_date = _parse_date_any(str(payload.get("date", "")))
    df = _extract_tpex_v2_table(payload, "上櫃股票")
    return df, data_date


def fetch_tpex_3insti_v2(
    session: requests.Session,
    date: dt.date,
) -> tuple[pd.DataFrame, dt.date | None]:
    """Fetch TPEX 3-institutional-investors data using the new API."""
    roc = _date_to_roc(date)
    params = {"date": roc, "response": "json", "type": "Daily", "se": "EW"}
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TPEX_3INSTI_V2_URL, params=params, timeout=30, verify=False)
    response.raise_for_status()
    payload = response.json()

    if payload.get("stat") not in {None, "ok", "OK"}:
        raise DataUnavailableError(payload.get("stat") or "TPEX V2 三大法人回傳異常")

    data_date = _parse_date_any(str(payload.get("date", "")))
    df = _extract_tpex_v2_table(payload, "三大法人")

    # The fields have duplicated names (買進股數/賣出股數/買賣超股數 repeated for each
    # institutional category). Rename by position:
    #   [0] 代號, [1] 名稱,
    #   [2-4] 外資及陸資(不含自營商),
    #   [5-7] 外資自營商,
    #   [8-10] 外資及陸資合計,
    #   [11-13] 投信,
    #   [14-16] 自營商(自行買賣),
    #   [17-19] 自營商(避險),
    #   [20-22] 自營商合計,
    #   [23] 三大法人合計
    if len(df.columns) >= 24:
        cols = list(df.columns)
        cols[4] = "外資及陸資買賣超股數"
        cols[10] = "外資合計買賣超股數"
        cols[13] = "投信買賣超股數"
        cols[22] = "自營商合計買賣超股數"
        cols[23] = "三大法人買賣超股數合計"
        df.columns = cols

    return df, data_date


def fetch_twse_company_basic(session: requests.Session) -> pd.DataFrame:
    """Fetch TWSE listed company basic info including issued shares."""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TWSE_COMPANY_BASIC_URL, timeout=30, verify=False)
    response.raise_for_status()

    content = response.content
    for encoding in ("utf-8-sig", "cp950", "big5"):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            text = ""
    if not text:
        raise DataUnavailableError("TWSE 公司基本資料解碼失敗")

    df = pd.read_csv(io.StringIO(text))
    return df


def fetch_tpex_company_basic(session: requests.Session) -> pd.DataFrame:
    """Fetch TPEX OTC company basic info including issued shares."""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TPEX_COMPANY_BASIC_URL, timeout=30, verify=False)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise DataUnavailableError("TPEX 公司基本資料回傳格式異常")
    if not payload:
        raise DataUnavailableError("TPEX 公司基本資料無資料")

    return pd.DataFrame(payload)

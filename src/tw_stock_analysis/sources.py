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
TWSE_MARGIN_URL = "https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN"
TPEX_MARGIN_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance"
MONEYDJ_MARGIN_URL = "https://concords.moneydj.com/z/zc/zcn/zcn.djhtm"
MONEYDJ_HOLDING_URL = "https://concords.moneydj.com/z/zc/zcl/zcl.djhtm"


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


def _parse_roc_date_compact(value: str) -> dt.date | None:
    """Parse ROC date in compact format like '1150211' (YYYMMDD)."""
    text = value.strip()
    if not text or len(text) != 7:
        return None
    try:
        year = int(text[:3]) + 1911
        month = int(text[3:5])
        day = int(text[5:7])
        return dt.date(year, month, day)
    except (ValueError, IndexError):
        return None


def fetch_twse_margin(session: requests.Session) -> tuple[pd.DataFrame, dt.date | None]:
    """Fetch TWSE margin trading data for all listed stocks (today only).

    Returns DataFrame and data date. The DataFrame has Chinese column names.
    Key columns: 股票代號, 融資買進, 融資賣出, 融資現金償還, 融資今日餘額,
                 融券賣出, 融券買進, 融券現券償還, 融券今日餘額

    Note: TWSE OpenAPI does not return date in record, returns None for date.
    Caller should assume data is for today when date is None.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TWSE_MARGIN_URL, timeout=30, verify=False)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise DataUnavailableError("TWSE MI_MARGN 回傳格式異常")
    if not payload:
        raise DataUnavailableError("TWSE MI_MARGN 無資料")

    # TWSE OpenAPI doesn't include date in records, return None
    # Caller should assume it's today's data
    return pd.DataFrame(payload), None


def fetch_tpex_margin(session: requests.Session) -> tuple[pd.DataFrame, dt.date | None]:
    """Fetch TPEX margin trading data for all OTC stocks (today only).

    Returns DataFrame and data date. The DataFrame has English column names.
    Key columns: SecuritiesCompanyCode, MarginPurchase, MarginSales, CashRedemption,
                 MarginPurchaseBalance, ShortSale, ShortCovering, StockRedemption,
                 ShortSaleBalance
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(TPEX_MARGIN_URL, timeout=30, verify=False)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise DataUnavailableError("TPEX margin 回傳格式異常")
    if not payload:
        raise DataUnavailableError("TPEX margin 無資料")

    # Extract date from first record
    # TPEX uses compact ROC format like "1150211" (YYY/MM/DD without separators)
    data_date = None
    sample = payload[0]
    if isinstance(sample, dict):
        for key in ("Date", "date", "日期", "ReportDate"):
            if key in sample:
                date_str = str(sample.get(key, ""))
                # Try compact ROC format first (e.g., "1150211")
                data_date = _parse_roc_date_compact(date_str)
                if data_date:
                    break
                # Fallback to other formats
                data_date = _parse_date_any(date_str)
                if data_date:
                    break

    return pd.DataFrame(payload), data_date


def fetch_moneydj_margin(
    session: requests.Session,
    symbol: str,
    start: dt.date,
    end: dt.date,
) -> pd.DataFrame:
    """Fetch historical margin trading data from MoneyDJ for a single stock.

    Args:
        session: HTTP session
        symbol: Stock symbol (e.g., "2330")
        start: Start date
        end: End date

    Returns:
        DataFrame with columns: date, margin_buy, margin_sell, margin_balance,
                               margin_change, short_sell, short_buy, short_balance,
                               short_change (units: lots/張)
    """
    # MoneyDJ uses YYYY-M-D format (no zero padding)
    start_str = f"{start.year}-{start.month}-{start.day}"
    end_str = f"{end.year}-{end.month}-{end.day}"

    params = {"a": symbol, "c": start_str, "d": end_str}

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(MONEYDJ_MARGIN_URL, params=params, timeout=30, verify=False)
    response.raise_for_status()

    # Parse HTML tables
    try:
        tables = pd.read_html(io.StringIO(response.text), encoding="utf-8")
    except ValueError as exc:
        raise DataUnavailableError(f"MoneyDJ 融資融券頁面解析失敗：{exc}") from exc

    # Find the margin data table - it's typically the largest table with date data
    # MoneyDJ table structure:
    # - Row 5: top-level headers (融資, 融券)
    # - Row 6: sub-headers (日期, 買進, 賣出, 現償, 餘額, 增減, ...)
    # - Row 7+: data rows
    target_table = None
    for table in tables:
        # Check if table has enough rows and columns for margin data
        if len(table) < 8 or len(table.columns) < 12:
            continue
        # Check if row 6 contains "日期" (date header)
        row6 = table.iloc[6] if len(table) > 6 else None
        if row6 is not None:
            row6_str = " ".join(str(v) for v in row6.values if pd.notna(v))
            if "日期" in row6_str and ("買進" in row6_str or "賣出" in row6_str):
                target_table = table
                break

    if target_table is None:
        raise DataUnavailableError("MoneyDJ 找不到融資融券表格")

    # Extract data rows (skip header rows 0-6)
    data_rows = target_table.iloc[7:].copy()

    # Filter out summary rows (contain "合計" or non-date values in first column)
    def _is_valid_date_row(val):
        if pd.isna(val):
            return False
        text = str(val).strip()
        # Valid ROC date format: 115/02/11
        return bool(re.match(r"^\d{2,3}/\d{1,2}/\d{1,2}$", text))

    valid_mask = data_rows.iloc[:, 0].apply(_is_valid_date_row)
    data_rows = data_rows[valid_mask]

    if data_rows.empty:
        raise DataUnavailableError("MoneyDJ 融資融券無有效資料")

    # MoneyDJ column mapping (0-indexed):
    # 0: 日期, 1: 融資買進, 2: 融資賣出, 3: 融資現償, 4: 融資餘額, 5: 融資增減,
    # 6: 融資限額, 7: 融資使用率, 8: 融券賣出, 9: 融券買進, 10: 融券券償,
    # 11: 融券餘額, 12: 融券增減, 13: 券資比, 14: 資券相抵
    col_map = {
        0: "date",
        1: "margin_buy",
        2: "margin_sell",
        4: "margin_balance",
        5: "margin_change",
        8: "short_sell",
        9: "short_buy",
        11: "short_balance",
        12: "short_change",
        13: "short_margin_ratio",
    }

    result = pd.DataFrame()
    for idx, col_name in col_map.items():
        if idx < len(data_rows.columns):
            result[col_name] = data_rows.iloc[:, idx].values

    return result


def fetch_moneydj_holding_pct(
    session: requests.Session,
    symbol: str,
    start: dt.date,
    end: dt.date,
) -> pd.DataFrame:
    """Fetch institutional holding percentage from MoneyDJ for a single stock.

    Args:
        session: HTTP session
        symbol: Stock symbol (e.g., "2330")
        start: Start date
        end: End date

    Returns:
        DataFrame with columns: date, foreign_holding_pct, insti_holding_pct
        (percentage strings like "35.03%")
    """
    start_str = f"{start.year}-{start.month}-{start.day}"
    end_str = f"{end.year}-{end.month}-{end.day}"

    params = {"a": symbol, "c": start_str, "d": end_str}

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.get(MONEYDJ_HOLDING_URL, params=params, timeout=30, verify=False)
    response.raise_for_status()

    try:
        tables = pd.read_html(io.StringIO(response.text))
    except ValueError as exc:
        raise DataUnavailableError(f"MoneyDJ 法人持股頁面解析失敗：{exc}") from exc

    # Find the holding data table (11 columns, has header rows with 持股比重)
    # Table structure:
    # - Row 5: top-level headers (買賣超, 估計持股, 持股比重)
    # - Row 6: sub-headers (日期, 外資, 投信, 自營商, ...)
    # - Row 7+: data rows
    target_table = None
    for table in tables:
        if len(table) < 8 or len(table.columns) < 11:
            continue
        row6 = table.iloc[6] if len(table) > 6 else None
        if row6 is not None:
            row6_str = " ".join(str(v) for v in row6.values if pd.notna(v))
            if "日期" in row6_str and "外資" in row6_str:
                target_table = table
                break

    if target_table is None:
        raise DataUnavailableError("MoneyDJ 找不到法人持股表格")

    data_rows = target_table.iloc[7:].copy()

    def _is_valid_date_row(val):
        if pd.isna(val):
            return False
        text = str(val).strip()
        return bool(re.match(r"^\d{2,3}/\d{1,2}/\d{1,2}$", text))

    valid_mask = data_rows.iloc[:, 0].apply(_is_valid_date_row)
    data_rows = data_rows[valid_mask]

    if data_rows.empty:
        raise DataUnavailableError("MoneyDJ 法人持股無有效資料")

    # Column mapping (0-indexed):
    # 0: 日期, 9: 外資持股比重, 10: 三大法人持股比重
    result = pd.DataFrame()
    result["date"] = data_rows.iloc[:, 0].values
    result["foreign_holding_pct"] = data_rows.iloc[:, 9].values
    result["insti_holding_pct"] = data_rows.iloc[:, 10].values

    return result

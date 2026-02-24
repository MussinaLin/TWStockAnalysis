"""Alpha stock picking analysis module."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from .config import AppConfig


def build_alpha_sheets_batch(
    config: AppConfig,
    dates: list[dt.date],
    daily_file: Path,
    alpha_file: Path,
    sheet_prefix: str = "replay",
    preloaded_data: dict[str, pd.DataFrame] | None = None,
) -> dict[str, set[str]]:
    """Batch analyse multiple dates and write all results at once.

    This is optimized for replay mode - loads Excel once and writes all at once.

    Args:
        config: Application configuration
        dates: List of dates to analyze
        daily_file: Path to daily stock data Excel file
        alpha_file: Path to output alpha picks Excel file
        sheet_prefix: Prefix for output sheet names
        preloaded_data: Pre-loaded daily sheets data to avoid re-reading Excel

    Returns:
        Dict mapping date string to set of picked symbols (for sell exclusion).
    """
    if not dates:
        print("無日期可分析。")
        return {}

    if preloaded_data is not None:
        all_sheets_data = preloaded_data
    else:
        if not daily_file.exists():
            print("尚無每日資料，無法產生 alpha 分析。")
            return {}

        # Load Excel once
        print(f"載入 {daily_file}...")
        xls = pd.ExcelFile(daily_file)
        all_date_sheets_list = [s for s in xls.sheet_names if s != "market_closed"]
        if not all_date_sheets_list:
            print("尚無每日交易資料，無法產生 alpha 分析。")
            return {}

        # Pre-load all sheets into memory
        print("載入所有 sheets 到記憶體...")
        all_sheets_data = {}
        for s in all_date_sheets_list:
            all_sheets_data[s] = xls.parse(s)

    all_date_sheets = sorted(all_sheets_data.keys(), reverse=True)
    if not all_date_sheets:
        print("尚無每日交易資料，無法產生 alpha 分析。")
        return {}

    # Determine max sheets needed
    max_needed = max(config.alpha_insti_days_long, config.bb_narrow_long_days)

    # Process each date
    results: dict[str, pd.DataFrame] = {}
    for replay_date in dates:
        max_date_str = replay_date.isoformat()

        # Filter sheets up to this date
        date_sheets = [s for s in all_date_sheets if s <= max_date_str]
        if not date_sheets:
            print(f"跳過 {max_date_str}：無此日期之前的資料")
            continue

        if max_date_str not in all_sheets_data:
            print(f"跳過 {max_date_str}：sheet 不存在")
            continue

        # Get sheets needed for this date's analysis
        needed_sheets = date_sheets[:max_needed]
        recent = {s: all_sheets_data[s] for s in needed_sheets if s in all_sheets_data}

        # Analyze
        alpha_df = _analyze_date(config, date_sheets, recent)
        if alpha_df is not None and not alpha_df.empty:
            sheet_name = f"{sheet_prefix}_{max_date_str}"
            results[sheet_name] = alpha_df
            print(f"分析完成 {sheet_name}，共 {len(alpha_df)} 檔")
        else:
            print(f"跳過 {max_date_str}：無符合條件的股票")

    # Build return value: date_str -> set of picked symbols
    alpha_symbols: dict[str, set[str]] = {}
    for sheet_name, df in results.items():
        # sheet_name is like "alpha_2025-10-15", extract date part
        date_str = sheet_name.split("_", 1)[1] if "_" in sheet_name else sheet_name
        if "symbol" in df.columns:
            alpha_symbols[date_str] = set(df["symbol"].astype(str).str.strip())

    if not results:
        print("無分析結果可寫入。")
        return alpha_symbols

    # Write all results at once
    print(f"寫入 {len(results)} 個 sheets 到 {alpha_file}...")
    all_sheets: dict[str, pd.DataFrame]
    if alpha_file.exists():
        # Load existing sheets first
        existing_xls = pd.ExcelFile(alpha_file)
        all_sheets = {s: existing_xls.parse(s) for s in existing_xls.sheet_names}
        # Merge with new results (new overwrites existing)
        all_sheets.update(results)
    else:
        all_sheets = dict(results)

    with pd.ExcelWriter(alpha_file, engine="openpyxl", mode="w") as writer:
        for sheet_name, df in all_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"批次寫入完成，共 {len(results)} 個 sheets")

    # Update summary sheet using in-memory data (no re-read)
    build_summary_sheet(alpha_file, sheets_data=all_sheets)

    return alpha_symbols


def _analyze_date(
    config: AppConfig,
    date_sheets: list[str],
    recent: dict[str, pd.DataFrame],
) -> pd.DataFrame | None:
    """Analyze a single date using pre-loaded data."""
    if not date_sheets or not recent:
        return None

    latest_sheet = date_sheets[0]
    latest_df = recent.get(latest_sheet)
    if latest_df is None or "symbol" not in latest_df.columns:
        return None

    # Pre-build symbol indices for all sheets (one-time string conversion)
    sym_indices: dict[str, dict[str, int]] = {}
    for s, df in recent.items():
        sym_indices[s] = _build_symbol_index(df)

    symbols = list(sym_indices.get(latest_sheet, {}).keys())

    short_n = config.alpha_insti_days_short
    long_n = config.alpha_insti_days_long
    short_sheets = date_sheets[:short_n]
    long_sheets = date_sheets[:long_n]
    bb_short_sheets = date_sheets[:config.bb_narrow_short_days]
    bb_long_sheets = date_sheets[:config.bb_narrow_long_days]

    latest_idx = sym_indices.get(latest_sheet, {})

    rows: list[dict] = []
    for sym in symbols:
        row_data = _analyze_symbol(
            sym, latest_df, recent, short_sheets, long_sheets,
            bb_short_sheets, bb_long_sheets, config,
            sym_indices=sym_indices, latest_row_idx=latest_idx.get(sym),
        )
        if row_data:
            rows.append(row_data)

    if not rows:
        return None

    return pd.DataFrame(rows)


def build_alpha_sheet(
    config: AppConfig,
    target_date: dt.date,
    daily_file: Path,
    alpha_file: Path,
    max_date: dt.date | None = None,
    sheet_prefix: str = "alpha",
    preloaded_xls: pd.ExcelFile | None = None,
) -> set[str]:
    """Analyse recent trading data and write alpha picks to Excel.

    Args:
        config: Application configuration
        target_date: Target date for analysis
        daily_file: Path to daily stock data Excel file
        alpha_file: Path to output alpha picks Excel file
        max_date: If set, only consider sheets up to this date (for replay mode)
        sheet_prefix: Prefix for output sheet name (default: "alpha")
        preloaded_xls: Pre-loaded ExcelFile to avoid re-reading daily file

    Returns:
        Set of picked symbol strings (for sell exclusion).
    """
    if preloaded_xls is not None:
        xls = preloaded_xls
    else:
        if not daily_file.exists():
            print("尚無每日資料，無法產生 alpha 分析。")
            return set()
        xls = pd.ExcelFile(daily_file)

    date_sheets = sorted(
        [s for s in xls.sheet_names if s != "market_closed"],
        reverse=True
    )
    if not date_sheets:
        print("尚無每日交易資料，無法產生 alpha 分析。")
        return set()

    # Filter sheets by max_date if specified (for replay mode)
    if max_date is not None:
        max_date_str = max_date.isoformat()
        date_sheets = [s for s in date_sheets if s <= max_date_str]
        if not date_sheets:
            print(f"無 {max_date_str} 及之前的交易資料。")
            return set()

    long_n = config.alpha_insti_days_long
    short_n = config.alpha_insti_days_short
    bb_long_n = config.bb_narrow_long_days
    # Need enough sheets for both insti and BB narrow analysis
    max_needed = max(long_n, bb_long_n)
    needed_sheets = date_sheets[:max_needed]

    # Load recent sheets
    recent: dict[str, pd.DataFrame] = {}
    for s in needed_sheets:
        recent[s] = xls.parse(s)

    latest_sheet = date_sheets[0]
    latest_df = recent[latest_sheet]
    if "symbol" not in latest_df.columns:
        print("最新 sheet 缺少 symbol 欄位，無法分析。")
        return set()

    # Pre-build symbol indices for fast lookup
    sym_indices: dict[str, dict[str, int]] = {}
    for s, df in recent.items():
        sym_indices[s] = _build_symbol_index(df)

    symbols = list(sym_indices.get(latest_sheet, {}).keys())
    short_sheets = date_sheets[:short_n]
    long_sheets = date_sheets[:long_n]
    bb_short_sheets = date_sheets[:config.bb_narrow_short_days]
    bb_long_sheets = date_sheets[:config.bb_narrow_long_days]

    latest_idx = sym_indices.get(latest_sheet, {})

    rows: list[dict] = []
    for sym in symbols:
        row_data = _analyze_symbol(
            sym, latest_df, recent, short_sheets, long_sheets,
            bb_short_sheets, bb_long_sheets, config,
            sym_indices=sym_indices, latest_row_idx=latest_idx.get(sym),
        )
        if row_data:
            rows.append(row_data)

    if not rows:
        print("未找到符合 alpha 條件的股票。")
        return set()

    alpha_df = pd.DataFrame(rows)
    picked_symbols = set(alpha_df["symbol"].astype(str).str.strip())
    sheet_name = f"{sheet_prefix}_{target_date.isoformat()}"

    if alpha_file.exists():
        with pd.ExcelWriter(
            alpha_file, engine="openpyxl", mode="a", if_sheet_exists="replace"
        ) as writer:
            alpha_df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(alpha_file, engine="openpyxl", mode="w") as writer:
            alpha_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Alpha 分析已寫入 {alpha_file} ({sheet_name})，共 {len(rows)} 檔")

    # Update summary sheet
    build_summary_sheet(alpha_file)

    return picked_symbols


def _analyze_symbol(
    sym: str,
    latest_df: pd.DataFrame,
    recent: dict[str, pd.DataFrame],
    short_sheets: list[str],
    long_sheets: list[str],
    bb_short_sheets: list[str],
    bb_long_sheets: list[str],
    config: AppConfig,
    sym_indices: dict[str, dict[str, int]] | None = None,
    latest_row_idx: int | None = None,
) -> dict | None:
    """Analyze a single symbol for alpha conditions.

    Returns:
        Dict with analysis data if any condition met, None otherwise.
    """
    if latest_row_idx is not None:
        if latest_row_idx >= len(latest_df):
            return None
        r = latest_df.iloc[latest_row_idx]
    else:
        row_latest = latest_df[latest_df["symbol"].astype(str).str.strip() == sym]
        if row_latest.empty:
            return None
        r = row_latest.iloc[0]
    name = str(r.get("name", "")).strip()
    close = r.get("close")
    rsi = r.get("rsi_14")
    macd = r.get("macd")
    macd_signal = r.get("macd_signal")
    macd_hist = r.get("macd_hist")
    volume = r.get("volume")
    vol_ma5 = r.get("vol_ma5")
    vol_ma10 = r.get("vol_ma10")
    vol_ma20 = r.get("vol_ma20")
    bb_upper = r.get("bb_upper")
    bb_bandwidth = r.get("bb_bandwidth")
    bb_percent_b = r.get("bb_percent_b")
    turnover_rate = r.get("turnover_rate")
    turnover_ma20 = r.get("turnover_ma20")

    # Collect institutional net across recent sheets
    insti_short = _collect_values(
        sym, recent, short_sheets, "institutional_investors_net", sym_indices
    )
    insti_long = _collect_values(
        sym, recent, long_sheets, "institutional_investors_net", sym_indices
    )

    insti_short_sum = sum(insti_short) if insti_short else None
    insti_short_avg = (insti_short_sum / len(insti_short)) if insti_short else None
    insti_long_avg = (sum(insti_long) / len(insti_long)) if insti_long else None

    # Collect BB bandwidth across recent sheets
    bb_bw_short = _collect_values(sym, recent, bb_short_sheets, "bb_bandwidth", sym_indices)
    bb_bw_long = _collect_values(sym, recent, bb_long_sheets, "bb_bandwidth", sym_indices)

    bb_bw_short_avg = (sum(bb_bw_short) / len(bb_bw_short)) if bb_bw_short else None
    bb_bw_long_avg = (sum(bb_bw_long) / len(bb_bw_long)) if bb_bw_long else None

    short_n = config.alpha_insti_days_short
    long_n = config.alpha_insti_days_long
    bb_short_n = config.bb_narrow_short_days
    bb_long_n = config.bb_narrow_long_days

    # Evaluate conditions
    cond_insti = (
        insti_short_sum is not None
        and insti_long_avg is not None
        and insti_short_avg is not None
        and insti_short_sum > 0
        and insti_short_avg > insti_long_avg
    )

    cond_rsi = (
        rsi is not None
        and not pd.isna(rsi)
        and config.alpha_rsi_min <= float(rsi) <= config.alpha_rsi_max
    )

    cond_macd = (
        macd_hist is not None
        and not pd.isna(macd_hist)
        and float(macd_hist) > config.alpha_macd_hist_min
    )

    vol_ratio = config.vol_breakout_ratio

    cond_vol_ma10 = (
        volume is not None
        and vol_ma10 is not None
        and not pd.isna(volume)
        and not pd.isna(vol_ma10)
        and float(volume) > float(vol_ma10) * vol_ratio
    )

    cond_vol_ma20 = (
        volume is not None
        and vol_ma20 is not None
        and not pd.isna(volume)
        and not pd.isna(vol_ma20)
        and float(volume) > float(vol_ma20) * vol_ratio
    )

    cond_bb_narrow = (
        bb_bw_short_avg is not None
        and bb_bw_long_avg is not None
        and bb_bw_short_avg < bb_bw_long_avg
    )

    cond_bb_near_upper = (
        bb_percent_b is not None
        and not pd.isna(bb_percent_b)
        and float(bb_percent_b) > config.bb_percent_b_min
    )

    cond_turnover_surge = (
        turnover_rate is not None
        and turnover_ma20 is not None
        and not pd.isna(turnover_rate)
        and not pd.isna(turnover_ma20)
        and turnover_ma20 > 0
        and float(turnover_rate) > float(turnover_ma20) * config.turnover_surge_ratio
    )

    # cond_insti_bullish: 法人看好（當天買超 or 賣壓減緩）
    insti_today = insti_short[0] if insti_short else None
    if insti_today is not None and insti_today > 0:
        cond_insti_bullish = True
    elif insti_today is not None and insti_today <= 0:
        sell_days = [abs(v) for v in insti_short if v < 0]
        if sell_days:
            avg_sell = sum(sell_days) / len(sell_days)
            cond_insti_bullish = abs(insti_today) < config.alpha_insti_bullish_ratio * avg_sell
        else:
            cond_insti_bullish = False
    else:
        cond_insti_bullish = False

    # Selection logic:
    # 1. Required: cond_insti AND cond_insti_bullish AND (cond_vol_ma10 OR cond_vol_ma20)
    # 2. Optional: at least 2 of [cond_rsi, cond_macd, cond_bb_narrow, cond_bb_near_upper, cond_turnover_surge]
    required_met = cond_insti and cond_insti_bullish and (cond_vol_ma10 or cond_vol_ma20)
    optional_count = sum([cond_rsi, cond_macd, cond_bb_narrow, cond_bb_near_upper, cond_turnover_surge])
    optional_met = optional_count >= 2

    if not (required_met and optional_met):
        return None

    # Build reasons
    reasons: list[str] = []
    if cond_insti:
        reasons.append(
            f"法人加碼：近{len(insti_short)}日淨買超合計"
            f"{insti_short_sum:+,.0f}，"
            f"日均{insti_short_avg:+,.0f} > "
            f"近{len(insti_long)}日均{insti_long_avg:+,.0f}"
        )
    if cond_insti_bullish:
        if insti_today is not None and insti_today > 0:
            reasons.append(f"法人看好：當日買超 {insti_today:+,.0f}")
        else:
            sell_days_vals = [abs(v) for v in insti_short if v < 0]
            avg_s = (sum(sell_days_vals) / len(sell_days_vals)) if sell_days_vals else 0
            reasons.append(
                f"法人看好：賣壓減緩 {abs(insti_today):,.0f}"
                f" < {config.alpha_insti_bullish_ratio}×均賣超{avg_s:,.0f}"
            )
    if cond_rsi:
        reasons.append(
            f"RSI 健康：{float(rsi):.1f}（區間 {config.alpha_rsi_min}-{config.alpha_rsi_max}）"
        )
    if cond_macd:
        reasons.append(f"MACD 多方：histogram {float(macd_hist):+.2f}")
    if cond_vol_ma10:
        reasons.append(f"量突破10MA：{int(volume):,} > {int(vol_ma10):,}×{vol_ratio}")
    if cond_vol_ma20:
        reasons.append(f"量突破20MA：{int(volume):,} > {int(vol_ma20):,}×{vol_ratio}")
    if cond_bb_narrow:
        reasons.append(
            f"布林收窄：近{bb_short_n}日BW均{bb_bw_short_avg:.4f} < "
            f"近{bb_long_n}日BW均{bb_bw_long_avg:.4f}"
        )
    if cond_bb_near_upper:
        reasons.append(
            f"接近布林上軌：%B={float(bb_percent_b):.2f} > {config.bb_percent_b_min}"
        )
    if cond_turnover_surge:
        turnover_ratio = float(turnover_rate) / float(turnover_ma20)
        reasons.append(
            f"週轉率爆升：{float(turnover_rate)*100:.2f}%>{float(turnover_ma20)*100:.2f}%×{config.turnover_surge_ratio}({turnover_ratio:.1f}倍)"
        )

    return {
        "symbol": sym,
        "name": name,
        "close": close,
        "volume": volume,
        "vol_ma5": vol_ma5,
        "vol_ma10": vol_ma10,
        "vol_ma20": vol_ma20,
        "rsi_14": round(float(rsi), 2) if pd.notna(rsi) else None,
        "macd": round(float(macd), 2) if pd.notna(macd) else None,
        "macd_signal": round(float(macd_signal), 2) if pd.notna(macd_signal) else None,
        "macd_hist": round(float(macd_hist), 2) if pd.notna(macd_hist) else None,
        f"insti_net_{short_n}d_sum": insti_short_sum,
        f"insti_net_{short_n}d_avg": round(insti_short_avg, 0) if insti_short_avg else None,
        f"insti_net_{long_n}d_avg": round(insti_long_avg, 0) if insti_long_avg else None,
        "bb_upper": round(float(bb_upper), 2) if pd.notna(bb_upper) else None,
        "bb_bandwidth": round(float(bb_bandwidth), 4) if pd.notna(bb_bandwidth) else None,
        "bb_percent_b": round(float(bb_percent_b), 4) if pd.notna(bb_percent_b) else None,
        f"bb_bw_{bb_short_n}d_avg": round(bb_bw_short_avg, 4) if bb_bw_short_avg else None,
        f"bb_bw_{bb_long_n}d_avg": round(bb_bw_long_avg, 4) if bb_bw_long_avg else None,
        "cond_insti": cond_insti,
        "cond_insti_bullish": cond_insti_bullish,
        "cond_rsi": cond_rsi,
        "cond_macd": cond_macd,
        "cond_vol_ma10": cond_vol_ma10,
        "cond_vol_ma20": cond_vol_ma20,
        "cond_bb_narrow": cond_bb_narrow,
        "cond_bb_near_upper": cond_bb_near_upper,
        "cond_turnover_surge": cond_turnover_surge,
        "reasons": "；".join(reasons),
    }


def _build_symbol_index(df: pd.DataFrame) -> dict[str, int]:
    """Build a symbol -> row index mapping for fast lookup.

    Performs .astype(str).str.strip() once per DataFrame instead of per-symbol.
    """
    if "symbol" not in df.columns:
        return {}
    symbols = df["symbol"].astype(str).str.strip()
    return {sym: idx for idx, sym in symbols.items()}


def _collect_values(
    sym: str,
    recent: dict[str, pd.DataFrame],
    sheets: list[str],
    column: str,
    sym_indices: dict[str, dict[str, int]] | None = None,
) -> list[float]:
    """Collect values for a symbol across multiple sheets."""
    values: list[float] = []
    for s in sheets:
        df = recent.get(s)
        if df is None:
            continue
        if sym_indices is not None and s in sym_indices:
            idx = sym_indices[s].get(sym)
            if idx is None:
                continue
            val = df.iloc[idx].get(column) if idx < len(df) else None
        else:
            row = df[df["symbol"].astype(str).str.strip() == sym]
            if row.empty:
                continue
            val = row.iloc[0].get(column)
        if pd.notna(val):
            values.append(float(val))
    return values


def build_summary_sheet(
    alpha_file: Path,
    sheets_data: dict[str, pd.DataFrame] | None = None,
) -> None:
    """Build a summary sheet showing stock appearance frequency across all sheets.

    Args:
        alpha_file: Path to alpha picks Excel file
        sheets_data: Optional pre-loaded sheets data to avoid re-reading file
    """
    if sheets_data is not None:
        all_sheet_names = list(sheets_data.keys())
    else:
        if not alpha_file.exists():
            return
        xls = pd.ExcelFile(alpha_file)
        all_sheet_names = xls.sheet_names

    # Get all alpha/replay sheets, exclude summary and market_closed
    data_sheet_names = [
        s for s in all_sheet_names
        if s not in ("summary", "market_closed")
        and (s.startswith("alpha_") or s.startswith("replay_"))
    ]

    if not data_sheet_names:
        return

    # Collect stock appearances: {symbol: {name, dates}}
    stock_data: dict[str, dict] = {}

    for sheet_name in data_sheet_names:
        # Extract date from sheet name (alpha_2025-10-15 or replay_2025-10-15)
        date_part = sheet_name.split("_", 1)[1] if "_" in sheet_name else sheet_name

        if sheets_data is not None:
            df = sheets_data[sheet_name]
        else:
            df = xls.parse(sheet_name)
        if "symbol" not in df.columns:
            continue

        for _, row in df.iterrows():
            sym = str(row.get("symbol", "")).strip()
            if not sym:
                continue
            name = str(row.get("name", "")).strip()

            if sym not in stock_data:
                stock_data[sym] = {"name": name, "dates": set()}
            stock_data[sym]["dates"].add(date_part)
            # Update name if we have a better one
            if name and not stock_data[sym]["name"]:
                stock_data[sym]["name"] = name

    if not stock_data:
        return

    # Get all unique dates, sorted
    all_dates = sorted(set(
        d for data in stock_data.values() for d in data["dates"]
    ))

    # Build summary DataFrame
    rows = []
    for sym in sorted(stock_data.keys()):
        data = stock_data[sym]
        row = {
            "symbol": sym,
            "name": data["name"],
            "count": len(data["dates"]),
        }
        # Add date columns with ● marker
        for date in all_dates:
            # Use shorter date format for column header (MM-DD)
            col_name = date[5:] if len(date) == 10 else date  # 2025-10-15 -> 10-15
            row[col_name] = "●" if date in data["dates"] else ""
        rows.append(row)

    summary_df = pd.DataFrame(rows)

    # Reorder columns: symbol, name, count, then dates
    date_cols = [d[5:] if len(d) == 10 else d for d in all_dates]
    col_order = ["symbol", "name", "count"] + date_cols
    summary_df = summary_df[[c for c in col_order if c in summary_df.columns]]

    # Write summary sheet
    if sheets_data is not None:
        existing_sheets = {s: sheets_data[s] for s in all_sheet_names if s != "summary"}
    else:
        existing_sheets = {s: xls.parse(s) for s in all_sheet_names if s != "summary"}
    existing_sheets["summary"] = summary_df

    with pd.ExcelWriter(alpha_file, engine="openpyxl", mode="w") as writer:
        # Write summary first
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        # Then other sheets
        for sheet_name, df in existing_sheets.items():
            if sheet_name != "summary":
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Summary 已更新：{len(stock_data)} 檔股票，{len(all_dates)} 個日期")

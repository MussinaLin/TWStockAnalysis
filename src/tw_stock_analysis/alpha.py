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
) -> None:
    """Batch analyse multiple dates and write all results at once.

    This is optimized for replay mode - loads Excel once and writes all at once.

    Args:
        config: Application configuration
        dates: List of dates to analyze
        daily_file: Path to daily stock data Excel file
        alpha_file: Path to output alpha picks Excel file
        sheet_prefix: Prefix for output sheet names
    """
    if not daily_file.exists():
        print("尚無每日資料，無法產生 alpha 分析。")
        return

    if not dates:
        print("無日期可分析。")
        return

    # Load Excel once
    print(f"載入 {daily_file}...")
    xls = pd.ExcelFile(daily_file)
    all_date_sheets = sorted(
        [s for s in xls.sheet_names if s != "market_closed"],
        reverse=True
    )
    if not all_date_sheets:
        print("尚無每日交易資料，無法產生 alpha 分析。")
        return

    # Determine max sheets needed
    max_needed = max(config.alpha_insti_days_long, config.bb_narrow_long_days)

    # Pre-load all sheets into memory
    print(f"載入所有 sheets 到記憶體...")
    all_sheets_data: dict[str, pd.DataFrame] = {}
    for s in all_date_sheets:
        all_sheets_data[s] = xls.parse(s)

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

    if not results:
        print("無分析結果可寫入。")
        return

    # Write all results at once
    print(f"寫入 {len(results)} 個 sheets 到 {alpha_file}...")
    if alpha_file.exists():
        # Load existing sheets first
        existing_xls = pd.ExcelFile(alpha_file)
        existing_sheets = {s: existing_xls.parse(s) for s in existing_xls.sheet_names}
        # Merge with new results (new overwrites existing)
        existing_sheets.update(results)
        with pd.ExcelWriter(alpha_file, engine="openpyxl", mode="w") as writer:
            for sheet_name, df in existing_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(alpha_file, engine="openpyxl", mode="w") as writer:
            for sheet_name, df in results.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"批次寫入完成，共 {len(results)} 個 sheets")


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

    symbols = latest_df["symbol"].astype(str).str.strip().tolist()

    short_n = config.alpha_insti_days_short
    long_n = config.alpha_insti_days_long
    short_sheets = date_sheets[:short_n]
    long_sheets = date_sheets[:long_n]
    bb_short_sheets = date_sheets[:config.bb_narrow_short_days]
    bb_long_sheets = date_sheets[:config.bb_narrow_long_days]

    rows: list[dict] = []
    for sym in symbols:
        row_data = _analyze_symbol(
            sym, latest_df, recent, short_sheets, long_sheets,
            bb_short_sheets, bb_long_sheets, config
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
) -> None:
    """Analyse recent trading data and write alpha picks to Excel.

    Args:
        config: Application configuration
        target_date: Target date for analysis
        daily_file: Path to daily stock data Excel file
        alpha_file: Path to output alpha picks Excel file
        max_date: If set, only consider sheets up to this date (for replay mode)
        sheet_prefix: Prefix for output sheet name (default: "alpha")
    """
    if not daily_file.exists():
        print("尚無每日資料，無法產生 alpha 分析。")
        return

    xls = pd.ExcelFile(daily_file)
    date_sheets = sorted(
        [s for s in xls.sheet_names if s != "market_closed"],
        reverse=True
    )
    if not date_sheets:
        print("尚無每日交易資料，無法產生 alpha 分析。")
        return

    # Filter sheets by max_date if specified (for replay mode)
    if max_date is not None:
        max_date_str = max_date.isoformat()
        date_sheets = [s for s in date_sheets if s <= max_date_str]
        if not date_sheets:
            print(f"無 {max_date_str} 及之前的交易資料。")
            return

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
        return

    symbols = latest_df["symbol"].astype(str).str.strip().tolist()
    short_sheets = date_sheets[:short_n]
    long_sheets = date_sheets[:long_n]
    bb_short_sheets = date_sheets[:config.bb_narrow_short_days]
    bb_long_sheets = date_sheets[:config.bb_narrow_long_days]

    rows: list[dict] = []
    for sym in symbols:
        row_data = _analyze_symbol(
            sym, latest_df, recent, short_sheets, long_sheets,
            bb_short_sheets, bb_long_sheets, config
        )
        if row_data:
            rows.append(row_data)

    if not rows:
        print("未找到符合 alpha 條件的股票。")
        return

    alpha_df = pd.DataFrame(rows)
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


def _analyze_symbol(
    sym: str,
    latest_df: pd.DataFrame,
    recent: dict[str, pd.DataFrame],
    short_sheets: list[str],
    long_sheets: list[str],
    bb_short_sheets: list[str],
    bb_long_sheets: list[str],
    config: AppConfig,
) -> dict | None:
    """Analyze a single symbol for alpha conditions.

    Returns:
        Dict with analysis data if any condition met, None otherwise.
    """
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

    # Collect institutional net across recent sheets
    insti_short = _collect_values(sym, recent, short_sheets, "institutional_investors_net")
    insti_long = _collect_values(sym, recent, long_sheets, "institutional_investors_net")

    insti_short_sum = sum(insti_short) if insti_short else None
    insti_short_avg = (insti_short_sum / len(insti_short)) if insti_short else None
    insti_long_avg = (sum(insti_long) / len(insti_long)) if insti_long else None

    # Collect BB bandwidth across recent sheets
    bb_bw_short = _collect_values(sym, recent, bb_short_sheets, "bb_bandwidth")
    bb_bw_long = _collect_values(sym, recent, bb_long_sheets, "bb_bandwidth")

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

    if not (cond_insti or cond_rsi or cond_macd or cond_vol_ma10
            or cond_vol_ma20 or cond_bb_narrow or cond_bb_near_upper):
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
        "cond_rsi": cond_rsi,
        "cond_macd": cond_macd,
        "cond_vol_ma10": cond_vol_ma10,
        "cond_vol_ma20": cond_vol_ma20,
        "cond_bb_narrow": cond_bb_narrow,
        "cond_bb_near_upper": cond_bb_near_upper,
        "reasons": "；".join(reasons),
    }


def _collect_values(
    sym: str,
    recent: dict[str, pd.DataFrame],
    sheets: list[str],
    column: str,
) -> list[float]:
    """Collect values for a symbol across multiple sheets."""
    values: list[float] = []
    for s in sheets:
        df = recent.get(s)
        if df is None:
            continue
        row = df[df["symbol"].astype(str).str.strip() == sym]
        if row.empty:
            continue
        val = row.iloc[0].get(column)
        if pd.notna(val):
            values.append(float(val))
    return values

"""Sell alert analysis module."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from .config import AppConfig


def build_sell_sheet(
    config: AppConfig,
    target_date: dt.date,
    daily_file: Path,
    sell_file: Path,
    max_date: dt.date | None = None,
    sheet_prefix: str = "sell",
) -> None:
    """Analyse recent trading data and write sell alerts to Excel.

    Args:
        config: Application configuration
        target_date: Target date for analysis
        daily_file: Path to daily stock data Excel file
        sell_file: Path to output sell alerts Excel file
        max_date: If set, only consider sheets up to this date (for replay mode)
        sheet_prefix: Prefix for output sheet name (default: "sell")
    """
    if not daily_file.exists():
        print("尚無每日資料，無法產生賣出分析。")
        return

    xls = pd.ExcelFile(daily_file)
    date_sheets = sorted(
        [s for s in xls.sheet_names if s != "market_closed"],
        reverse=True
    )
    if not date_sheets:
        print("尚無每日交易資料，無法產生賣出分析。")
        return

    # Filter sheets by max_date if specified (for replay mode)
    if max_date is not None:
        max_date_str = max_date.isoformat()
        date_sheets = [s for s in date_sheets if s <= max_date_str]
        if not date_sheets:
            print(f"無 {max_date_str} 及之前的交易資料。")
            return

    # Need enough sheets for analysis
    max_needed = max(
        config.sell_insti_days_long,
        config.sell_price_high_days + 1,  # +1 for previous day comparison
    )
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

    rows: list[dict] = []
    for sym in symbols:
        row_data = _analyze_symbol_sell(sym, latest_df, recent, date_sheets, config)
        if row_data:
            rows.append(row_data)

    if not rows:
        print("未找到符合賣出條件的股票。")
        return

    sell_df = pd.DataFrame(rows)
    sheet_name = f"{sheet_prefix}_{target_date.isoformat()}"

    if sell_file.exists():
        with pd.ExcelWriter(
            sell_file, engine="openpyxl", mode="a", if_sheet_exists="replace"
        ) as writer:
            sell_df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(sell_file, engine="openpyxl", mode="w") as writer:
            sell_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Sell 分析已寫入 {sell_file} ({sheet_name})，共 {len(rows)} 檔")

    # Update summary sheet
    build_sell_summary_sheet(sell_file)


def build_sell_sheets_batch(
    config: AppConfig,
    dates: list[dt.date],
    daily_file: Path,
    sell_file: Path,
    sheet_prefix: str = "sell",
) -> None:
    """Batch analyse multiple dates for sell alerts and write all results at once."""
    if not daily_file.exists():
        print("尚無每日資料，無法產生賣出分析。")
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
        print("尚無每日交易資料，無法產生賣出分析。")
        return

    # Determine max sheets needed
    max_needed = max(
        config.sell_insti_days_long,
        config.sell_price_high_days + 1,
    )

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
        sell_df = _analyze_date_sell(config, date_sheets, recent)
        if sell_df is not None and not sell_df.empty:
            sheet_name = f"{sheet_prefix}_{max_date_str}"
            results[sheet_name] = sell_df
            print(f"賣出分析完成 {sheet_name}，共 {len(sell_df)} 檔")
        else:
            print(f"跳過 {max_date_str}：無符合賣出條件的股票")

    if not results:
        print("無賣出分析結果可寫入。")
        return

    # Write all results at once
    print(f"寫入 {len(results)} 個 sheets 到 {sell_file}...")
    if sell_file.exists():
        existing_xls = pd.ExcelFile(sell_file)
        existing_sheets = {s: existing_xls.parse(s) for s in existing_xls.sheet_names}
        existing_sheets.update(results)
        with pd.ExcelWriter(sell_file, engine="openpyxl", mode="w") as writer:
            for sheet_name, df in existing_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(sell_file, engine="openpyxl", mode="w") as writer:
            for sheet_name, df in results.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"批次寫入完成，共 {len(results)} 個 sheets")

    # Update summary sheet
    build_sell_summary_sheet(sell_file)


def _analyze_date_sell(
    config: AppConfig,
    date_sheets: list[str],
    recent: dict[str, pd.DataFrame],
) -> pd.DataFrame | None:
    """Analyze a single date for sell alerts using pre-loaded data."""
    if not date_sheets or not recent:
        return None

    latest_sheet = date_sheets[0]
    latest_df = recent.get(latest_sheet)
    if latest_df is None or "symbol" not in latest_df.columns:
        return None

    symbols = latest_df["symbol"].astype(str).str.strip().tolist()

    rows: list[dict] = []
    for sym in symbols:
        row_data = _analyze_symbol_sell(sym, latest_df, recent, date_sheets, config)
        if row_data:
            rows.append(row_data)

    if not rows:
        return None

    return pd.DataFrame(rows)


def _analyze_symbol_sell(
    sym: str,
    latest_df: pd.DataFrame,
    recent: dict[str, pd.DataFrame],
    date_sheets: list[str],
    config: AppConfig,
) -> dict | None:
    """Analyze a single symbol for sell conditions.

    Returns:
        Dict with analysis data if any condition met, None otherwise.
    """
    row_latest = latest_df[latest_df["symbol"].astype(str).str.strip() == sym]
    if row_latest.empty:
        return None

    r = row_latest.iloc[0]
    name = str(r.get("name", "")).strip()
    open_price = r.get("open")
    close = r.get("close")
    high = r.get("high")
    rsi = r.get("rsi_14")
    macd_hist = r.get("macd_hist")
    volume = r.get("volume")
    vol_ma10 = r.get("vol_ma10")
    bb_percent_b = r.get("bb_percent_b")

    short_n = config.sell_insti_days_short
    long_n = config.sell_insti_days_long
    short_sheets = date_sheets[:short_n]
    long_sheets = date_sheets[:long_n]
    price_high_days = config.sell_price_high_days
    price_sheets = date_sheets[:price_high_days]

    # Collect foreign net
    foreign_short = _collect_values(sym, recent, short_sheets, "foreign_net")
    foreign_long = _collect_values(sym, recent, long_sheets, "foreign_net")

    # Collect trust net
    trust_short = _collect_values(sym, recent, short_sheets, "trust_net")
    trust_long = _collect_values(sym, recent, long_sheets, "trust_net")

    # Collect close prices for high detection
    close_history = _collect_values(sym, recent, price_sheets, "close")

    # Collect RSI history
    rsi_history = _collect_values(sym, recent, price_sheets, "rsi_14")

    # Collect MACD hist history (need at least 2 days)
    macd_hist_history = _collect_values(sym, recent, date_sheets[:2], "macd_hist")

    # Calculate averages
    foreign_short_sum = sum(foreign_short) if foreign_short else None
    foreign_short_avg = (foreign_short_sum / len(foreign_short)) if foreign_short else None
    foreign_long_avg = (sum(foreign_long) / len(foreign_long)) if foreign_long else None

    trust_short_sum = sum(trust_short) if trust_short else None
    trust_short_avg = (trust_short_sum / len(trust_short)) if trust_short else None
    trust_long_avg = (sum(trust_long) / len(trust_long)) if trust_long else None

    # --- Evaluate all 11 conditions ---

    # 1. Foreign net sell (short-term) < 0
    cond_foreign_sell = (
        foreign_short_sum is not None
        and foreign_short_sum < 0
    )

    # 2. Foreign selling acceleration (short avg < 0 AND short avg < long avg)
    cond_foreign_accel = (
        foreign_short_avg is not None
        and foreign_long_avg is not None
        and foreign_short_avg < 0
        and foreign_short_avg < foreign_long_avg
    )

    # 3. Trust net sell (short-term) < 0
    cond_trust_sell = (
        trust_short_sum is not None
        and trust_short_sum < 0
    )

    # 4. Trust selling acceleration (short avg < 0 AND short avg < long avg)
    cond_trust_accel = (
        trust_short_avg is not None
        and trust_long_avg is not None
        and trust_short_avg < 0
        and trust_short_avg < trust_long_avg
    )

    # 5. High volume long black candle
    cond_high_black = False
    if (
        high is not None and close is not None and open_price is not None
        and volume is not None and vol_ma10 is not None
        and pd.notna(high) and pd.notna(close) and pd.notna(open_price)
        and pd.notna(volume) and pd.notna(vol_ma10)
    ):
        upper_shadow = float(high) - float(close)
        threshold = float(open_price) * config.sell_high_black_ratio
        vol_breakout = float(volume) > float(vol_ma10) * config.vol_breakout_ratio
        cond_high_black = upper_shadow > threshold and vol_breakout

    # 6. Price up volume down (divergence)
    cond_price_up_vol_down = False
    if (
        close is not None and volume is not None and vol_ma10 is not None
        and pd.notna(close) and pd.notna(volume) and pd.notna(vol_ma10)
        and len(close_history) >= price_high_days
    ):
        is_price_high = float(close) >= max(close_history)
        vol_shrink = float(volume) < float(vol_ma10) * config.sell_volume_shrink_ratio
        cond_price_up_vol_down = is_price_high and vol_shrink

    # 7. RSI overbought
    cond_rsi_overbought = (
        rsi is not None
        and pd.notna(rsi)
        and float(rsi) > config.sell_rsi_overbought
    )

    # 8. RSI divergence: price high but RSI not high
    cond_rsi_divergence = False
    if (
        close is not None and rsi is not None
        and pd.notna(close) and pd.notna(rsi)
        and len(close_history) >= price_high_days
        and len(rsi_history) >= price_high_days
    ):
        is_price_high = float(close) >= max(close_history)
        is_rsi_not_high = float(rsi) < max(rsi_history)
        cond_rsi_divergence = is_price_high and is_rsi_not_high

    # 9. MACD hist turns negative (positive -> negative)
    cond_macd_turn_neg = False
    if len(macd_hist_history) >= 2:
        today_hist = macd_hist_history[0]
        yesterday_hist = macd_hist_history[1]
        cond_macd_turn_neg = yesterday_hist > 0 and today_hist < 0

    # 10. MACD divergence: price high but macd_hist not high
    cond_macd_divergence = False
    macd_hist_long = _collect_values(sym, recent, price_sheets, "macd_hist")
    if (
        close is not None and macd_hist is not None
        and pd.notna(close) and pd.notna(macd_hist)
        and len(close_history) >= price_high_days
        and len(macd_hist_long) >= price_high_days
    ):
        is_price_high = float(close) >= max(close_history)
        is_macd_not_high = float(macd_hist) < max(macd_hist_long)
        cond_macd_divergence = is_price_high and is_macd_not_high

    # 11. Price breaks below Bollinger middle
    cond_bb_below = (
        bb_percent_b is not None
        and pd.notna(bb_percent_b)
        and float(bb_percent_b) < config.sell_bb_percent_b_max
    )

    # Check if any condition is met (OR logic)
    any_condition_met = any([
        cond_foreign_sell,
        cond_foreign_accel,
        cond_trust_sell,
        cond_trust_accel,
        cond_high_black,
        cond_price_up_vol_down,
        cond_rsi_overbought,
        cond_rsi_divergence,
        cond_macd_turn_neg,
        cond_macd_divergence,
        cond_bb_below,
    ])

    if not any_condition_met:
        return None

    # Build reasons
    reasons: list[str] = []
    if cond_foreign_sell:
        reasons.append(f"外資近{short_n}日淨賣超{foreign_short_sum:+,.0f}")
    if cond_foreign_accel:
        reasons.append(f"外資賣超加速：近{short_n}日均{foreign_short_avg:+,.0f}<近{long_n}日均{foreign_long_avg:+,.0f}")
    if cond_trust_sell:
        reasons.append(f"投信近{short_n}日淨賣超{trust_short_sum:+,.0f}")
    if cond_trust_accel:
        reasons.append(f"投信賣超加速：近{short_n}日均{trust_short_avg:+,.0f}<近{long_n}日均{trust_long_avg:+,.0f}")
    if cond_high_black:
        reasons.append(f"高檔爆量長黑：上影線>{config.sell_high_black_ratio*100:.0f}%開盤價")
    if cond_price_up_vol_down:
        reasons.append(f"價漲量縮：創{price_high_days}日新高但量縮")
    if cond_rsi_overbought:
        reasons.append(f"RSI超買：{float(rsi):.1f}>{config.sell_rsi_overbought}")
    if cond_rsi_divergence:
        reasons.append(f"RSI背離：股價創{price_high_days}日高但RSI未創高")
    if cond_macd_turn_neg:
        reasons.append(f"MACD柱轉負：{macd_hist_history[1]:+.2f}→{macd_hist_history[0]:+.2f}")
    if cond_macd_divergence:
        reasons.append(f"MACD背離：股價創{price_high_days}日高但MACD柱未創高")
    if cond_bb_below:
        reasons.append(f"跌破布林中軌：%B={float(bb_percent_b):.2f}<{config.sell_bb_percent_b_max}")

    # Count how many conditions are met
    conditions_met = sum([
        cond_foreign_sell,
        cond_foreign_accel,
        cond_trust_sell,
        cond_trust_accel,
        cond_high_black,
        cond_price_up_vol_down,
        cond_rsi_overbought,
        cond_rsi_divergence,
        cond_macd_turn_neg,
        cond_macd_divergence,
        cond_bb_below,
    ])

    return {
        "symbol": sym,
        "name": name,
        "close": close,
        "volume": volume,
        "vol_ma10": vol_ma10,
        "rsi_14": round(float(rsi), 2) if pd.notna(rsi) else None,
        "macd_hist": round(float(macd_hist), 2) if pd.notna(macd_hist) else None,
        "bb_percent_b": round(float(bb_percent_b), 4) if pd.notna(bb_percent_b) else None,
        f"foreign_net_{short_n}d_sum": foreign_short_sum,
        f"foreign_net_{short_n}d_avg": round(foreign_short_avg, 0) if foreign_short_avg else None,
        f"foreign_net_{long_n}d_avg": round(foreign_long_avg, 0) if foreign_long_avg else None,
        f"trust_net_{short_n}d_sum": trust_short_sum,
        f"trust_net_{short_n}d_avg": round(trust_short_avg, 0) if trust_short_avg else None,
        f"trust_net_{long_n}d_avg": round(trust_long_avg, 0) if trust_long_avg else None,
        "cond_foreign_sell": cond_foreign_sell,
        "cond_foreign_accel": cond_foreign_accel,
        "cond_trust_sell": cond_trust_sell,
        "cond_trust_accel": cond_trust_accel,
        "cond_high_black": cond_high_black,
        "cond_price_up_vol_down": cond_price_up_vol_down,
        "cond_rsi_overbought": cond_rsi_overbought,
        "cond_rsi_divergence": cond_rsi_divergence,
        "cond_macd_turn_neg": cond_macd_turn_neg,
        "cond_macd_divergence": cond_macd_divergence,
        "cond_bb_below": cond_bb_below,
        "conditions_met": conditions_met,
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


def build_sell_summary_sheet(sell_file: Path) -> None:
    """Build a summary sheet showing stock appearance frequency across all sell sheets."""
    if not sell_file.exists():
        return

    xls = pd.ExcelFile(sell_file)
    # Get all sell sheets, exclude summary
    data_sheets = [
        s for s in xls.sheet_names
        if s not in ("summary", "market_closed")
        and s.startswith("sell_")
    ]

    if not data_sheets:
        return

    # Collect stock appearances: {symbol: {name, dates}}
    stock_data: dict[str, dict] = {}

    for sheet_name in data_sheets:
        date_part = sheet_name.split("_", 1)[1] if "_" in sheet_name else sheet_name

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
        for date in all_dates:
            col_name = date[5:] if len(date) == 10 else date
            row[col_name] = "●" if date in data["dates"] else ""
        rows.append(row)

    summary_df = pd.DataFrame(rows)

    date_cols = [d[5:] if len(d) == 10 else d for d in all_dates]
    col_order = ["symbol", "name", "count"] + date_cols
    summary_df = summary_df[[c for c in col_order if c in summary_df.columns]]

    # Write summary sheet
    existing_sheets = {s: xls.parse(s) for s in xls.sheet_names if s != "summary"}
    existing_sheets["summary"] = summary_df

    with pd.ExcelWriter(sell_file, engine="openpyxl", mode="w") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        for sheet_name, df in existing_sheets.items():
            if sheet_name != "summary":
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Sell Summary 已更新：{len(stock_data)} 檔股票，{len(all_dates)} 個日期")

# 台股每日分析工具

每天抓取指定股票的開盤/收盤價、三大法人買賣超，計算技術指標（RSI、MACD、布林通道），並進行 Alpha 選股分析。

## 功能重點

- 從 `.env` 讀取要追蹤的股票清單
- 取得每日 OHLCV（開高低收量）資料
- 取得三大法人（外資、投信、自營商）買賣超
- 取得融資融券資料（餘額、增減、券資比）
- 計算技術指標：RSI(9/14)、MACD、布林通道
- Alpha 選股：根據多種條件篩選潛力股
- 賣出警示：根據多種條件提示風險
- 復盤模式：使用歷史資料進行回測分析

## 需求

- Python 3.12+

## 安裝

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

## 設定

複製 `.env.example` 為 `.env` 並設定股票清單：

```bash
cp .env.example .env
```

編輯 `.env`：

```bash
STOCKS=2330,2317,2454,3017,8299
```

詳細設定說明請參考 `.env.example`。

## 使用方式

### 抓取當天資料

```bash
tw-stock-analysis
```

### 指定日期

```bash
tw-stock-analysis --date 2025-10-15
```

### 回補最近 N 天

```bash
tw-stock-analysis --backfill-days 90
```

### 指定回補區間

```bash
tw-stock-analysis --backfill-start 2025-08-01 --backfill-end 2025-10-15
```

### 初始化回補（僅當 Excel 不存在時）

```bash
tw-stock-analysis --init-backfill
```

### 復盤模式

使用現有資料進行 Alpha 分析，不呼叫 API：

```bash
# 單一日期
tw-stock-analysis --replay --date 2025-10-15

# 指定區間
tw-stock-analysis --replay-start 2025-10-01 --replay-end 2025-10-15
```

### 更新 Summary

僅更新 alpha_pick.xlsx 的 summary sheet，不執行分析：

```bash
tw-stock-analysis --update-summary
```

### 更新發行股數

更新 tw_stock_shares.xlsx（週轉率計算用）：

```bash
tw-stock-analysis --update-shares
```

注意：首次執行時會自動取得發行股數並儲存，之後讀取快取檔案。

### 賣出警示分析（復盤）

僅執行賣出警示分析：

```bash
# 單一日期
tw-stock-analysis --replay-sell-analysis --date 2025-01-21

# 指定區間
tw-stock-analysis --replay-sell-analysis-start 2025-01-01 --replay-sell-analysis-end 2025-01-21
```

不執行賣出警示（僅執行 alpha 分析）：

```bash
tw-stock-analysis --no-sell
```

## 輸出檔案

### tw_stock_daily.xlsx

每日交易資料，每個交易日一個工作表。

| 欄位 | 說明 |
|------|------|
| symbol, name | 股票代號、名稱 |
| open, close, high, low | 開高低收 |
| volume | 成交量（張） |
| turnover_rate | 週轉率（%） |
| vol_ma5, vol_ma10, vol_ma20 | 均量（張） |
| foreign_net, trust_net, dealer_net | 三大法人買賣超（張） |
| institutional_investors_net | 三大法人合計（張） |
| margin_buy, margin_sell | 融資買進、賣出（張） |
| margin_balance, margin_change | 融資餘額、增減（張） |
| short_sell, short_buy | 融券賣出、回補（張） |
| short_balance, short_change | 融券餘額、增減（張） |
| short_margin_ratio | 券資比（%） |
| rsi_9, rsi_14 | RSI(9)、RSI(14)（Wilder's SMMA） |
| macd, macd_signal, macd_hist | MACD 指標 |
| bb_upper, bb_middle, bb_lower | 布林通道上中下軌 |
| bb_percent_b, bb_bandwidth | %B 及 bandwidth |

### alpha_pick.xlsx

Alpha 選股分析結果。

- `alpha_YYYY-MM-DD`：每日選股結果
- `summary` sheet：股票出現頻率統計矩陣

### alpha_sell.xlsx

賣出警示分析結果。

- `sell_YYYY-MM-DD`：每日賣出警示
- `summary` sheet：股票出現頻率統計矩陣

### tw_stock_shares.xlsx

發行股數快取（週轉率計算用）。

| 欄位 | 說明 |
|------|------|
| symbol | 股票代號 |
| name | 股票名稱 |
| issued_shares | 發行股數 |
| updated_at | 更新日期 |

## Alpha 選股條件

**必要條件**：

| 條件 | 說明 |
|------|------|
| cond_insti | 法人加碼：近期淨買超 > 長期平均（必須） |
| cond_vol_ma10 / cond_vol_ma20 | 量突破 10MA 或 20MA × N 倍（二擇一） |

**選配條件**（至少 2 個成立）：

| 條件 | 說明 |
|------|------|
| cond_rsi | RSI 健康：介於設定區間內 |
| cond_macd | MACD 多方：histogram > 0 |
| cond_bb_narrow | 布林收窄：近期 BW < 長期 BW |
| cond_bb_near_upper | 接近上軌：%B > 設定值 |

詳細參數設定請參考 `.env.example`。

## Sell 賣出警示條件

警示邏輯：**必要條件（至少 1 項）** AND **選配條件（至少 N 項，預設 2）**

**必要條件（法人賣超，至少 1 項）**：

| 條件 | 說明 |
|------|------|
| cond_foreign_sell | 外資近 N 日淨賣超 < 0 |
| cond_foreign_accel | 外資賣超加速：近 N 日均 < 0 且 < 近 M 日均 |
| cond_trust_sell | 投信近 N 日淨賣超 < 0 |
| cond_trust_accel | 投信賣超加速：近 N 日均 < 0 且 < 近 M 日均 |

**選配條件（技術面，至少 N 項）**：

| 條件 | 說明 |
|------|------|
| cond_high_black | 高檔爆量長黑 |
| cond_price_up_vol_down | 價漲量縮 |
| cond_rsi_overbought | RSI > 80（超買） |
| cond_rsi_divergence | RSI 背離：股價創高但 RSI 未創高 |
| cond_macd_turn_neg | MACD 柱由正轉負 |
| cond_macd_divergence | MACD 背離：股價創高但 MACD 柱未創高 |
| cond_bb_below | 跌破布林中軌：%B < 0.5 |
| cond_macd_death_cross | MACD 高檔死叉：MACD/Signal > 0 且柱連兩日負且加速 |
| cond_margin_surge | 融資餘額爆升：今日融資餘額 > 昨日 × (1 + N%) |

## 資料來源

- 上市股票：TWSE OpenAPI、STOCK_DAY、MI_INDEX
- 上市三大法人：TWSE T86
- 上櫃股票：TPEX 每日收盤行情
- 上櫃三大法人：TPEX 三大法人買賣超
- 融資融券（當日）：TWSE MI_MARGN、TPEX margin_balance
- 融資融券（歷史回補）：MoneyDJ
- 發行股數（週轉率計算）：TWSE/TPEX 公司基本資料

## 備註

- 若當天資料尚未公告，程式會跳過寫入
- 技術指標需要足夠歷史資料，初期可能顯示空值
- 週末及休市日會自動跳過

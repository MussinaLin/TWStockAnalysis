# 台股每日分析工具

每天抓取指定股票的開盤/收盤價、三大法人買賣超，計算技術指標（RSI、MACD、布林通道），並進行 Alpha 選股分析。

## 功能重點

- 從 `.env` 讀取要追蹤的股票清單
- 取得每日 OHLCV（開高低收量）資料
- 取得三大法人（外資、投信、自營商）買賣超
- 計算技術指標：RSI(14)、MACD、布林通道
- Alpha 選股：根據多種條件篩選潛力股
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
| rsi_14 | RSI(14) |
| macd, macd_signal, macd_hist | MACD 指標 |
| bb_upper, bb_middle, bb_lower | 布林通道上中下軌 |
| bb_percent_b, bb_bandwidth | %B 及 bandwidth |

### alpha_pick.xlsx

Alpha 選股分析結果。

- 一般模式：`alpha_YYYY-MM-DD`
- 復盤模式：`replay_YYYY-MM-DD`
- `summary` sheet：股票出現頻率統計矩陣

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

## 資料來源

- 上市股票：TWSE OpenAPI、STOCK_DAY、MI_INDEX
- 上市三大法人：TWSE T86
- 上櫃股票：TPEX 每日收盤行情
- 上櫃三大法人：TPEX 三大法人買賣超
- 發行股數（週轉率計算）：TWSE/TPEX 公司基本資料

## 備註

- 若當天資料尚未公告，程式會跳過寫入
- 技術指標需要足夠歷史資料，初期可能顯示空值
- 週末及休市日會自動跳過

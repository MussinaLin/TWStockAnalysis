# 00987A 成份股每日分析

這個專案每天抓取「主動台新優勢成長（00987A）」成份股，產出當天開盤/收盤價、三大法人買賣超，以及常用技術指標（RSI、MACD），寫入單一 Excel 檔 `tw_00987A_daily.xlsx`，每個交易日一個工作表。

## 功能重點

- 自動抓取 00987A 成份股名單
- 取得當天開盤價、收盤價
- 取得三大法人（外資、投信、自營商）買賣超
- 計算 RSI(14)、MACD(12,26,9)
- 依日期建立/更新 Excel 工作表
- 支援指定日期與初始化回補歷史資料

## 需求

- Python 3.14+

## 安裝

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

## 使用

### 預設抓取當天

```bash
tw-00987a-daily
```

### 指定日期

```bash
tw-00987a-daily --date 2026-02-03
```

### 另外分析指定股票

在 `.env` 設定：

```bash
STOCKS=2330,2317
```

程式會自動合併 00987A 成份股與 `STOCKS`，重複代號會被忽略，
並嘗試從當日行情/三大法人資料補上股票名稱。

### 初始化回補歷史資料（只在 Excel 不存在時）

```bash
tw-00987a-daily --init-backfill
```

### 回補最近 N 天

```bash
tw-00987a-daily --backfill-days 90
```

### 指定回補區間

```bash
tw-00987a-daily --backfill-start 2025-10-01 --backfill-end 2026-02-03
```

## 輸出

- 檔案：`tw_00987A_daily.xlsx`
- 工作表名稱：`YYYY-MM-DD`
- 欄位（範例）：
  - `symbol`, `name`, `open`, `close`
  - `foreign_net`, `trust_net`, `dealer_net`, `institutional_investors_net`
  - `rsi_14`, `macd`, `macd_signal`, `macd_hist`

## 上櫃歷史回補設定

上櫃（TPEX）歷史行情/三大法人需要指定日期的官方 API，因此請設定環境變數模板：

```bash
# 也可以放在 .env 檔案中，程式會自動載入
export TPEX_DAILY_QUOTES_URL_TEMPLATE='...{date}...'
export TPEX_3INSTI_URL_TEMPLATE='...{date}...'
```

模板可使用 `{date}`（YYYY-MM-DD）或 `{roc}`（民國年 YYY/MM/DD）。

若未設定模板，程式仍可抓取「當日」上櫃資料，但無法回補指定日期。

## SSL 驗證設定

若遇到台新投信網站 SSL 憑證驗證失敗，程式已預設只對該網站關閉驗證。

## 備註

- 若當天資料尚未公告，程式會跳出提示並結束，不會寫入空白資料。
- 技術指標需要歷史收盤價，第一次執行可能會出現 `NaN`，之後每天累積即可補齊。

## 資料來源

- 00987A 成份股：台新投信 ETF 詳細頁面
- 上市股票日資料（開收盤）：TWSE 股票日成交資訊
- 上市三大法人：TWSE 三大法人買賣超 (T86)
- 上櫃日行情：data.gov.tw 上櫃股票行情資料集
- 上櫃三大法人：data.gov.tw 上櫃三大法人買賣超日報（股）明細

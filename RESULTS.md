# Backtest results — consolidated (as of 1 April 2026)

Summary of **full-calendar-2025, month-by-month** RebateMM simulations on **Crypto.com L2** (S3 replay). Each row is **one independent monthly backtest** (not a single continuous year-long run). **Annual figures below are sums of monthly PnL and volume** unless stated otherwise.

**Shared harness (typical):** `configs/backtest_engine_harness.yaml` — e.g. maker fee **−0.75 bps**, taker **+1.5 bps**, `touch_queue_pct` **0.4**, crossed-book survival **0.5**. Strategy: `**rebate_mm_eth.yaml`** / `**rebate_mm_btc.yaml**` with **dynamic order sizing** on for the tables below (see logs for exact YAML revisions per period).

---

## ETH_USD — 2025 by month


| Month | Final PnL ($) | Vol (~$) | Net edge (bps) | Spread cap. (bps) | Rebate (bps) | Sharpe | Max DD % | Fill % |
| ----- | ------------- | -------- | -------------- | ----------------- | ------------ | ------ | -------- | ------ |
| 01    | 53,615.59     | ~0.668B  | 0.80           | +0.05             | 0.75         | 67.51  | 0.07     | 13.83  |
| 02    | 39,374.24     | ~0.565B  | 0.70           | −0.05             | 0.75         | 67.36  | 0.06     | 17.06  |
| 03    | 20,442.12     | ~0.262B  | 0.78           | +0.03             | 0.75         | 21.14  | 0.09     | 11.34  |
| 04    | 13,157.55     | ~0.164B  | 0.80           | +0.05             | 0.75         | 42.17  | 0.04     | 7.40   |
| 05    | 16,332.23     | ~0.233B  | 0.70           | −0.05             | 0.75         | 39.30  | 0.04     | 8.68   |
| 06    | 26,631.88     | ~0.316B  | 0.84           | +0.09             | 0.75         | 48.12  | 0.03     | 8.39   |
| 07    | 28,610.47     | ~0.336B  | 0.85           | +0.10             | 0.75         | 61.58  | 0.04     | 7.29   |
| 08    | 32,295.45     | ~0.502B  | 0.64           | −0.11             | 0.75         | 37.28  | 0.07     | 8.51   |
| 09    | 22,450.15     | ~0.335B  | 0.67           | −0.08             | 0.75         | 37.73  | 0.04     | 5.81   |
| 10    | 16,623.93     | ~0.299B  | 0.56           | −0.19             | 0.75         | 26.49  | 0.08     | 6.28   |
| 11    | 7,109.77      | ~0.218B  | 0.33           | −0.42             | 0.75         | 12.48  | 0.10     | 7.67   |
| 12    | 10,227.68     | ~0.209B  | 0.49           | −0.26             | 0.75         | 19.09  | 0.07     | 6.52   |


**12-month totals (sum of months):**


|               |                                        |
| ------------- | -------------------------------------- |
| **Final PnL** | **~$286,871**                          |
| **Volume**    | **~$3.51B** (sum of monthly notionals) |


## BTC_USD — 2025 by month (dynamic quoting baseline)


| Month | Final PnL ($) | Vol (~$) | Net edge (bps) | Spread cap. (bps) | Rebate (bps) | Sharpe | Max DD % |
| ----- | ------------- | -------- | -------------- | ----------------- | ------------ | ------ | -------- |
| 01    | 200,330.61    | ~3.03B   | 0.66           | −0.09             | 0.75         | 18.62  | 1.90     |
| 02    | 84,485.57     | ~2.28B   | 0.37           | −0.38             | 0.75         | 17.01  | 1.43     |
| 03    | 2,899.64      | ~1.03B   | 0.03           | −0.72             | 0.75         | 0.09   | 3.26     |
| 04    | 49,867.48     | ~0.666B  | 0.75           | ~0.00             | 0.75         | 10.69  | 1.38     |
| 05    | 39,287.61     | ~0.611B  | 0.64           | −0.11             | 0.75         | 17.77  | 0.42     |
| 06    | 50,289.65     | ~0.548B  | 0.92           | +0.17             | 0.75         | 23.30  | 0.28     |
| 07    | 42,711.67     | ~0.469B  | 0.91           | +0.16             | 0.75         | 26.09  | 0.36     |
| 08    | 81,594.39     | ~0.810B  | 1.01           | +0.26             | 0.75         | 23.04  | 0.33     |
| 09    | 61,813.82     | ~0.592B  | 1.04           | +0.29             | 0.75         | 26.30  | 0.30     |
| 10    | 44,251.92     | ~0.850B  | 0.52           | −0.23             | 0.75         | 10.17  | 1.19     |
| 11    | 27,049.66     | ~0.838B  | 0.32           | −0.43             | 0.75         | 5.00   | 1.74     |
| 12    | 17,826.22     | ~0.750B  | 0.24           | −0.51             | 0.75         | 3.80   | 1.88     |


**12-month totals (sum of months):**


|               |                                         |
| ------------- | --------------------------------------- |
| **Final PnL** | **~$702,408**                           |
| **Volume**    | **~$12.47B** (sum of monthly notionals) |


---

## License

Same as repository (MIT).
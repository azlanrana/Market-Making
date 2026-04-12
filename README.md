# Crypto market making engine (Rust)

**Rebate-focused market making on crypto:** full **L2 order book replay**, a **queue-aware matching model**, and **PnL / microstructure metrics** so strategies are judged on realistic fill quality—not just mid-price PnL.

The core idea is **maker economics under fees and queue position**: quote, get filled like a maker would (touch queue, depletion, latency), then decompose outcomes into **rebate, spread capture, inventory drag, markouts, and adverse selection**.

---

## What this is (in one pass)

- **Strategies** — **RebateMM** is the main line of work: spread and volatility adaptation, inventory skew, book imbalance, microprice / impulse gating, phase-based sizing, and queue-aware touch logic. Additional experiments live under `queue-farmer*` and `rebate-alpha`.
- **Simulation** — Event-driven loop: **strategy → orders → latency → matcher → portfolio → metrics**, replaying historical books from **S3** or **CSV**.
- **Infrastructure** — Rust workspace (`engine`, `simulator`, `metrics`, `data-loader`, `cli`) plus optional **SFTP → S3** ingestion and a **live paper** path (public websocket feed, no live orders).

This repo is a **research and backtesting stack**. It is not a drop-in production trading system, and **live parameters / deployment specifics are intentionally not the focus here**.

---

## Highlights

| Area | What you get |
|------|----------------|
| **Execution realism** | Configurable **queue model** (touch fraction, cancel-ahead, crossed-book survival, optional churn) and **latency** on submit/cancel. |
| **Rebate MM** | YAML-driven profiles + **parameter sweeps** (grid or named runs) for systematic optimisation. |
| **Fill quality** | **1s / 5s markouts**, adverse selection, toxic vs neutral fill buckets, maker vs crossed reasons, quote lifetime. |
| **PnL attribution** | **Rebate vs spread capture vs inventory drag** (and dashboard stats aligned with that decomposition). |
| **Scale** | Multi-month **gzipped L2** replay from object storage; filters by prefix and calendar range. |

---

## Example results (simulation, historical L2)

On long **BTC USDT** RebateMM-style backtests with the bundled fee / queue harness, exported metrics have looked like **~0.5 bps net edge per fill** with **rebate carrying the economics** while short-horizon markouts stay near flat—i.e. the edge is **rebate and microstructure**, not directional alpha.

**Full-year 2025 month-by-month tables (ETH_USD & BTC_USD):** [RESULTS.md](RESULTS.md) *(as of 1 April 2026; sums of independent monthly runs).*

Illustrative row from `*_metrics.csv` (one run; not a guarantee for other pairs or regimes):

```text
net_edge_bps,rebate_earned_bps,markout_1s_bps,adverse_selection_1s_bps,final_pnl_usd
0.50,0.75,-0.004,0.013,347390.59
```

**Maker share** in the same setup is **dominated by maker-style fills** (the run above: maker-ratio style stats in the printed dashboard are ~**100%** maker-class; see `backtest_s3_rebate_mm` output). Always interpret under **your** fee table and queue assumptions.

---

## System design (short)

- **Modular workspace** — `mm-core-types` + `mm-core` (strategy boundary), **`mm-simulator`** (matching), **`mm-engine`** (backtest orchestration), **`mm-metrics`**, **`mm-portfolio`**, **`data-loader`**, strategy crates, **`mm-cli`**.
- **Pipeline** — L2 snapshots in → strategy intents → delayed working orders → matcher fills → portfolio + **round-trip / markout trackers** → summary stats and optional CSV export.

**Full crate layout, CLI, env vars, and runbooks:** [DETAILED_README.md](DETAILED_README.md).

---

## Quick start

```bash
cd mm
cargo check --workspace
```

S3 backtest (needs AWS credentials + bucket; see detailed doc):

```bash
REBATE_MM_PROFILE=eth S3_BUCKET=... S3_PREFIX=2025/ TRADING_PAIR=ETH_USD \
  cargo test -p mm-engine backtest_s3_rebate_mm --release -- --ignored --nocapture
```

More: [QUICK_START.md](QUICK_START.md) · [PARAMETER_SWEEP.md](PARAMETER_SWEEP.md) · [DATA_GUIDE.md](DATA_GUIDE.md)

---

## Disclaimer

Educational / research use. **Not financial advice.** Past simulation performance does not predict live results. Production systems need separate risk, compliance, and execution guarantees.

---

## License

MIT

# Parameter Sweep — systematic config testing

Test many variable combinations without running each backtest by hand.

---

## Quick start

**Important:** Run from the `mm` directory so the config path resolves:

```bash
cd mm
SWEEP_CONFIG=configs/sweep_rebate_mm.yaml \
S3_BUCKET=ethusdt2025 S3_PREFIX=ETH_USDT/ AWS_REGION=us-east-1 \
MAX_FILES=10607 \
cargo test -p mm-engine backtest_s3_sweep --release -- --ignored --nocapture
```

Results → `sweep_results.csv` (or `SWEEP_OUTPUT_CSV`).

---

## Config format (`configs/sweep_rebate_mm.yaml`)

### Base + grid (cartesian product)

```yaml
base:
  order_amount: 0.5
  base_spread: 4.0
  impulse_phase1_sizing: false
  # ... all defaults

grid:
  impulse_phase1_sizing: [false, true]
  base_spread: [3.5, 4.0, 4.5]
```

→ 2 × 3 = **6 runs**. Each run = base + one combo from the grid.

### Base + explicit experiments

```yaml
base:
  # ...

experiments:
  - name: baseline
    impulse_phase1_sizing: false
  - name: phase1_on
    impulse_phase1_sizing: true
  - name: phase1_loose_spread_4
    impulse_phase1_sizing: true
    base_spread: 4.0
```

→ 3 runs with custom names.

---

## Supported params ( RebateMM )

| Param | Type | Example |
|-------|------|---------|
| `base_spread` | f64 | 4.0 |
| `impulse_phase1_sizing` | bool | true |
| `vol_lookback` | int | 50 |
| `vol_threshold_bps` | f64 | 2.0 |
| `max_spread_multiplier` | f64 | 4.0 |
| `inventory_skew_k` | f64 | 0.25 |
| `book_imbalance_threshold` | f64 | 0.80 |
| `microprice_impulse_lookback` | int | 4 |
| `impulse_kill_threshold_bps` | f64 | 2.0 |
| `order_refresh_sec` | f64 | 0.75 |
| `warmup_sec` | f64 | 30.0 |
| `impulse_phase1_calm` | f64 | 1.3 | |imp|&lt;0.30 bps |
| `impulse_phase1_mid` | f64 | 0.7 | 0.60≤|imp|&lt;1.00 |
| `impulse_phase1_min` | f64 | 0.4 | |imp|≥1.00 |
| ... | | | |

Add more in `build_strategy()` in `backtest_s3_sweep.rs` as needed.

---

## Output CSV columns

`run_id`, `total_pnl`, `realized_pnl`, `net_edge_bps`, `spread_capture_bps`, `rebate_bps`,  
`fill_rate_pct`, `round_trips`, `volume`, `sharpe`, `max_dd_pct`,  
`phase1_calm`, `phase1_mid`, `phase1_min`, `elapsed_sec`

**Gate diagnostics** (printed per run): `avg_mult`, `p25`/`p50`/`p75` multiplier, `pct_below_1` — explains "we are below 0.9× 70% of the time".

**Evaluation (Phase 1 bucket sweep):**

1. **Filter** — Reject if `net_edge_bps` < baseline (1.40) or `volume` < 95% of baseline.
2. **Shortlist** — Keep configs where edge ≈ baseline or better, volume ≈ baseline.
3. **Winner** — Pick max `total_pnl` from shortlist.

**Baseline reference (Phase 1 OFF):** $1,521 PnL, +1.40 bps edge, 2.16% fill rate, $10.86M volume.

Sort by `net_edge_bps` or `total_pnl` to find best configs.

---

## Env vars

| Var | Default | Purpose |
|-----|---------|---------|
| `SWEEP_CONFIG` | `configs/sweep_rebate_mm.yaml` | Sweep YAML path |
| `SWEEP_OUTPUT_CSV` | `sweep_results.csv` | Output file |
| `S3_BUCKET` | (required) | Data bucket |
| `S3_PREFIX` | `{pair}/` | Data prefix |
| `MAX_FILES` | 10607 | Max files per run |
| `MAX_CONCURRENT_DOWNLOADS` | 32 | S3 parallelism |

---

## Caveats

- Each run uses a new S3 loader → full data reload per experiment. 6 runs ≈ 6× single-run time.
- Caching / preloading would speed this up (future work).

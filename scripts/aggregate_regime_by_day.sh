#!/bin/bash
# Aggregate [REGIME_SAMPLE] lines from backtest output to compare Dec 10/13 vs losing days.
# Usage: cargo test --test backtest_s3_rebate_alpha --release -- --ignored --nocapture 2>&1 | ./scripts/aggregate_regime_by_day.sh
# Or: ./scripts/aggregate_regime_by_day.sh < backtest.log

input="${1:--}"  # stdin if no file given; "-" = read from stdin

grep '\[REGIME_SAMPLE\]' "$input" | awk '
{
  # Parse: [REGIME_SAMPLE] day=2025-12-10 regime=Ranging vol_bps=18.50 trend_bps=12.1 inv_pct=39.5%
  day = ""
  regime = ""
  vol = 0
  trend = 0
  for (i=2; i<=NF; i++) {
    if ($i ~ /^day=/) { split($i,a,"="); day=a[2] }
    if ($i ~ /^regime=/) { split($i,a,"="); regime=a[2] }
    if ($i ~ /^vol_bps=/) { split($i,a,"="); vol=a[2]+0 }
    if ($i ~ /^trend_bps=/) { split($i,a,"="); trend=a[2]+0 }
  }
  if (day != "") {
    count[day]++
    vol_sum[day] += vol
    trend_sum[day] += trend
    regime_count[day,regime]++
  }
}
END {
  printf "%-12s %6s %10s %12s %12s %12s\n", "day", "samples", "avg_vol_bps", "avg_trend_bps", "Ranging%", "Trending%"
  printf "%s\n", "----------------------------------------------------------------"
  for (d in count) {
    c = count[d]
    v = (c>0) ? vol_sum[d]/c : 0
    t = (c>0) ? trend_sum[d]/c : 0
    rng = regime_count[d,"Ranging"]+0
    hvr = regime_count[d,"HighVolRange"]+0
    trd = regime_count[d,"Trending"]+0
    rng_pct = (c>0) ? (rng/c)*100 : 0
    trd_pct = (c>0) ? (trd/c)*100 : 0
    printf "%-12s %6d %10.2f %12.1f %11.1f%% %11.1f%%\n", d, c, v, t, rng_pct, trd_pct
  }
}' > /tmp/regime_agg.$$ && (head -2 /tmp/regime_agg.$$; tail -n +3 /tmp/regime_agg.$$ | sort -k1); rm -f /tmp/regime_agg.$$

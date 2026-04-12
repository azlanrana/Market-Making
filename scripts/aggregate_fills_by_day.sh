#!/bin/bash
# Aggregate [L1_FILL] lines: fill count, avg vol/trend at entry by day.
# Usage: ./scripts/aggregate_fills_by_day.sh < backtest.log

input="${1:--}"

grep '\[L1_FILL\]' "$input" | awk '
{
  # Parse: [L1_FILL] day=2025-12-10 ts=... side=Buy regime=Ranging trend_bps=12.1 vol_bps=0.85 inv_pct=39.5%
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
  printf "%-12s %8s %10s %12s %12s %12s\n", "day", "fills", "avg_vol_bps", "avg_trend_bps", "Ranging%", "Trending%"
  printf "%s\n", "------------------------------------------------------------------------"
  for (d in count) {
    c = count[d]
    v = (c>0) ? vol_sum[d]/c : 0
    t = (c>0) ? trend_sum[d]/c : 0
    rng = regime_count[d,"Ranging"]+0
    hvr = regime_count[d,"HighVolRange"]+0
    trd = regime_count[d,"Trending"]+0
    rng_pct = (c>0) ? (rng/c)*100 : 0
    trd_pct = (c>0) ? (trd/c)*100 : 0
    printf "%-12s %8d %10.2f %12.1f %11.1f%% %11.1f%%\n", d, c, v, t, rng_pct, trd_pct
  }
}' > /tmp/fills_agg.$$ 2>/dev/null && (head -2 /tmp/fills_agg.$$; tail -n +3 /tmp/fills_agg.$$ | sort -k1); rm -f /tmp/fills_agg.$$

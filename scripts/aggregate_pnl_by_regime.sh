#!/bin/bash
# Aggregate [SCRATCH] by entry_regime: count, avg gross, avg net, total gross, total net.
# Usage: ./scripts/aggregate_pnl_by_regime.sh < backtest.log

input="${1:--}"

grep '\[SCRATCH\]' "$input" | awk '
{
  gross = 0
  net = 0
  regime = ""
  for (i=2; i<=NF; i++) {
    if ($i ~ /^gross=/) { split($i,a,"="); gross=a[2]+0 }
    if ($i ~ /^net=/) { split($i,a,"="); net=a[2]+0 }
    if ($i ~ /^entry_regime=/) { split($i,a,"="); regime=a[2] }
  }
  if (regime != "") {
    count[regime]++
    gross_sum[regime] += gross
    net_sum[regime] += net
  }
}
END {
  printf "%-15s %8s %12s %12s %12s %12s\n", "entry_regime", "count", "avg_gross", "avg_net", "tot_gross", "tot_net"
  printf "%s\n", "--------------------------------------------------------------------------------"
  for (r in count) {
    c = count[r]
    ag = (c>0) ? gross_sum[r]/c : 0
    an = (c>0) ? net_sum[r]/c : 0
    printf "%-15s %8d %12.2f %12.2f %12.2f %12.2f\n", r, c, ag, an, gross_sum[r], net_sum[r]
  }
}' | sort -k2 -rn

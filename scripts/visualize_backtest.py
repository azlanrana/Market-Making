#!/usr/bin/env python3
"""
Visualize backtest results: single HTML with equity curve, drawdown, daily P&L, and fills table.

Export CSV from backtest (also exports fills when BACKTEST_OUTPUT_CSV is set):
  BACKTEST_OUTPUT_CSV=backtest_results.csv MAX_FILES=60000 S3_BUCKET=l2ethusd \\
  S3_PREFIX=2025/ TRADING_PAIR=ETH_USD cargo test -p mm-engine backtest_s3_rebate_mm \\
  --release -- --ignored --nocapture

This creates backtest_results.csv and backtest_results_fills.csv (same directory).

Then run:
  python scripts/visualize_backtest.py backtest_results.csv --asset ETH_USDT
  python scripts/visualize_backtest.py backtest_results.csv --asset ETH_USDT --pdf   # static PDF (needs kaleido)

Default report name: <ASSET>_dd-mm-yyyy_to_dd-mm-yyyy.html in --output-dir (override with --output-html). --pdf with no filename uses the same stem as the HTML report.

To merge month-by-month equity CSVs into one file (chained curve) without re-running the engine, see scripts/combine_monthly_backtests.py.

When the Rust test runs with BACKTEST_OUTPUT_CSV, it also writes <stem>_metrics.csv; the HTML includes a metrics summary table (bottom) plus a bar to download metrics / equity / fills as CSV. Very large fills are not embedded (use *_fills.csv on disk; copied next to the HTML if --output-dir differs).
Use --individual to also save separate chart files.

Dependencies: pip install pandas plotly
Optional PDF: pip install kaleido  (static snapshot only — PDF cannot keep Plotly zoom/hover; use HTML for that, or Print → Save as PDF from the browser)
"""

import argparse
import io
import json
import os
import re
import shutil
import sys
from pathlib import Path

try:
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    print("Error: Install dependencies: pip install pandas plotly")
    sys.exit(1)


def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    if "timestamp" not in df.columns or "portfolio_value" not in df.columns:
        raise ValueError("CSV must have 'timestamp' and 'portfolio_value' columns")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["rolling_max"] = df["portfolio_value"].cummax()
    # Drawdown depth: positive % below peak (0 at highs, e.g. 0.01 = 0.01% below peak)
    df["drawdown"] = (
        (df["rolling_max"] - df["portfolio_value"]) / df["rolling_max"] * 100
    )
    df["date"] = df["timestamp"].dt.date
    return df


def plot_equity_curve(df: pd.DataFrame) -> go.Figure:
    fig = px.line(
        df,
        x="timestamp",
        y="portfolio_value",
        title="Equity Curve",
    )
    fig.update_traces(hovertemplate="%{x}<br>$%{y:,.2f}<extra></extra>")
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Portfolio Value ($)",
        hovermode="x unified",
        template="plotly_white",
        yaxis=dict(
            tickprefix="$",
            tickformat=",.2f",
            exponentformat="none",
            showexponent="none",
            separatethousands=True,
        ),
    )
    return fig


def plot_drawdown(df: pd.DataFrame) -> go.Figure:
    fig = px.area(
        df,
        x="timestamp",
        y="drawdown",
        title="Drawdown (%)",
    )
    fig.update_traces(
        fill="tozeroy",
        line_color="crimson",
        hovertemplate="%{x}<br>%{y:.8f}%<extra></extra>",
    )
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Drawdown %",
        hovermode="x unified",
        template="plotly_white",
        yaxis=dict(
            tickformat=".8f",
            exponentformat="none",
            showexponent="none",
        ),
    )
    return fig


def plot_daily_pnl(df: pd.DataFrame) -> go.Figure:
    daily = (
        df.groupby("date")["portfolio_value"]
        .agg(["first", "last"])
        .assign(pnl=lambda x: x["last"] - x["first"])
        .reset_index()
    )
    colors = ["green" if p >= 0 else "red" for p in daily["pnl"]]
    fig = go.Figure(
        data=[go.Bar(x=daily["date"], y=daily["pnl"], marker_color=colors)]
    )
    fig.update_traces(hovertemplate="%{x}<br>$%{y:,.2f}<extra></extra>")
    fig.update_layout(
        title="Daily P&L",
        xaxis_title="Date",
        yaxis_title="P&L ($)",
        template="plotly_white",
        showlegend=False,
        yaxis=dict(
            tickprefix="$",
            tickformat=",.2f",
            exponentformat="none",
            showexponent="none",
            separatethousands=True,
        ),
    )
    return fig


def load_fills(csv_path: Path) -> pd.DataFrame | None:
    """Load fills CSV if it exists (derived from portfolio CSV path)."""
    fills_path = csv_path.parent / f"{csv_path.stem}_fills{csv_path.suffix}"
    if not fills_path.exists():
        return None
    try:
        df = pd.read_csv(fills_path)
        df.columns = df.columns.str.strip()
        required = {"timestamp", "side", "price", "amount"}
        if not required.issubset(set(df.columns)):
            return None
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    except Exception:
        return None


def format_date_range(df: pd.DataFrame) -> str:
    """Format the backtest date range for display."""
    t_min = df["timestamp"].min()
    t_max = df["timestamp"].max()
    return f"{t_min.strftime('%Y-%m-%d %H:%M')} — {t_max.strftime('%Y-%m-%d %H:%M')} UTC"


def resolve_asset_label(cli_asset: str | None) -> str:
    """Pair / asset class label for the chart title."""
    if cli_asset is not None and str(cli_asset).strip():
        return str(cli_asset).strip()
    for key in ("BACKTEST_ASSET", "TRADING_PAIR"):
        v = os.environ.get(key, "").strip()
        if v:
            return v
    return "—"


def sanitize_filename_component(label: str) -> str:
    """Safe single path segment from asset / pair (slashes etc. → underscore)."""
    s = str(label).strip()
    if not s or s == "—":
        return "unknown"
    out: list[str] = []
    for c in s:
        if c.isalnum() or c in "_-.":
            out.append(c)
        else:
            out.append("_")
    collapsed = re.sub(r"_+", "_", "".join(out)).strip("._-")
    return collapsed or "unknown"


def report_stem_from_df(df: pd.DataFrame, asset: str) -> str:
    """Filename stem: ASSET_dd-mm-yyyy_to_dd-mm-yyyy (calendar dates in UTC)."""
    t_min = df["timestamp"].min()
    t_max = df["timestamp"].max()
    safe = sanitize_filename_component(asset)
    d0 = t_min.strftime("%d-%m-%Y")
    d1 = t_max.strftime("%d-%m-%Y")
    return f"{safe}_{d0}_to_{d1}"


def format_dashboard_title(
    df: pd.DataFrame,
    asset: str,
    *,
    engine_final_pnl: float | None = None,
) -> str:
    """Title line: date range, asset, final P&L (prefers engine total_pnl if provided)."""
    dr = format_date_range(df)
    if engine_final_pnl is not None:
        pnl_s = f"${engine_final_pnl:+,.2f}"
    else:
        pv1 = float(df["portfolio_value"].iloc[-1])
        pv0 = float(df["portfolio_value"].iloc[0])
        pnl_s = f"${(pv1 - pv0):+,.2f}"
    return f"{dr} · {asset} · Final P&L {pnl_s}"


def load_metrics_csv(portfolio_csv: Path) -> dict[str, float | None] | None:
    """Load optional one-row <stem>_metrics.csv from the Rust backtest export."""
    mpath = portfolio_csv.parent / f"{portfolio_csv.stem}_metrics.csv"
    if not mpath.exists():
        return None
    row = pd.read_csv(mpath).iloc[0]
    out: dict[str, float | None] = {}
    for k in row.index:
        key = str(k).strip()
        v = row[k]
        if pd.isna(v) or (isinstance(v, str) and not str(v).strip()):
            out[key] = None
        else:
            try:
                out[key] = float(v)
            except (TypeError, ValueError):
                out[key] = None
    return out


# Same column order as `backtest_s3_rebate_mm` metrics export (subset used for curve-only fallback)
METRICS_CSV_COLUMNS = [
    "win_rate_pct",
    "sharpe",
    "max_drawdown_pct",
    "fill_rate_pct",
    "net_edge_bps",
    "spread_capture_usd",
    "spread_capture_bps",
    "rebate_earned_usd",
    "rebate_earned_bps",
    "turnover_daily",
    "volume_usd",
    "final_pnl_usd",
]


def build_engine_metrics_csv(metrics: dict[str, float | None] | None) -> str:
    """One-row CSV matching the Rust export when *_metrics.csv is missing."""
    buf = io.StringIO()
    buf.write(",".join(METRICS_CSV_COLUMNS) + "\n")

    def cell(k: str) -> str:
        if not metrics:
            return ""
        v = metrics.get(k)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(v)

    buf.write(",".join(cell(k) for k in METRICS_CSV_COLUMNS) + "\n")
    return buf.getvalue()


def prepare_download_payload(
    portfolio_csv: Path,
    metrics: dict[str, float | None] | None,
    max_embed_bytes: int,
) -> dict:
    """Build payload for in-browser CSV downloads (reads disk when present)."""
    stem = portfolio_csv.stem
    metrics_path = portfolio_csv.parent / f"{stem}_metrics.csv"
    if metrics_path.exists():
        metrics_csv = metrics_path.read_text(encoding="utf-8")
    else:
        metrics_csv = build_engine_metrics_csv(metrics)

    equity_csv = portfolio_csv.read_text(encoding="utf-8")

    fills_path = portfolio_csv.parent / f"{stem}_fills.csv"
    fills_csv: str | None = None
    fills_bar_note = ""
    if fills_path.exists():
        raw = fills_path.read_text(encoding="utf-8")
        if len(raw.encode("utf-8")) <= max_embed_bytes:
            fills_csv = raw
        else:
            mb = max(1, len(raw.encode("utf-8")) // 1_000_000)
            fills_bar_note = (
                f"Fills CSV is large (~{mb}MB) and not embedded. "
                f"Use <code>{fills_path.name}</code> beside the portfolio CSV "
                "(or the copy next to this HTML if the tool copied it for you)."
            )
    else:
        fills_bar_note = "No fills CSV beside this portfolio file."

    return {
        "filenames": {
            "metrics": f"{stem}_metrics.csv",
            "equity": portfolio_csv.name,
            "fills": f"{stem}_fills.csv",
        },
        "metrics_csv": metrics_csv,
        "equity_csv": equity_csv,
        "fills_csv": fills_csv,
        "fills_bar_note": fills_bar_note,
    }


def inject_csv_download_ui(html_path: Path, payload: dict) -> None:
    """Inject a download bar + JSON payload into Plotly HTML (safe for </script> in data)."""
    names = payload["filenames"]
    pkg = {
        "metricsName": names["metrics"],
        "equityName": names["equity"],
        "fillsName": names["fills"],
        "metricsCsv": payload["metrics_csv"],
        "equityCsv": payload["equity_csv"],
        "fillsCsv": payload["fills_csv"],
        "fillsNote": payload["fills_bar_note"],
    }
    raw_json = json.dumps(pkg, ensure_ascii=False)
    safe_json = raw_json.replace("<", "\\u003c")

    bar_html = f"""
<div id="bt-export-bar" style="font-family:system-ui,-apple-system,sans-serif;font-size:13px;padding:10px 16px;background:#eef2f7;border-bottom:1px solid #c5cdd8;display:flex;flex-wrap:wrap;align-items:center;gap:10px;">
  <span style="font-weight:600;color:#1a1a2e;">Export CSV</span>
  <button type="button" id="bt-dl-metrics" style="padding:6px 12px;border:1px solid #8892a6;border-radius:6px;background:#fff;cursor:pointer;">Metrics</button>
  <button type="button" id="bt-dl-equity" style="padding:6px 12px;border:1px solid #8892a6;border-radius:6px;background:#fff;cursor:pointer;">Equity curve</button>
  <button type="button" id="bt-dl-fills" style="padding:6px 12px;border:1px solid #8892a6;border-radius:6px;background:#fff;cursor:pointer;">Fills</button>
  <span id="bt-fills-hint" style="color:#555;max-width:48rem;"></span>
</div>
<script type="application/json" id="bt-export-payload">{safe_json}</script>
<script>
(function () {{
  var el = document.getElementById("bt-export-payload");
  if (!el) return;
  var P = JSON.parse(el.textContent);
  var hint = document.getElementById("bt-fills-hint");
  if (hint && P.fillsNote) hint.innerHTML = P.fillsNote;
  function download(filename, text) {{
    if (!text) return;
    var blob = new Blob([text], {{ type: "text/csv;charset=utf-8" }});
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }}
  var bm = document.getElementById("bt-dl-metrics");
  if (bm) bm.onclick = function () {{ download(P.metricsName, P.metricsCsv); }};
  var be = document.getElementById("bt-dl-equity");
  if (be) be.onclick = function () {{ download(P.equityName, P.equityCsv); }};
  var bf = document.getElementById("bt-dl-fills");
  if (bf) {{
    if (!P.fillsCsv) {{
      bf.disabled = true;
      bf.style.opacity = "0.45";
      bf.style.cursor = "not-allowed";
    }} else {{
      bf.onclick = function () {{ download(P.fillsName, P.fillsCsv); }};
    }}
  }}
}})();
</script>
"""

    text = html_path.read_text(encoding="utf-8")

    # Callable repl: bar_html contains "\\u003c" from JSON; a string repl would be parsed as re template (bad \\u).
    def _after_open_body(m: re.Match[str]) -> str:
        return m.group(0) + bar_html

    new_text, n = re.subn(
        r"<body([^>]*)>", _after_open_body, text, count=1, flags=re.IGNORECASE
    )
    if n != 1:
        print("Warning: could not inject CSV download bar (no <body> tag found).", file=sys.stderr)
        return
    html_path.write_text(new_text, encoding="utf-8")


def build_metrics_table(
    raw: dict[str, float | None] | None,
    df: pd.DataFrame,
) -> go.Table:
    """Two-column summary: metric name and value (curve fallbacks if no *_metrics.csv)."""

    def g(key: str) -> float | None:
        return raw.get(key) if raw else None

    pv0 = float(df["portfolio_value"].iloc[0])
    pv1 = float(df["portfolio_value"].iloc[-1])
    curve_pnl = pv1 - pv0
    max_dd_curve = float(df["drawdown"].max())

    wr = g("win_rate_pct")
    sharpe = g("sharpe")
    mdd = g("max_drawdown_pct")
    if mdd is None:
        mdd = max_dd_curve

    fr = g("fill_rate_pct")
    ne = g("net_edge_bps")
    scu = g("spread_capture_usd")
    scb = g("spread_capture_bps")
    ru = g("rebate_earned_usd")
    rb = g("rebate_earned_bps")
    td = g("turnover_daily")
    vol = g("volume_usd")
    final = g("final_pnl_usd")
    if final is None:
        final = curve_pnl

    def money(x: float | None, plus: bool = False) -> str:
        if x is None:
            return "—"
        if plus:
            return f"${x:+,.2f}"
        return f"${x:,.2f}"

    labels = [
        "Win rate",
        "Sharpe",
        "Max drawdown (peak to trough, %)",
        "Fill ratio / fill rate",
        "Net edge / trade",
        "Spread capture ($)",
        "Spread capture (bps)",
        "Rebate earned ($)",
        "Rebate earned (bps)",
        "Turnover (× daily)",
        "Volume (notional USD)",
        "Final P&L",
    ]
    values = [
        f"{wr:.1f}%" if wr is not None else "—",
        f"{sharpe:.2f}" if sharpe is not None else "—",
        f"{mdd:.4f}%" if mdd is not None else "—",
        f"{fr:.2f}%" if fr is not None else "—",
        f"{ne:+.2f} bps" if ne is not None else "—",
        money(scu, plus=True) if scu is not None else "—",
        f"{scb:+.4f} bps" if scb is not None else "—",
        money(ru, plus=True) if ru is not None else "—",
        f"{rb:+.4f} bps" if rb is not None else "—",
        f"{td:.2f}×" if td is not None else "—",
        money(vol, plus=False) if vol is not None else "—",
        money(final, plus=True),
    ]
    if raw:
        labels[-1] = "Final P&L (engine total_pnl)"
    else:
        labels[-1] = "Final P&L (portfolio curve, no *_metrics.csv)"

    return go.Table(
        columnwidth=[220, 320],
        header=dict(
            values=["Metric", "Value"],
            fill_color="rgb(40,55,90)",
            font=dict(color="white", size=12),
            align="left",
            height=30,
        ),
        cells=dict(
            values=[labels, values],
            fill_color=[["white", "rgb(245,248,252)"] * (len(labels) // 2 + 1)],
            align="left",
            font=dict(size=11, color="rgb(17, 24, 39)", family="system-ui, sans-serif"),
            height=24,
        ),
    )


def plot_combined_dashboard(
    df: pd.DataFrame,
    fills_df: pd.DataFrame | None = None,
    *,
    asset: str = "—",
    metrics: dict[str, float | None] | None = None,
) -> go.Figure:
    """Equity, drawdown, daily P&L, optional fills, then metrics table at the bottom."""
    daily = (
        df.groupby("date")["portfolio_value"]
        .agg(["first", "last"])
        .assign(pnl=lambda x: x["last"] - x["first"])
        .reset_index()
    )
    colors = ["green" if p >= 0 else "red" for p in daily["pnl"]]

    engine_pnl = metrics.get("final_pnl_usd") if metrics else None
    title_text = format_dashboard_title(df, asset, engine_final_pnl=engine_pnl)

    has_fills = fills_df is not None and len(fills_df) > 0
    # Rows 1–3: equity, drawdown, daily; row 4: fills (optional); last row: metrics table
    if has_fills:
        n_rows = 5
        subplot_titles = (
            "Equity Curve",
            "Drawdown (%)",
            "Daily P&L",
            "All Fills",
            "Metrics",
        )
        row_heights = [0.21, 0.14, 0.13, 0.22, 0.30]
        specs = [
            [{"type": "xy"}],
            [{"type": "xy"}],
            [{"type": "xy"}],
            [{"type": "table"}],
            [{"type": "table"}],
        ]
    else:
        n_rows = 4
        subplot_titles = ("Equity Curve", "Drawdown (%)", "Daily P&L", "Metrics")
        row_heights = [0.26, 0.20, 0.19, 0.35]
        specs = [
            [{"type": "xy"}],
            [{"type": "xy"}],
            [{"type": "xy"}],
            [{"type": "table"}],
        ]

    # Extra gap when a table row follows a chart — avoids “All Fills” overlapping the Time header
    vspace = 0.058 if has_fills else 0.048
    fig = make_subplots(
        rows=n_rows,
        cols=1,
        subplot_titles=subplot_titles,
        vertical_spacing=vspace,
        row_heights=row_heights,
        specs=specs,
    )

    equity_row, dd_row, daily_row = 1, 2, 3

    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["portfolio_value"],
            mode="lines",
            name="Portfolio",
            line=dict(color="steelblue", width=1.5),
            hovertemplate="<b>Portfolio</b><br>%{x}<br>$%{y:,.2f}<extra></extra>",
        ),
        row=equity_row,
        col=1,
    )
    fig.update_yaxes(
        title_text="Portfolio Value ($)",
        tickprefix="$",
        tickformat=",.2f",
        exponentformat="none",
        showexponent="none",
        separatethousands=True,
        hoverformat=",.2f",
        row=equity_row,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["drawdown"],
            mode="lines",
            fill="tozeroy",
            name="Drawdown",
            line=dict(color="crimson", width=1),
            hovertemplate="<b>Drawdown</b><br>%{x}<br>%{y:.8f}%<extra></extra>",
        ),
        row=dd_row,
        col=1,
    )
    fig.update_yaxes(
        title_text="Drawdown %",
        tickformat=".8f",
        exponentformat="none",
        showexponent="none",
        hoverformat=".8f",
        row=dd_row,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=daily["date"],
            y=daily["pnl"],
            marker_color=colors,
            name="Daily PnL",
            hovertemplate="%{x}<br>P&amp;L: $%{y:,.2f}<extra></extra>",
        ),
        row=daily_row,
        col=1,
    )
    fig.update_yaxes(
        title_text="P&L ($)",
        tickprefix="$",
        tickformat=",.2f",
        exponentformat="none",
        showexponent="none",
        separatethousands=True,
        hoverformat=",.2f",
        row=daily_row,
        col=1,
    )

    metrics_row = 5 if has_fills else 4
    if has_fills:
        max_table_rows = 3000
        display_fills = fills_df.tail(max_table_rows) if len(fills_df) > max_table_rows else fills_df
        truncated = len(fills_df) > max_table_rows

        fill_reason = (
            display_fills["fill_reason"].fillna("").astype(str)
            if "fill_reason" in display_fills.columns
            else [""] * len(display_fills)
        )
        fig.add_trace(
            go.Table(
                header=dict(
                    values=[
                        "Time",
                        "Side",
                        "Price",
                        "Amount",
                        "Value ($)",
                        "Reason",
                    ],
                    fill_color="rgb(30, 64, 120)",
                    font=dict(color="white", size=12, family="system-ui, sans-serif"),
                    align="left",
                    height=32,
                ),
                cells=dict(
                    values=[
                        display_fills["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S"),
                        display_fills["side"],
                        display_fills["price"].round(4).astype(str),
                        display_fills["amount"].round(6).astype(str),
                        (
                            display_fills["value_usd"].round(2).astype(str)
                            if "value_usd" in display_fills.columns
                            else (display_fills["price"].astype(float) * display_fills["amount"].astype(float))
                            .round(2)
                            .astype(str)
                        ),
                        fill_reason,
                    ],
                    fill_color=[["white", "rgb(250,250,252)"] * (len(display_fills) // 2 + 1)],
                    align="left",
                    font=dict(size=11, color="rgb(17, 24, 39)", family="system-ui, sans-serif"),
                    height=22,
                ),
            ),
            row=4,
            col=1,
        )
        if truncated:
            fig.add_annotation(
                text=f"Showing last {max_table_rows} of {len(fills_df)} fills",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.02,
                xanchor="center",
                showarrow=False,
                font=dict(size=11, color="rgb(17, 24, 39)"),
            )

    fig.add_trace(build_metrics_table(metrics, df), row=metrics_row, col=1)

    fig.update_layout(
        title_text=title_text,
        template="plotly_white",
        hovermode="x unified",
        height=1780 if has_fills else 1420,
        showlegend=False,
    )
    # Subplot titles: dark text + nudge up so they don’t sit on table headers (Time, etc.)
    if fig.layout.annotations:

        def _style_subplot_title_ann(ann: go.layout.Annotation) -> None:
            if ann.xref == "paper" and ann.yref == "paper":
                return  # e.g. “Showing last N fills” footnote
            ann.update(
                font=dict(color="rgb(17, 24, 39)", size=14),
                yshift=18,
            )

        fig.for_each_annotation(_style_subplot_title_ann)

    fig.update_xaxes(title_text="Time", row=daily_row, col=1)
    return fig


def write_figure_pdf(fig: go.Figure, pdf_path: Path) -> bool:
    """
    Export figure to PDF via Kaleido. This is a static snapshot: no zoom/hover like the HTML report.
    Returns True on success.
    """
    try:
        h = fig.layout.height
        w = fig.layout.width
        h = int(h) if h is not None else 1680
        w = int(w) if w is not None else 1400
    except (TypeError, ValueError):
        w, h = 1400, 1680
    try:
        fig.write_image(str(pdf_path), format="pdf", width=w, height=h, scale=1)
    except Exception as e:
        print(f"Warning: PDF export failed: {e}", file=sys.stderr)
        print("  Install: pip install kaleido", file=sys.stderr)
        print(
            "  Or open the HTML report in a browser → Print → Save as PDF (browser snapshot).",
            file=sys.stderr,
        )
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Visualize backtest CSV results")
    parser.add_argument(
        "csv_path",
        help="Path to backtest_results.csv (from BACKTEST_OUTPUT_CSV)",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory for HTML output (default: current dir)",
    )
    parser.add_argument(
        "--output-html",
        default=None,
        metavar="PATH",
        help="HTML report path (default: <ASSET>_dd-mm-yyyy_to_dd-mm-yyyy.html in --output-dir)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Only save HTML file, don't open in browser",
    )
    parser.add_argument(
        "--individual",
        action="store_true",
        help="Also save individual chart files (equity, drawdown, daily PnL)",
    )
    parser.add_argument(
        "--asset",
        default=None,
        metavar="LABEL",
        help="Asset / pair in title (else BACKTEST_ASSET, TRADING_PAIR env, or —)",
    )
    parser.add_argument(
        "--max-embed-mb",
        type=float,
        default=12.0,
        metavar="MB",
        help="Max fills CSV size to embed for browser download (default: 12). Larger: use file on disk.",
    )
    parser.add_argument(
        "--pdf",
        nargs="?",
        const="__AUTO__",
        default=None,
        metavar="FILE",
        help="Also write a static PDF to --output-dir (default: same base name as the HTML report). Requires kaleido.",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading {csv_path}...")
    df = load_data(str(csv_path))
    df = add_derived_columns(df)
    date_range = format_date_range(df)
    print(f"  {len(df)} data points | Date range: {date_range}")

    fills_df = load_fills(csv_path)
    if fills_df is not None:
        print(f"  {len(fills_df)} fills loaded from {csv_path.stem}_fills.csv")

    asset_label = resolve_asset_label(args.asset)
    print(f"  Title asset: {asset_label}")

    metrics = load_metrics_csv(csv_path)
    if metrics is not None:
        print(f"  Loaded {csv_path.stem}_metrics.csv")
    else:
        print("  No *_metrics.csv (table uses curve-only fallbacks where possible)")

    fig_combined = plot_combined_dashboard(
        df, fills_df, asset=asset_label, metrics=metrics
    )

    if args.output_html:
        custom = Path(args.output_html)
        output_path = custom if custom.is_absolute() else output_dir / custom.name
        report_stem = output_path.stem
    else:
        report_stem = report_stem_from_df(df, asset_label)
        output_path = output_dir / f"{report_stem}.html"

    fig_combined.write_html(str(output_path))

    max_embed = int(max(args.max_embed_mb, 0.5) * 1_000_000)
    dl_payload = prepare_download_payload(csv_path, metrics, max_embed)
    fills_path = csv_path.parent / f"{csv_path.stem}_fills.csv"
    if (
        fills_path.exists()
        and dl_payload["fills_csv"] is None
        and output_dir.resolve() != csv_path.parent.resolve()
    ):
        dest = output_dir / fills_path.name
        shutil.copy2(fills_path, dest)
        dl_payload["fills_bar_note"] = (
            f"Fills CSV is large and not embedded. Copied to this folder: <code>{fills_path.name}</code>."
        )
        print(f"  Copied fills to {dest} (too large to embed; share with HTML)")
    inject_csv_download_ui(output_path, dl_payload)

    print(f"\nSaved: {output_path}")

    if args.pdf:
        if args.pdf == "__AUTO__":
            pdf_path = output_dir / f"{report_stem}.pdf"
        else:
            pdf_arg = Path(args.pdf)
            pdf_path = pdf_arg if pdf_arg.is_absolute() else output_dir / pdf_arg.name
        if write_figure_pdf(fig_combined, pdf_path):
            print(f"Saved: {pdf_path} (static PDF — not interactive)")
    msg = "Equity curve + drawdown + daily P&L"
    if fills_df is not None and len(fills_df) > 0:
        msg += f" + {len(fills_df)} fills table"
    msg += ", title + metrics table + CSV download bar"
    print(f"  ({msg})")

    if args.individual:
        fig_equity = plot_equity_curve(df)
        fig_drawdown = plot_drawdown(df)
        fig_daily = plot_daily_pnl(df)
        equity_path = output_dir / f"{report_stem}_equity_curve.html"
        drawdown_path = output_dir / f"{report_stem}_drawdown.html"
        daily_path = output_dir / f"{report_stem}_daily_pnl.html"
        fig_equity.write_html(str(equity_path))
        fig_drawdown.write_html(str(drawdown_path))
        fig_daily.write_html(str(daily_path))
        print(f"  Also saved individual: equity, drawdown, daily_pnl")

    if not args.no_browser:
        fig_combined.show()


if __name__ == "__main__":
    main()

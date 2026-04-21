//! RebateMM Strategy — latest strategy 
//!
//! First version: microprice fair value + fixed spread + inventory skew +
//! volatility filter + simple hedge rule + fast cancel/repost.
//!
//! PnL = maker rebates + spread capture - adverse selection - hedging cost
//!
//! Hard filters (optional): impulse kill (symmetric, asymmetric, or hybrid extreme), impulse **size skew** (signed microprice → bid/ask ×), **Phase 1 impulse sizing** (`|microprice impulse|` → smoothed global size mult), optional **microprice edge size scale** (`|microprice−mid|` bps → clip multiplier, always quote), optional **tape-quality regime** on that scale (EMA of per-fill spread capture bps → blend multiplier toward **1×** on good tape), optional **tape upside** (same EMA → stack **>1×** capped size on top of mp-edge mult), wide-spread no-quote, asymmetric passive depth — see `with_impulse_kill_switch`, `with_impulse_kill_asymmetric`, `with_impulse_kill_hybrid_extreme_bps`, `with_impulse_size_skew`, `with_impulse_phase1_sizing`, `with_microprice_edge_size_scale`, `with_microprice_edge_tape_regime`, `with_microprice_edge_tape_upside`, `with_wide_spread_no_quotes`, `with_asymmetric_passive_depth`.

use mm_core::strategy::{Fill, OrderIntent, OrderType, Strategy, StrategyError};
use mm_core::market_data::{OrderBook, OrderSide};
use mm_core::Portfolio;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::collections::VecDeque;

/// Maps spread-capture EMA to **r∈[0,1]** for blending mp-edge mult toward 1. Pure for unit tests.
fn mp_tape_relaxation_from_state(
    spread_ewma: Option<f64>,
    fill_count: u64,
    min_fills: u32,
    low: f64,
    high: f64,
) -> f64 {
    if min_fills > 0 && fill_count < min_fills as u64 {
        return 0.0;
    }
    let Some(s) = spread_ewma else {
        return 0.0;
    };
    if high <= low + 1e-12 {
        return 0.0;
    }
    ((s - low) / (high - low)).clamp(0.0, 1.0)
}

/// Maps spread-capture EMA to a multiplier in **[1, max_mult]** (linear in EMA between thresholds). Pure for tests.
fn mp_tape_upside_multiplier_from_state(
    spread_ewma: Option<f64>,
    fill_count: u64,
    min_fills: u32,
    low_bps: f64,
    high_bps: f64,
    max_mult: f64,
) -> f64 {
    if max_mult <= 1.0 + 1e-12 {
        return 1.0;
    }
    if min_fills > 0 && fill_count < min_fills as u64 {
        return 1.0;
    }
    let Some(s) = spread_ewma else {
        return 1.0;
    };
    if high_bps <= low_bps + 1e-12 {
        return 1.0;
    }
    let t = ((s - low_bps) / (high_bps - low_bps)).clamp(0.0, 1.0);
    (1.0 + t * (max_mult - 1.0)).clamp(1.0, max_mult)
}

pub struct RebateMMStrategy {
    // --- Config ---
    order_amount: Decimal,
    tick_size: Decimal,

    // Spread: base 2–4 bps, scaled by volatility
    base_spread_bps: f64,
    vol_lookback: usize,
    vol_threshold_bps: f64,
    max_spread_multiplier: f64,

    // Inventory (Stoikov): target=0, skew
    inventory_target: Decimal,
    inventory_limit: Decimal, // hedge when abs(inventory) > this (in base aunits)
    hedge_limit: Decimal,    // same or slightly below inventory_limit
    inventory_skew_k: f64,   // inventory_adjustment = k * inventory (in bps)

    // Adverse selection filters
    book_imbalance_threshold: f64,
    book_imbalance_levels: usize,

    // Directional alpha filter: use microprice-vs-mid to lean away from the
    // weaker side while keeping neutral conditions two-sided.
    directional_skew_enabled: bool,
    directional_signal_threshold_ticks: f64,
    directional_suppress_threshold_ticks: f64,
    directional_widen_ticks: u32,
    queue_join_touch_enabled: bool,
    queue_join_safe_side_enabled: bool,
    queue_join_safe_side_threshold_bps: f64,
    queue_join_safe_side_max_touch_qty: Decimal,
    microprice_impulse_enabled: bool,
    microprice_impulse_lookback: usize,
    microprice_impulse_threshold_bps: f64,
    microprice_impulse_pause_sec: f64,
    /// Phase 4: if `> 0`, do not quote when `|microprice−mid|/mid·10⁴` bps is **below** this (weak book edge).
    microprice_edge_threshold_bps: f64,
    /// When true: multiply resting clip by a factor in `[min_mult, max_mult]` from **instant** `|microprice−mid|` (bps); `k_bps` = edge at which full size is reached (linear ramp). Does not cancel quotes.
    microprice_edge_size_scale_enabled: bool,
    microprice_edge_scale_k_bps: f64,
    microprice_edge_scale_min_mult: f64,
    microprice_edge_scale_max_mult: f64,
    microprice_edge_scale_samples: VecDeque<f64>,
    /// When true with mp edge scale: EMA of **spread capture bps** (per maker fill vs `dynamic_last_mid`) maps to a blend **r∈[0,1]**; effective mult = `raw + r·(1−raw)` so good tape relaxes toward full size.
    microprice_edge_tape_regime_enabled: bool,
    microprice_edge_tape_spread_ewma_alpha: f64,
    /// Below this EMA (bps), **r=0** — full microprice shrink (bad tape).
    microprice_edge_tape_relax_low_spread_bps: f64,
    /// At or above this EMA (bps), **r=1** — ignore mp shrink for that refresh (`mult→1` from mp layer).
    microprice_edge_tape_relax_high_spread_bps: f64,
    microprice_edge_tape_min_fills: u32,
    mp_tape_spread_ewma: Option<f64>,
    mp_tape_regime_fill_count: u64,
    /// When true with mp edge scale: same spread EMA as tape regime → extra size mult in **[1, max_mult]** (stacked after mp-edge effective mult).
    microprice_edge_tape_upside_enabled: bool,
    microprice_edge_tape_upside_max_mult: f64,
    /// Below this EMA (bps), upside mult = **1×** (no boost). Used only when `microprice_edge_tape_upside_use_regime_band` is **false**.
    microprice_edge_tape_upside_low_spread_bps: f64,
    /// At or above this EMA (bps), upside mult = **max_mult**. Used only when `microprice_edge_tape_upside_use_regime_band` is **false**.
    microprice_edge_tape_upside_high_spread_bps: f64,
    /// When **true** (default): `upside_mult = 1 + r·(max_mult−1)` with **r** from the **same** spread EMA map as tape relax (`relax_low`/`relax_high`), gated by `microprice_edge_tape_upside_min_fills`. Matches the EMA scale (~−0.02…0.08); separate 0.10–0.20 bps gates never fire when the EMA sits near 0.
    microprice_edge_tape_upside_use_regime_band: bool,
    microprice_edge_tape_upside_min_fills: u32,
    tape_upside_multiplier_samples: VecDeque<f64>,
    /// If enabled and `|impulse_bps| >=` this (same scale as impulse filter), cancel both sides — no quotes until impulse cools (checked every snapshot).
    impulse_kill_switch_enabled: bool,
    impulse_kill_threshold_bps: f64,
    /// If true with impulse kill: `impulse >= threshold` suppresses **ask** only; `impulse <= -threshold` suppresses **bid** only (symmetric full cancel when false). Mutually exclusive with `impulse_kill_hybrid_extreme_bps`.
    impulse_kill_asymmetric: bool,
    /// When `Some(E)` with impulse kill: `threshold <= |imp| < E` → symmetric full cancel; `imp >= E` → suppress ask only; `imp <= -E` → suppress bid only. Requires `E > impulse_kill_threshold_bps`. Mutually exclusive with `impulse_kill_asymmetric`.
    impulse_kill_hybrid_extreme_bps: Option<f64>,
    /// Layer 2.5b Run 1: scale bid/ask limit size from **signed** microprice impulse (`>0` → bid favored, `<0` → ask favored); `None`/zero impulse → **1×** both.
    impulse_size_skew_enabled: bool,
    /// Multiplier on the **favored** side (default **1.15**).
    impulse_skew_favored: f64,
    /// Multiplier on the **lean** side (default **0.75**).
    impulse_skew_lean: f64,
    /// Phase 1 capital scaling: same size multiplier on **both** sides from **`|microprice impulse|`** (bps), EMA-smoothed and clamped — independent of signed impulse skew.
    impulse_phase1_sizing_enabled: bool,
    /// EMA state for [`impulse_phase1_sizing_enabled`]; `None` until first valid impulse sample after warmup.
    impulse_phase1_sizing_smoothed: Option<f64>,
    /// Bucket multipliers: |imp|<0.30→calm, 0.30–0.60→1.0, 0.60–1.00→mid, ≥1.00→min. Sweepable.
    impulse_phase1_bucket_calm: f64,
    impulse_phase1_bucket_mid: f64,
    impulse_phase1_bucket_min: f64,
    /// Samples of smoothed multiplier for diagnostics (avg, p25/p50/p75, %<1.0). Capped at 50k.
    impulse_phase1_multiplier_samples: VecDeque<f64>,
    /// Cancel both sides when top-of-book spread (bps of mid) exceeds this — unstable / toxic regime filter.
    wide_spread_no_quotes_enabled: bool,
    wide_spread_max_bps: f64,
    /// Split passive depth: bid vs ask step-back in ticks (microprice vs mid).
    asymmetric_passive_depth_enabled: bool,
    microprice_drift_enabled: bool,
    microprice_drift_lookback: usize,
    microprice_drift_threshold_bps: f64,
    /// When true (after warmup): if microprice **below** mid by `min_edge_bps`, pull **bid** `fade_ticks` less aggressive; if **above** mid, pull **ask** (soft retreat from touch — Test 1 / fill quality).
    microprice_fade_enabled: bool,
    /// Require `|(fair−mid)/mid|·10⁴ ≥ min_edge_bps` to apply fade (avoids flicker when fair ≈ mid).
    microprice_fade_min_edge_bps: f64,
    microprice_fade_ticks: u32,
    /// If `> 0`, fade only when `|microprice impulse| ≥` this (noise filter; see LogsBTCUSD-RebateMM.md Test 1).
    microprice_fade_impulse_min_bps: f64,
    /// If true, fade tick count scales with `|impulse|` (0 / 1 / `bucket_max_ticks`) instead of fixed `microprice_fade_ticks`.
    microprice_fade_impulse_bucketed: bool,
    microprice_fade_bucket_impulse_lo_bps: f64,
    microprice_fade_bucket_impulse_hi_bps: f64,
    microprice_fade_bucket_max_ticks: u32,
    /// With `queue_join_touch_enabled`: only use touch when `|impulse|` and `|fair−mid|` bps are within caps; else passive interior.
    conditional_touch_join_enabled: bool,
    conditional_touch_max_impulse_bps: f64,
    conditional_touch_max_edge_bps: f64,
    dynamic_conditional_touch_enabled: bool,
    dynamic_conditional_touch_ewma_alpha: f64,
    dynamic_conditional_touch_min_fills: u32,
    dynamic_conditional_touch_enter_bps: f64,
    dynamic_conditional_touch_exit_bps: f64,
    /// After a fill with spread capture (vs `dynamic_last_mid`) **≤** threshold bps, avoid touch until `fill_touch_brake_sec` elapses (proxy for bad immediate markout).
    fill_touch_brake_enabled: bool,
    fill_touch_brake_bad_spread_bps: f64,
    fill_touch_brake_sec: f64,
    dynamic_spread_enabled: bool,
    /// When true, pull quotes `depth` ticks away from the most aggressive passive
    /// prices, where `depth` is derived from microprice impulse, short vol, and imbalance.
    state_passive_depth_enabled: bool,
    /// Cap on passive depth in ticks (per side).
    state_passive_max_depth_ticks: u32,
    /// Phase 3: widen quiet-signal buckets in [`passive_depth_ticks`] → more `depth=0`, less time at `d=2` when mild impulse/imbalance (spread capture).
    passive_depth_spread_capture_bias: bool,

    /// Per-fill spread capture EMA → discrete depth (and thus touch vs passive). Independent of tape-regime **size** flags; updates in [`on_fill`] when enabled.
    spread_depth_regime_enabled: bool,
    spread_depth_regime_ewma_alpha: f64,
    spread_depth_regime_min_fills: u32,
    /// Hysteresis: enter **Bad** when EWMA drops below this (bps).
    spread_depth_bad_enter_bps: f64,
    /// Exit **Bad** → Neutral when EWMA rises above this (bps).
    spread_depth_bad_exit_bps: f64,
    /// Enter **Good** when EWMA rises above this (bps).
    spread_depth_good_enter_bps: f64,
    /// Exit **Good** → Neutral when EWMA drops below this (bps).
    spread_depth_good_exit_bps: f64,
    /// Passive depth ticks when hysteresis state is Bad / Neutral / Good (`Good` uses 0 → touch when other gates allow).
    spread_depth_ticks_bad: u32,
    spread_depth_ticks_neutral: u32,
    spread_depth_ticks_good: u32,
    /// When enabled: if maker fills in the last `spread_depth_participation_window_sec` are strictly below `spread_depth_participation_min_fills`, reduce regime depth by 1 (min 0).
    spread_depth_participation_floor_enabled: bool,
    spread_depth_participation_window_sec: f64,
    spread_depth_participation_min_fills: u32,
    /// When enabled (with spread depth regime, after EMA warm): each refresh draws `u ~ U[0,1)` deterministically; if `u < p` for the current hysteresis state, allow touch-join; else force passive at regime depth (Good declines use depth 1).
    spread_depth_prob_touch_enabled: bool,
    spread_depth_prob_touch_p_good: f64,
    spread_depth_prob_touch_p_neutral: f64,
    spread_depth_prob_touch_p_bad: f64,

    /// **Tail brake:** own spread-capture EWMA + hysteresis (enter/exit bps). Outside extreme → baseline touch logic unchanged. In extreme → keep full touch-join only with probability `spread_toxicity_touch_mult` (else one-tick passive). No Good/Bad/Neutral.
    spread_toxicity_brake_enabled: bool,
    spread_toxicity_ewma_alpha: f64,
    spread_toxicity_min_fills: u32,
    /// Enter extreme when EWMA drops **below** this (bps), e.g. −0.7.
    spread_toxicity_enter_bps: f64,
    /// Leave extreme when EWMA rises **above** this (bps), e.g. −0.4 (must be **greater** than enter).
    spread_toxicity_exit_bps: f64,
    /// When extreme and touch would otherwise be on: Bernoulli keep-touch probability (e.g. 0.65).
    spread_toxicity_touch_mult: f64,
    /// Passive depth when toxicity downgrades touch (typically 1).
    spread_toxicity_forced_passive_ticks: u32,


    /// Mid-regime spread penalty: when |impulse| in [lo, hi] and live spread < tight_spread_bps, size *= size_mult. Surgical removal of bad trades.
    mid_regime_spread_penalty_enabled: bool,
    mid_regime_impulse_lo: f64,
    mid_regime_impulse_hi: f64,
    mid_regime_tight_spread_bps: f64,
    mid_regime_size_mult: f64,

    /// When enabled, keep last emitted bid/ask unless min-hold elapsed or move ≥ hysteresis ticks.
    quote_stickiness_price_enabled: bool,
    /// Do not adopt a new candidate price until this long after the last reprice (seconds).
    quote_stickiness_min_hold_sec: f64,
    /// Ignore candidate moves smaller than this many ticks (after min-hold allows updates).
    quote_stickiness_hysteresis_ticks: u32,
    /// Only change passive `depth_ticks` when |new − last| ≥ this (0 = off). Reduces depth flicker.
    quote_stickiness_depth_min_delta_ticks: u32,

    // Order management
    order_refresh_sec: f64,
    warmup_sec: f64,
    hedge_cooldown_sec: f64,

    // --- State ---
    start_ts: Option<f64>,
    last_refresh_ts: f64,
    hedge_cooldown_until: f64,
    mid_history: VecDeque<f64>,
    microprice_history: VecDeque<f64>,
    microprice_pause_until: f64,
    /// While `ts < touch_brake_until_ts`, touch-join is disabled if `fill_touch_brake_enabled`.
    touch_brake_until_ts: f64,
    /// Spread capture EMA (bps) for [`spread_depth_regime_enabled`]; updated on each maker fill.
    spread_depth_regime_ewma: Option<f64>,
    spread_depth_regime_fill_count: u64,
    /// 0 = Neutral, 1 = Bad, 2 = Good (hysteresis on `spread_depth_regime_ewma`).
    spread_depth_hyst_state: u8,
    /// Histogram of quote refreshes by regime state [Neutral, Bad, Good].
    diag_spread_depth_regime_hist: [u64; 3],
    /// Timestamps for [`spread_depth_participation_floor_enabled`] fill counting.
    spread_depth_participation_fill_ts: VecDeque<f64>,
    /// Refreshes where participation floor reduced depth by one tick.
    diag_spread_depth_participation_floor_pulls: u64,
    spread_depth_prob_touch_u64_salt: u64,
    diag_spread_depth_prob_touch_rolls: u64,
    diag_spread_depth_prob_touch_chose_touch: u64,

    spread_toxicity_ewma: Option<f64>,
    spread_toxicity_fill_count: u64,
    spread_toxicity_extreme: bool,
    spread_toxicity_u64_salt: u64,
    diag_spread_toxicity_touch_downgrades: u64,
    diag_spread_toxicity_extreme_refreshes: u64,
    dynamic_conditional_touch_ewma: Option<f64>,
    dynamic_conditional_touch_fill_count: u64,
    dynamic_conditional_touch_active: bool,
    diag_dynamic_conditional_touch_refreshes: u64,
    diag_dynamic_conditional_touch_transitions: u64,


    /// Histogram of passive `depth_ticks` at each quote refresh (buckets 0..=6, 7+).
    /// Only incremented when state-dependent passive is on and not in full touch-join mode.
    passive_depth_hist: [u64; 8],
    /// Tight-book fix: `max_passive_bid` can equal `min_passive_ask` (e.g. spread = 2 ticks) → same quote price.
    passive_pinch_recoveries: u64,
    /// Quote refresh aborted after pinch recovery still failed (`bid >= ask`).
    passive_pinch_aborts: u64,

    // Quote stickiness (last emitted resting prices / depth)
    last_sticky_bid: Option<Decimal>,
    last_sticky_ask: Option<Decimal>,
    last_sticky_ask_amount: Option<Decimal>,
    last_sticky_depth_ticks: Option<u32>,
    /// Last time we accepted a new candidate (either side or depth) for stickiness clocks.
    last_sticky_reprice_ts: f64,

    /// Diagnostics: how often we kept vs changed bid/ask at refresh (for tape / churn).
    diag_sticky_bid_kept: u64,
    diag_sticky_bid_repriced: u64,
    diag_sticky_ask_kept: u64,
    diag_sticky_ask_repriced: u64,
    diag_sticky_depth_kept: u64,
    diag_sticky_depth_changed: u64,
    diag_sticky_price_min_hold_clamps: u64,
    diag_sticky_price_hysteresis_clamps: u64,

    diag_impulse_kill_cancels: u64,
    diag_wide_spread_cancels: u64,
    diag_microprice_fade_bid: u64,
    diag_microprice_fade_ask: u64,
    diag_touch_join_fallback: u64,
    diag_fill_touch_brake_triggers: u64,
    diag_spread_depth_regime_transitions: u64,

    /// Directional skew: `(microprice_fair - mid) / tick_size` at each quote refresh (when skew enabled).
    diag_dir_skew_refresh_samples: u64,
    /// Count where `|signal_ticks| < directional_signal_threshold_ticks` (no widen / suppress from skew).
    diag_dir_skew_in_deadband: u64,
    /// Histogram of `|signal_ticks|`: [0,0.15), [0.15,0.5), [0.5,1), [1,2), [2,∞).
    diag_dir_skew_abs_hist: [u64; 5],
    diag_dir_skew_widen_ask: u64,
    diag_dir_skew_widen_bid: u64,
    diag_dir_skew_suppress_ask: u64,
    diag_dir_skew_suppress_bid: u64,

    /// Optional: scale `order_amount` from rolling net-edge (bps) + fill-rate signals (see `maybe_apply_dynamic_order_resize`).
    dynamic_sizing_enabled: bool,
    dynamic_clip_min: Decimal,
    dynamic_clip_max: Decimal,
    dynamic_step_up: f64,
    dynamic_step_down: f64,
    dynamic_cooldown_sec: f64,
    dynamic_edge_alpha_fast: f64,
    dynamic_edge_alpha_slow: f64,
    dynamic_edge_deadband_bps: f64,
    dynamic_bad_edge_streak: u32,
    dynamic_flow_window_sec: f64,
    dynamic_flow_low_ratio: f64,
    dynamic_flow_ref_alpha: f64,
    dynamic_maker_fee_bps: f64,
    dynamic_min_fills_before_resize: u32,
    dynamic_current_clip: Decimal,
    dynamic_last_mid: f64,
    dynamic_edge_ewma: Option<f64>,
    dynamic_edge_ref: Option<f64>,
    dynamic_flow_ref: Option<f64>,
    dynamic_fill_ts: VecDeque<f64>,
    dynamic_bad_streak: u32,
    dynamic_last_resize_ts: f64,
    dynamic_fill_count: u64,
    dynamic_resize_up: u64,
    dynamic_resize_down: u64,
    /// Min `flow_ref` (fills/s) before "low flow → step up" applies; avoids ratcheting when EMA is ~0.
    dynamic_flow_ref_min_for_step_up: f64,
}

impl RebateMMStrategy {
    pub fn new(order_amount: Decimal, tick_size: Decimal) -> Self {
        Self {
            order_amount,
            tick_size,
            base_spread_bps: 3.0,
            vol_lookback: 50,
            vol_threshold_bps: 2.0,
            max_spread_multiplier: 4.0,

            inventory_target: Decimal::ZERO,
            inventory_limit: dec!(1.0),
            hedge_limit: dec!(1.0),
            inventory_skew_k: 0.5,

            book_imbalance_threshold: 0.65,
            book_imbalance_levels: 3,

            directional_skew_enabled: false,
            directional_signal_threshold_ticks: 0.0,
            directional_suppress_threshold_ticks: 0.0,
            directional_widen_ticks: 0,
            queue_join_touch_enabled: false,
            queue_join_safe_side_enabled: false,
            queue_join_safe_side_threshold_bps: 0.0,
            queue_join_safe_side_max_touch_qty: Decimal::ZERO,
            microprice_impulse_enabled: false,
            microprice_impulse_lookback: 0,
            microprice_impulse_threshold_bps: 0.0,
            microprice_impulse_pause_sec: 0.0,
            microprice_edge_threshold_bps: 0.0,
            microprice_edge_size_scale_enabled: false,
            microprice_edge_scale_k_bps: 0.05,
            microprice_edge_scale_min_mult: 0.3,
            microprice_edge_scale_max_mult: 1.0,
            microprice_edge_scale_samples: VecDeque::new(),
            microprice_edge_tape_regime_enabled: false,
            microprice_edge_tape_spread_ewma_alpha: 0.02,
            microprice_edge_tape_relax_low_spread_bps: -0.02,
            microprice_edge_tape_relax_high_spread_bps: 0.08,
            microprice_edge_tape_min_fills: 50,
            mp_tape_spread_ewma: None,
            mp_tape_regime_fill_count: 0,
            microprice_edge_tape_upside_enabled: false,
            microprice_edge_tape_upside_max_mult: 1.15,
            microprice_edge_tape_upside_low_spread_bps: 0.10,
            microprice_edge_tape_upside_high_spread_bps: 0.20,
            microprice_edge_tape_upside_use_regime_band: true,
            microprice_edge_tape_upside_min_fills: 50,
            tape_upside_multiplier_samples: VecDeque::new(),
            impulse_kill_switch_enabled: false,
            impulse_kill_threshold_bps: 1.75,
            impulse_kill_asymmetric: false,
            impulse_kill_hybrid_extreme_bps: None,
            impulse_size_skew_enabled: false,
            impulse_skew_favored: 1.15,
            impulse_skew_lean: 0.75,
            impulse_phase1_sizing_enabled: false,
            impulse_phase1_sizing_smoothed: None,
            impulse_phase1_bucket_calm: 1.3,
            impulse_phase1_bucket_mid: 0.7,
            impulse_phase1_bucket_min: 0.4,
            impulse_phase1_multiplier_samples: VecDeque::new(),
            wide_spread_no_quotes_enabled: false,
            wide_spread_max_bps: 6.0,
            asymmetric_passive_depth_enabled: false,
            microprice_drift_enabled: false,
            microprice_drift_lookback: 0,
            microprice_drift_threshold_bps: 0.0,
            microprice_fade_enabled: false,
            microprice_fade_min_edge_bps: 0.02,
            microprice_fade_ticks: 1,
            microprice_fade_impulse_min_bps: 0.0,
            microprice_fade_impulse_bucketed: false,
            microprice_fade_bucket_impulse_lo_bps: 0.3,
            microprice_fade_bucket_impulse_hi_bps: 0.8,
            microprice_fade_bucket_max_ticks: 2,
            conditional_touch_join_enabled: false,
            conditional_touch_max_impulse_bps: 0.6,
            conditional_touch_max_edge_bps: 0.02,
            dynamic_conditional_touch_enabled: false,
            dynamic_conditional_touch_ewma_alpha: 0.02,
            dynamic_conditional_touch_min_fills: 50,
            dynamic_conditional_touch_enter_bps: -0.30,
            dynamic_conditional_touch_exit_bps: -0.15,
            fill_touch_brake_enabled: false,
            fill_touch_brake_bad_spread_bps: -0.5,
            fill_touch_brake_sec: 0.2,
            dynamic_spread_enabled: false,
            state_passive_depth_enabled: false,
            state_passive_max_depth_ticks: 2,
            passive_depth_spread_capture_bias: false,

            spread_depth_regime_enabled: false,
            spread_depth_regime_ewma_alpha: 0.02,
            spread_depth_regime_min_fills: 50,
            spread_depth_bad_enter_bps: -0.3,
            spread_depth_bad_exit_bps: -0.2,
            spread_depth_good_enter_bps: 0.1,
            spread_depth_good_exit_bps: 0.05,
            spread_depth_ticks_bad: 2,
            spread_depth_ticks_neutral: 1,
            spread_depth_ticks_good: 0,
            spread_depth_participation_floor_enabled: false,
            spread_depth_participation_window_sec: 900.0,
            spread_depth_participation_min_fills: 2,
            spread_depth_prob_touch_enabled: false,
            spread_depth_prob_touch_p_good: 0.85,
            spread_depth_prob_touch_p_neutral: 0.5,
            spread_depth_prob_touch_p_bad: 0.05,

            spread_toxicity_brake_enabled: false,
            spread_toxicity_ewma_alpha: 0.02,
            spread_toxicity_min_fills: 50,
            spread_toxicity_enter_bps: -0.7,
            spread_toxicity_exit_bps: -0.4,
            spread_toxicity_touch_mult: 0.65,
            spread_toxicity_forced_passive_ticks: 1,


            mid_regime_spread_penalty_enabled: false,
            mid_regime_impulse_lo: 0.5,
            mid_regime_impulse_hi: 1.0,
            mid_regime_tight_spread_bps: 3.0,
            mid_regime_size_mult: 0.7,

            quote_stickiness_price_enabled: false,
            quote_stickiness_min_hold_sec: 0.0,
            quote_stickiness_hysteresis_ticks: 0,
            quote_stickiness_depth_min_delta_ticks: 0,

            order_refresh_sec: 0.5,
            warmup_sec: 30.0,
            hedge_cooldown_sec: 60.0,

            start_ts: None,
            last_refresh_ts: 0.0,
            hedge_cooldown_until: 0.0,
            mid_history: VecDeque::new(),
            microprice_history: VecDeque::new(),
            microprice_pause_until: 0.0,
            touch_brake_until_ts: f64::NEG_INFINITY,
            spread_depth_regime_ewma: None,
            spread_depth_regime_fill_count: 0,
            spread_depth_hyst_state: 0,
            diag_spread_depth_regime_hist: [0; 3],
            spread_depth_participation_fill_ts: VecDeque::new(),
            diag_spread_depth_participation_floor_pulls: 0,
            spread_depth_prob_touch_u64_salt: 0xC6A4_A793_5BD1_E995,
            diag_spread_depth_prob_touch_rolls: 0,
            diag_spread_depth_prob_touch_chose_touch: 0,
            spread_toxicity_ewma: None,
            spread_toxicity_fill_count: 0,
            spread_toxicity_extreme: false,
            spread_toxicity_u64_salt: 0xD1B54A32C9E7F18A,
            diag_spread_toxicity_touch_downgrades: 0,
            diag_spread_toxicity_extreme_refreshes: 0,
            dynamic_conditional_touch_ewma: None,
            dynamic_conditional_touch_fill_count: 0,
            dynamic_conditional_touch_active: false,
            diag_dynamic_conditional_touch_refreshes: 0,
            diag_dynamic_conditional_touch_transitions: 0,
            passive_depth_hist: [0; 8],
            passive_pinch_recoveries: 0,
            passive_pinch_aborts: 0,

            last_sticky_bid: None,
            last_sticky_ask: None,
            last_sticky_ask_amount: None,
            last_sticky_depth_ticks: None,
            last_sticky_reprice_ts: 0.0,

            diag_sticky_bid_kept: 0,
            diag_sticky_bid_repriced: 0,
            diag_sticky_ask_kept: 0,
            diag_sticky_ask_repriced: 0,
            diag_sticky_depth_kept: 0,
            diag_sticky_depth_changed: 0,
            diag_sticky_price_min_hold_clamps: 0,
            diag_sticky_price_hysteresis_clamps: 0,

            diag_impulse_kill_cancels: 0,
            diag_wide_spread_cancels: 0,
            diag_microprice_fade_bid: 0,
            diag_microprice_fade_ask: 0,
            diag_touch_join_fallback: 0,
            diag_fill_touch_brake_triggers: 0,
            diag_spread_depth_regime_transitions: 0,

            diag_dir_skew_refresh_samples: 0,
            diag_dir_skew_in_deadband: 0,
            diag_dir_skew_abs_hist: [0; 5],
            diag_dir_skew_widen_ask: 0,
            diag_dir_skew_widen_bid: 0,
            diag_dir_skew_suppress_ask: 0,
            diag_dir_skew_suppress_bid: 0,

            dynamic_sizing_enabled: false,
            dynamic_clip_min: dec!(0.01),
            dynamic_clip_max: dec!(100.0),
            dynamic_step_up: 1.1,
            dynamic_step_down: 0.85,
            dynamic_cooldown_sec: 60.0,
            dynamic_edge_alpha_fast: 0.15,
            dynamic_edge_alpha_slow: 0.02,
            dynamic_edge_deadband_bps: 0.15,
            dynamic_bad_edge_streak: 5,
            dynamic_flow_window_sec: 120.0,
            dynamic_flow_low_ratio: 0.85,
            dynamic_flow_ref_alpha: 0.05,
            dynamic_maker_fee_bps: -0.75,
            dynamic_min_fills_before_resize: 30,
            dynamic_current_clip: order_amount,
            dynamic_last_mid: 0.0,
            dynamic_edge_ewma: None,
            dynamic_edge_ref: None,
            dynamic_flow_ref: None,
            dynamic_fill_ts: VecDeque::new(),
            dynamic_bad_streak: 0,
            dynamic_last_resize_ts: f64::NEG_INFINITY,
            dynamic_fill_count: 0,
            dynamic_resize_up: 0,
            dynamic_resize_down: 0,
            dynamic_flow_ref_min_for_step_up: 0.005,
        }
    }

    pub fn with_base_spread(mut self, bps: f64) -> Self {
        self.base_spread_bps = bps;
        self
    }

    pub fn with_volatility(mut self, lookback: usize, threshold_bps: f64, max_mult: f64) -> Self {
        self.vol_lookback = lookback;
        self.vol_threshold_bps = threshold_bps;
        self.max_spread_multiplier = max_mult;
        self
    }

    pub fn with_inventory_limits(mut self, limit: Decimal, hedge_limit: Decimal) -> Self {
        self.inventory_limit = limit;
        self.hedge_limit = hedge_limit;
        self
    }

    pub fn with_inventory_skew(mut self, k: f64) -> Self {
        self.inventory_skew_k = k;
        self
    }

    pub fn with_book_imbalance(mut self, threshold: f64, levels: usize) -> Self {
        self.book_imbalance_threshold = threshold;
        self.book_imbalance_levels = levels;
        self
    }

    pub fn with_directional_skew(
        mut self,
        signal_threshold_ticks: f64,
        suppress_threshold_ticks: f64,
        widen_ticks: u32,
    ) -> Self {
        self.directional_skew_enabled = true;
        self.directional_signal_threshold_ticks = signal_threshold_ticks;
        self.directional_suppress_threshold_ticks = suppress_threshold_ticks;
        self.directional_widen_ticks = widen_ticks;
        self
    }

    pub fn with_queue_join_touch(mut self, enabled: bool) -> Self {
        self.queue_join_touch_enabled = enabled;
        self
    }

    pub fn with_safe_side_touch_join(mut self, threshold_bps: f64) -> Self {
        self.queue_join_safe_side_enabled = true;
        self.queue_join_safe_side_threshold_bps = threshold_bps;
        self
    }

    pub fn with_queue_aware_safe_side_touch_join(
        mut self,
        threshold_bps: f64,
        max_touch_qty: Decimal,
    ) -> Self {
        self.queue_join_safe_side_enabled = true;
        self.queue_join_safe_side_threshold_bps = threshold_bps;
        self.queue_join_safe_side_max_touch_qty = max_touch_qty;
        self
    }

    pub fn with_microprice_impulse_filter(
        mut self,
        lookback_snapshots: usize,
        threshold_bps: f64,
        pause_sec: f64,
    ) -> Self {
        self.microprice_impulse_enabled = true;
        self.microprice_impulse_lookback = lookback_snapshots;
        self.microprice_impulse_threshold_bps = threshold_bps;
        self.microprice_impulse_pause_sec = pause_sec;
        self
    }

    /// Phase 4 edge filter: when `threshold_bps > 0`, skip quoting if `|microprice−mid|/mid·10⁴` is below `threshold_bps`.
    pub fn with_microprice_edge_filter(mut self, threshold_bps: f64) -> Self {
        self.microprice_edge_threshold_bps = threshold_bps;
        self
    }

    /// Scale resting clip from **|microprice−mid|** (bps): at 0 edge → `min_mult`; at `k_bps` and above → `max_mult` (linear). Composes after Phase-1 / before mid-regime penalty by default in `on_orderbook_update`.
    pub fn with_microprice_edge_size_scale(
        mut self,
        enabled: bool,
        k_bps: f64,
        min_mult: f64,
        max_mult: f64,
    ) -> Self {
        self.microprice_edge_size_scale_enabled = enabled;
        self.microprice_edge_scale_k_bps = k_bps;
        self.microprice_edge_scale_min_mult = min_mult;
        self.microprice_edge_scale_max_mult = max_mult;
        self
    }

    /// Blend mp-edge size mult toward **1×** when rolling spread-capture EMA is strong (good tape); toward raw ramp when weak. Requires `with_microprice_edge_size_scale(true, …)`.
    pub fn with_microprice_edge_tape_regime(
        mut self,
        enabled: bool,
        spread_ewma_alpha: f64,
        relax_low_spread_bps: f64,
        relax_high_spread_bps: f64,
        min_fills_before_relax: u32,
    ) -> Self {
        self.microprice_edge_tape_regime_enabled = enabled;
        self.microprice_edge_tape_spread_ewma_alpha = spread_ewma_alpha;
        self.microprice_edge_tape_relax_low_spread_bps = relax_low_spread_bps;
        self.microprice_edge_tape_relax_high_spread_bps = relax_high_spread_bps;
        self.microprice_edge_tape_min_fills = min_fills_before_relax;
        self
    }

    /// Stack **tape upside** on the mp-edge layer: same per-fill spread EMA as tape regime → **\[1, max_mult]** (requires `with_microprice_edge_size_scale(true, …)`). Uses `microprice_edge_tape_spread_ewma_alpha` for EMA updates (shared with tape regime).
    /// When `use_regime_band`: `mult = 1 + r·(max−1)` with **r** = same linear map as tape relax (`relax_low`/`relax_high`). Otherwise `mult` ramps between `low_spread_bps` and `high_spread_bps` (absolute EMA bps).
    pub fn with_microprice_edge_tape_upside(
        mut self,
        enabled: bool,
        max_mult: f64,
        low_spread_bps_for_boost: f64,
        high_spread_bps_for_full_boost: f64,
        min_fills_before_boost: u32,
    ) -> Self {
        self.microprice_edge_tape_upside_enabled = enabled;
        self.microprice_edge_tape_upside_max_mult = max_mult;
        self.microprice_edge_tape_upside_low_spread_bps = low_spread_bps_for_boost;
        self.microprice_edge_tape_upside_high_spread_bps = high_spread_bps_for_full_boost;
        self.microprice_edge_tape_upside_min_fills = min_fills_before_boost;
        self
    }

    pub fn with_microprice_edge_tape_upside_use_regime_band(mut self, use_regime_band: bool) -> Self {
        self.microprice_edge_tape_upside_use_regime_band = use_regime_band;
        self
    }

    /// Hard stop: no quotes while `|microprice impulse| >= threshold_bps` (same bps scale as impulse filter).
    /// Evaluated every snapshot after warmup; does not use the pause timer (impulse must drop below threshold).
    pub fn with_impulse_kill_switch(mut self, threshold_bps: f64) -> Self {
        self.impulse_kill_switch_enabled = true;
        self.impulse_kill_threshold_bps = threshold_bps.max(1e-9);
        self
    }

    /// Per-side impulse kill (Phase 2): with impulse kill enabled, positive impulse suppresses the **ask** only; negative impulse suppresses the **bid** only. Requires `with_impulse_kill_switch` and microprice impulse lookback.
    pub fn with_impulse_kill_asymmetric(mut self, enabled: bool) -> Self {
        self.impulse_kill_asymmetric = enabled;
        self
    }

    /// Layer 2.5b Run 3: hybrid kill — symmetric full cancel when `threshold <= |imp| < extreme_bps`; one-sided suppress when `|imp| >= extreme_bps` (`imp >= extreme` → no ask; `imp <= -extreme` → no bid). Requires `with_impulse_kill_switch(threshold)` with `extreme_bps > threshold`. Incompatible with `with_impulse_kill_asymmetric(true)`.
    pub fn with_impulse_kill_hybrid_extreme_bps(mut self, extreme_bps: f64) -> Self {
        self.impulse_kill_hybrid_extreme_bps = Some(extreme_bps.max(1e-9));
        self
    }

    /// Scale bid/ask **limit** sizes from signed microprice impulse (Run 1: lean instead of asymmetric kill). Requires microprice impulse lookback.
    pub fn with_impulse_size_skew(mut self, enabled: bool) -> Self {
        self.impulse_size_skew_enabled = enabled;
        self
    }

    pub fn with_impulse_size_skew_multipliers(mut self, favored: f64, lean: f64) -> Self {
        self.impulse_skew_favored = favored;
        self.impulse_skew_lean = lean;
        self
    }

    /// Phase 1: scale **both** bid and ask limit sizes by `base × mult(|impulse|)` with bucket → clamp `[0.5, 1.3]` → EMA `0.8·prev + 0.2·new`. Requires microprice impulse lookback (`with_microprice_impulse_filter`).
    pub fn with_impulse_phase1_sizing(mut self, enabled: bool) -> Self {
        self.impulse_phase1_sizing_enabled = enabled;
        self
    }

    /// Phase 1 bucket multipliers: |imp|<0.30→calm, 0.30–0.60→1.0, 0.60–1.00→mid, ≥1.00→min. For sweep tuning.
    pub fn with_impulse_phase1_buckets(mut self, calm: f64, mid: f64, min: f64) -> Self {
        self.impulse_phase1_bucket_calm = calm;
        self.impulse_phase1_bucket_mid = mid;
        self.impulse_phase1_bucket_min = min;
        self
    }

    /// No quotes when L2 spread (as bps of mid) is above `max_spread_bps` (e.g. 6–8).
    pub fn with_wide_spread_no_quotes(mut self, max_spread_bps: f64) -> Self {
        self.wide_spread_no_quotes_enabled = true;
        self.wide_spread_max_bps = max_spread_bps.max(1e-9);
        self
    }

    /// Use different passive step-back on bid vs ask from microprice vs mid (reduces selling into strength / buying into weakness).
    pub fn with_asymmetric_passive_depth(mut self, enabled: bool) -> Self {
        self.asymmetric_passive_depth_enabled = enabled;
        self
    }

    pub fn with_microprice_drift_filter(
        mut self,
        lookback_snapshots: usize,
        threshold_bps: f64,
    ) -> Self {
        self.microprice_drift_enabled = true;
        self.microprice_drift_lookback = lookback_snapshots;
        self.microprice_drift_threshold_bps = threshold_bps;
        self
    }

    /// Microprice **fade**: step the disadvantaged side back by `fade_ticks` (bid when fair\<mid, ask when fair\>mid). No hard off switch — see `LogsBTCUSD-RebateMM.md` Test 1.
    pub fn with_microprice_fade(mut self, enabled: bool, min_edge_bps: f64, fade_ticks: u32) -> Self {
        self.microprice_fade_enabled = enabled;
        self.microprice_fade_min_edge_bps = min_edge_bps;
        self.microprice_fade_ticks = fade_ticks.max(1);
        self
    }

    pub fn with_microprice_fade_impulse_gate(mut self, min_impulse_bps: f64) -> Self {
        self.microprice_fade_impulse_min_bps = min_impulse_bps.max(0.0);
        self
    }

    pub fn with_microprice_fade_impulse_buckets(
        mut self,
        enabled: bool,
        lo_bps: f64,
        hi_bps: f64,
        max_ticks: u32,
    ) -> Self {
        self.microprice_fade_impulse_bucketed = enabled;
        self.microprice_fade_bucket_impulse_lo_bps = lo_bps;
        self.microprice_fade_bucket_impulse_hi_bps = hi_bps;
        self.microprice_fade_bucket_max_ticks = max_ticks.max(1).min(6);
        self
    }

    /// Only join touch when `|impulse|` and `|fair−mid|` (bps) are within caps; otherwise use passive interior (same as non-touch path).
    pub fn with_conditional_touch_join(mut self, enabled: bool, max_impulse_bps: f64, max_edge_bps: f64) -> Self {
        self.conditional_touch_join_enabled = enabled;
        self.conditional_touch_max_impulse_bps = max_impulse_bps.max(0.0);
        self.conditional_touch_max_edge_bps = max_edge_bps.max(0.0);
        self
    }

    /// Dynamically enable the existing conditional-touch gate when recent spread capture turns bad.
    pub fn with_dynamic_conditional_touch(
        mut self,
        enabled: bool,
        ewma_alpha: f64,
        min_fills: u32,
        enter_bps: f64,
        exit_bps: f64,
    ) -> Self {
        self.dynamic_conditional_touch_enabled = enabled;
        self.dynamic_conditional_touch_ewma_alpha = ewma_alpha.clamp(1e-9, 1.0);
        self.dynamic_conditional_touch_min_fills = min_fills.max(1);
        self.dynamic_conditional_touch_enter_bps = enter_bps;
        self.dynamic_conditional_touch_exit_bps = exit_bps;
        self
    }

    /// Spread capture at fill vs `dynamic_last_mid`; if **≤** `bad_spread_bps`, disable touch-join for `brake_sec` (sim seconds).
    pub fn with_fill_touch_brake(mut self, enabled: bool, bad_spread_bps: f64, brake_sec: f64) -> Self {
        self.fill_touch_brake_enabled = enabled;
        self.fill_touch_brake_bad_spread_bps = bad_spread_bps;
        self.fill_touch_brake_sec = brake_sec.max(1e-6);
        self
    }

    /// Spread-capture EMA (bps per fill vs last mid) → hysteresis **Bad / Neutral / Good** → passive depth ticks; depth **0** allows touch-join when other gates pass.
    pub fn with_spread_depth_regime(
        mut self,
        enabled: bool,
        ewma_alpha: f64,
        min_fills: u32,
        bad_enter_bps: f64,
        bad_exit_bps: f64,
        good_enter_bps: f64,
        good_exit_bps: f64,
        ticks_bad: u32,
        ticks_neutral: u32,
        ticks_good: u32,
    ) -> Self {
        self.spread_depth_regime_enabled = enabled;
        self.spread_depth_regime_ewma_alpha = ewma_alpha.clamp(1e-9, 1.0);
        self.spread_depth_regime_min_fills = min_fills;
        self.spread_depth_bad_enter_bps = bad_enter_bps;
        self.spread_depth_bad_exit_bps = bad_exit_bps;
        self.spread_depth_good_enter_bps = good_enter_bps;
        self.spread_depth_good_exit_bps = good_exit_bps;
        self.spread_depth_ticks_bad = ticks_bad.min(6);
        self.spread_depth_ticks_neutral = ticks_neutral.min(6);
        self.spread_depth_ticks_good = ticks_good.min(6);
        self
    }

    /// When spread depth regime is on: if fill count in the rolling window is strictly below `min_fills`, pull regime depth down by one tick (floor at 0).
    pub fn with_spread_depth_participation_floor(
        mut self,
        enabled: bool,
        window_sec: f64,
        min_fills: u32,
    ) -> Self {
        self.spread_depth_participation_floor_enabled = enabled;
        self.spread_depth_participation_window_sec = window_sec.max(0.0);
        self.spread_depth_participation_min_fills = min_fills;
        self
    }

    /// Randomized touch-join vs passive mix from spread-depth hysteresis (reproducible draws). Requires [`spread_depth_regime_enabled`].
    pub fn with_spread_depth_probabilistic_touch(
        mut self,
        enabled: bool,
        p_good: f64,
        p_neutral: f64,
        p_bad: f64,
    ) -> Self {
        self.spread_depth_prob_touch_enabled = enabled;
        self.spread_depth_prob_touch_p_good = p_good;
        self.spread_depth_prob_touch_p_neutral = p_neutral;
        self.spread_depth_prob_touch_p_bad = p_bad;
        self
    }

    /// Tail brake: spread-capture EWMA vs mid; **extreme** only when very toxic. In extreme, keep touch-join with probability `touch_mult` (else `forced_passive_ticks` inside). Independent of Good/Bad/Neutral regime.
    pub fn with_spread_toxicity_brake(
        mut self,
        enabled: bool,
        ewma_alpha: f64,
        min_fills: u32,
        enter_bps: f64,
        exit_bps: f64,
        touch_mult: f64,
        forced_passive_ticks: u32,
    ) -> Self {
        self.spread_toxicity_brake_enabled = enabled;
        self.spread_toxicity_ewma_alpha = ewma_alpha.clamp(1e-9, 1.0);
        self.spread_toxicity_min_fills = min_fills;
        self.spread_toxicity_enter_bps = enter_bps;
        self.spread_toxicity_exit_bps = exit_bps;
        self.spread_toxicity_touch_mult = touch_mult.clamp(0.0, 1.0);
        self.spread_toxicity_forced_passive_ticks = forced_passive_ticks.min(6);
        self
    }

    pub fn with_dynamic_spread(mut self) -> Self {
        self.dynamic_spread_enabled = true;
        self
    }

    /// State-dependent multi-tick passive: combines impulse/imbalance (capped) with a soft vol add-on.
    /// Safe-side touch join only applies when computed depth is 0.
    /// Prefer `max_depth_ticks = 2` for participation; higher caps are easy to over-tighten in sim.
    pub fn with_state_dependent_multi_tick_passive(mut self, max_depth_ticks: u32) -> Self {
        self.state_passive_depth_enabled = true;
        self.state_passive_max_depth_ticks = max_depth_ticks.max(1);
        self
    }

    /// **Phase 3 (spread capture):** relax impulse / imbalance depth buckets so calm books stay at **`depth = 0`** more often; strong signals still step back. Requires state-dependent passive on.
    pub fn with_passive_depth_spread_capture_bias(mut self, enabled: bool) -> Self {
        self.passive_depth_spread_capture_bias = enabled;
        self
    }

    /// Mid-regime spread penalty: when |impulse| in [impulse_lo, impulse_hi] and live spread < tight_spread_bps, size *= size_mult. Isolated from Phase 1.
    pub fn with_mid_regime_spread_penalty(
        mut self,
        impulse_lo: f64,
        impulse_hi: f64,
        tight_spread_bps: f64,
        size_mult: f64,
    ) -> Self {
        self.mid_regime_spread_penalty_enabled = true;
        self.mid_regime_impulse_lo = impulse_lo;
        self.mid_regime_impulse_hi = impulse_hi;
        self.mid_regime_tight_spread_bps = tight_spread_bps;
        self.mid_regime_size_mult = size_mult.clamp(0.1, 1.0);
        self
    }

    /// Buckets: `0..=6` exact tick depth, `7` = 7+.
    pub fn passive_depth_histogram(&self) -> &[u64; 8] {
        &self.passive_depth_hist
    }

    pub fn reset_passive_depth_histogram(&mut self) {
        self.passive_depth_hist = [0; 8];
    }

    pub fn with_refresh(mut self, sec: f64) -> Self {
        self.order_refresh_sec = sec;
        self
    }

    pub fn with_warmup(mut self, sec: f64) -> Self {
        self.warmup_sec = sec;
        self
    }

    /// Reduce cancel/repost churn: hold last prices until `min_hold_sec` elapses, then ignore moves
    /// smaller than `reprice_hysteresis_ticks`. Requires `min_hold_sec > 0` and/or `reprice_hysteresis_ticks > 0`.
    pub fn with_quote_stickiness(mut self, min_hold_sec: f64, reprice_hysteresis_ticks: u32) -> Self {
        self.quote_stickiness_price_enabled = true;
        self.quote_stickiness_min_hold_sec = min_hold_sec.max(0.0);
        self.quote_stickiness_hysteresis_ticks = reprice_hysteresis_ticks;
        self
    }

    /// Only update passive depth when it moves by at least this many ticks (reduces depth flicker).
    pub fn with_quote_stickiness_depth(mut self, min_delta_ticks: u32) -> Self {
        self.quote_stickiness_depth_min_delta_ticks = min_delta_ticks;
        self
    }

    /// Scale resting clip from **fill-time** net-edge proxy (spread vs last mid + `maker_fee_bps`) and fill rate.
    /// Edge deterioration **cuts** size first; size **up** only when edge is healthy and flow is weak vs a slow reference.
    pub fn with_dynamic_order_sizing(
        mut self,
        clip_min: Decimal,
        clip_max: Decimal,
        maker_fee_bps: f64,
        step_up: f64,
        step_down: f64,
        cooldown_sec: f64,
        edge_alpha_fast: f64,
        edge_alpha_slow: f64,
        edge_deadband_bps: f64,
        bad_edge_streak: u32,
        flow_window_sec: f64,
        flow_low_ratio: f64,
        flow_ref_alpha: f64,
        min_fills_before_resize: u32,
        flow_ref_min_for_step_up: f64,
    ) -> Self {
        self.dynamic_sizing_enabled = true;
        self.dynamic_clip_min = clip_min;
        // Spot: bids need inventory + bid ≤ inventory_limit; clip cannot exceed limit at flat inv.
        self.dynamic_clip_max = clip_max
            .max(clip_min)
            .min(self.inventory_limit);
        self.dynamic_flow_ref_min_for_step_up = flow_ref_min_for_step_up.max(0.0);
        self.dynamic_maker_fee_bps = maker_fee_bps;
        self.dynamic_step_up = step_up.max(1.000_001);
        self.dynamic_step_down = step_down.clamp(0.01, 0.999_999);
        self.dynamic_cooldown_sec = cooldown_sec.max(0.0);
        self.dynamic_edge_alpha_fast = edge_alpha_fast.clamp(1e-9, 1.0);
        self.dynamic_edge_alpha_slow = edge_alpha_slow.clamp(1e-9, 1.0);
        self.dynamic_edge_deadband_bps = edge_deadband_bps.max(0.0);
        self.dynamic_bad_edge_streak = bad_edge_streak.max(1);
        self.dynamic_flow_window_sec = flow_window_sec.max(1.0);
        self.dynamic_flow_low_ratio = flow_low_ratio.clamp(0.01, 0.99);
        self.dynamic_flow_ref_alpha = flow_ref_alpha.clamp(1e-9, 1.0);
        self.dynamic_min_fills_before_resize = min_fills_before_resize.max(1);
        self.dynamic_current_clip = self
            .order_amount
            .max(self.dynamic_clip_min)
            .min(self.dynamic_clip_max);
        self
    }

    /// Resting size before Phase-1 / mid-regime multipliers (`order_amount` when dynamic sizing is off).
    pub fn effective_base_clip(&self) -> Decimal {
        if self.dynamic_sizing_enabled {
            self.dynamic_current_clip
        } else {
            self.order_amount
        }
    }

    /// Effective max resting clip (≤ YAML `dynamic_order_max`; spot profiles cap at `inventory_limit`).
    pub fn dynamic_clip_cap(&self) -> Decimal {
        self.dynamic_clip_max
    }

    fn clamp_dynamic_current_clip(&mut self) {
        self.dynamic_current_clip = self
            .dynamic_current_clip
            .max(self.dynamic_clip_min)
            .min(self.dynamic_clip_max);
    }

    fn maybe_apply_dynamic_order_resize(&mut self, ts: f64) {
        let window = self.dynamic_flow_window_sec;
        while let Some(&front) = self.dynamic_fill_ts.front() {
            if ts - front > window {
                self.dynamic_fill_ts.pop_front();
            } else {
                break;
            }
        }
        let n = self.dynamic_fill_ts.len() as f64;
        let flow_inst = if window > 1e-9 {
            n / window
        } else {
            0.0
        };

        let fa = self.dynamic_flow_ref_alpha;
        self.dynamic_flow_ref = Some(match self.dynamic_flow_ref {
            Some(prev) => fa * flow_inst + (1.0 - fa) * prev,
            None => flow_inst,
        });

        if self.dynamic_last_resize_ts.is_finite()
            && ts - self.dynamic_last_resize_ts < self.dynamic_cooldown_sec
        {
            return;
        }
        if self.dynamic_fill_count < self.dynamic_min_fills_before_resize as u64 {
            return;
        }

        let (Some(ew), Some(er)) = (self.dynamic_edge_ewma, self.dynamic_edge_ref) else {
            return;
        };

        let deteriorating = ew < er - self.dynamic_edge_deadband_bps;
        if deteriorating {
            self.dynamic_bad_streak += 1;
        } else {
            self.dynamic_bad_streak = 0;
        }

        if self.dynamic_bad_streak >= self.dynamic_bad_edge_streak {
            let cur = self.dynamic_current_clip.to_f64().unwrap_or(0.0);
            let mn = self.dynamic_clip_min.to_f64().unwrap_or(0.0);
            let mx = self.dynamic_clip_max.to_f64().unwrap_or(cur);
            let next = (cur * self.dynamic_step_down).clamp(mn, mx);
            self.dynamic_current_clip =
                Decimal::from_f64_retain(next).unwrap_or(self.dynamic_current_clip);
            self.clamp_dynamic_current_clip();
            self.dynamic_bad_streak = 0;
            self.dynamic_last_resize_ts = ts;
            self.dynamic_resize_down += 1;
            return;
        }

        if !deteriorating {
            let fr = self.dynamic_flow_ref.unwrap_or(flow_inst);
            if fr >= self.dynamic_flow_ref_min_for_step_up
                && flow_inst < fr * self.dynamic_flow_low_ratio
            {
                let cur = self.dynamic_current_clip.to_f64().unwrap_or(0.0);
                let mn = self.dynamic_clip_min.to_f64().unwrap_or(0.0);
                let mx = self.dynamic_clip_max.to_f64().unwrap_or(cur);
                let next = (cur * self.dynamic_step_up).clamp(mn, mx);
                self.dynamic_current_clip =
                    Decimal::from_f64_retain(next).unwrap_or(self.dynamic_current_clip);
                self.clamp_dynamic_current_clip();
                self.dynamic_last_resize_ts = ts;
                self.dynamic_resize_up += 1;
            }
        }
    }

    // --- Helpers ---

    fn clear_quote_stickiness_state(&mut self) {
        self.last_sticky_bid = None;
        self.last_sticky_ask = None;
        self.last_sticky_ask_amount = None;
        self.last_sticky_depth_ticks = None;
        self.last_sticky_reprice_ts = 0.0;
    }

    /// Integer tick distance |a − b| / tick_size (floor).
    fn tick_distance(&self, a: Decimal, b: Decimal) -> u32 {
        if self.tick_size <= Decimal::ZERO {
            return 0;
        }
        let ticks = ((a - b).abs() / self.tick_size).floor();
        let f = ticks.to_f64().unwrap_or(0.0);
        if f <= 0.0 {
            0
        } else if f >= u32::MAX as f64 {
            u32::MAX
        } else {
            f as u32
        }
    }

    /// Apply min-hold (global) then hysteresis vs last emitted price.
    fn apply_price_stickiness_one_side(
        &self,
        ts: f64,
        candidate: Decimal,
        last_emitted: Option<Decimal>,
    ) -> (Decimal, bool, bool) {
        let Some(last) = last_emitted else {
            return (candidate, false, false);
        };

        if self.quote_stickiness_min_hold_sec > 0.0
            && self.last_sticky_reprice_ts > 0.0
            && ts - self.last_sticky_reprice_ts < self.quote_stickiness_min_hold_sec
            && candidate != last
        {
            return (last, true, false);
        }

        if candidate == last {
            return (candidate, false, false);
        }

        if self.quote_stickiness_hysteresis_ticks > 0 {
            let dist = self.tick_distance(candidate, last);
            if dist < self.quote_stickiness_hysteresis_ticks {
                return (last, false, true);
            }
        }

        (candidate, false, false)
    }


    fn microprice(ob: &OrderBook) -> Option<Decimal> {
        let (bid, bid_qty) = ob.bids.first().map(|(p, q)| (*p, *q))?;
        let (ask, ask_qty) = ob.asks.first().map(|(p, q)| (*p, *q))?;
        let total = bid_qty + ask_qty;
        if total <= Decimal::ZERO {
            return Some((bid + ask) / dec!(2));
        }
        Some((bid * ask_qty + ask * bid_qty) / total)
    }

    fn book_imbalance(&self, ob: &OrderBook) -> f64 {
        let bid_qty: Decimal = ob.bids.iter().take(self.book_imbalance_levels).map(|(_, q)| *q).sum();
        let ask_qty: Decimal = ob.asks.iter().take(self.book_imbalance_levels).map(|(_, q)| *q).sum();
        let total = bid_qty + ask_qty;
        if total <= Decimal::ZERO {
            return 0.0;
        }
        // imbalance = bid_volume / (bid_volume + ask_volume) — 0 to 1
        (bid_qty / total).to_f64().unwrap_or(0.5)
    }

    fn volatility_factor(&self) -> f64 {
        if self.mid_history.len() < self.vol_lookback + 1 {
            return 1.0;
        }
        let mids: Vec<f64> = self.mid_history.iter().rev().take(self.vol_lookback + 1).copied().collect();
        let mut returns = Vec::with_capacity(self.vol_lookback);
        for i in 1..mids.len() {
            if mids[i - 1] > 0.0 {
                returns.push((mids[i] - mids[i - 1]) / mids[i - 1]);
            }
        }
        if returns.len() < 5 {
            return 1.0;
        }
        let mean: f64 = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance: f64 = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        let vol_bps = (variance.sqrt() * 10000.0).abs();
        if vol_bps <= self.vol_threshold_bps {
            1.0
        } else {
            let ratio = vol_bps / self.vol_threshold_bps;
            (1.0 + (ratio - 1.0) * 0.5).min(self.max_spread_multiplier)
        }
    }

    fn recent_microprice_impulse_bps(&self) -> Option<f64> {
        if self.microprice_impulse_lookback == 0
            || self.microprice_history.len() <= self.microprice_impulse_lookback
        {
            return None;
        }

        let mp_now = self.microprice_history.back().copied()?;
        let mp_old = self
            .microprice_history
            .get(self.microprice_history.len() - 1 - self.microprice_impulse_lookback)
            .copied()?;
        if mp_old <= 0.0 {
            return None;
        }

        Some((mp_now - mp_old) / mp_old * 10000.0)
    }

    fn recent_microprice_drift_bps(&self) -> Option<f64> {
        if self.microprice_drift_lookback == 0
            || self.microprice_history.len() <= self.microprice_drift_lookback
        {
            return None;
        }

        let mp_now = self.microprice_history.back().copied()?;
        let mp_old = self
            .microprice_history
            .get(self.microprice_history.len() - 1 - self.microprice_drift_lookback)
            .copied()?;
        if mp_old <= 0.0 {
            return None;
        }

        Some((mp_now - mp_old) / mp_old * 10000.0)
    }

    fn round_bid_to_tick(&self, price: Decimal) -> Decimal {
        if self.tick_size <= Decimal::ZERO {
            return price;
        }
        let ticks = (price / self.tick_size).floor();
        ticks * self.tick_size
    }

    fn round_ask_to_tick(&self, price: Decimal) -> Decimal {
        if self.tick_size <= Decimal::ZERO {
            return price;
        }
        let ticks = (price / self.tick_size).ceil();
        ticks * self.tick_size
    }

    fn cancel(side: OrderSide, layer: u32) -> OrderIntent {
        OrderIntent {
            side,
            price: Decimal::ZERO,
            amount: Decimal::ZERO,
            order_type: OrderType::Cancel,
            layer,
        }
    }

    fn should_join_touch(&self, side: OrderSide, ob: &OrderBook) -> bool {
        if self.queue_join_safe_side_max_touch_qty <= Decimal::ZERO {
            return true;
        }

        let touch_qty = match side {
            OrderSide::Buy => ob.bids.first().map(|(_, qty)| *qty),
            OrderSide::Sell => ob.asks.first().map(|(_, qty)| *qty),
        };

        touch_qty
            .map(|qty| qty <= self.queue_join_safe_side_max_touch_qty)
            .unwrap_or(false)
    }

    /// Same vol estimator as the high-vol kill switch (bps of per-snapshot returns).
    fn estimate_short_horizon_vol_bps(&self) -> Option<f64> {
        if self.mid_history.len() < self.vol_lookback + 1 {
            return None;
        }
        let mids: Vec<f64> = self
            .mid_history
            .iter()
            .rev()
            .take(self.vol_lookback + 1)
            .copied()
            .collect();
        let mut returns = Vec::with_capacity(self.vol_lookback);
        for i in 1..mids.len() {
            if mids[i - 1] > 0.0 {
                returns.push((mids[i] - mids[i - 1]) / mids[i - 1]);
            }
        }
        if returns.len() < 5 {
            return None;
        }
        let mean: f64 = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance: f64 = returns
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>()
            / returns.len() as f64;
        Some((variance.sqrt() * 10000.0).abs())
    }

    /// Depth in ticks to step back from the most aggressive passive quote (per side).
    fn passive_depth_ticks(
        &self,
        vol_bps: Option<f64>,
        impulse_bps: Option<f64>,
        imbalance: f64,
    ) -> u32 {
        if !self.state_passive_depth_enabled {
            return 0;
        }

        let mut d_imp = 0u32;
        if self.microprice_impulse_enabled {
            if let Some(ib) = impulse_bps {
                let t = self.microprice_impulse_threshold_bps;
                if t > 0.0 {
                    let f = ib.abs() / t;
                    let (lo, mid, hi) = if self.passive_depth_spread_capture_bias {
                        // Phase 3 / Layer 2 B1: wider mild-impulse band → more depth-0 before stepping back.
                        (0.45, 0.70, 0.90)
                    } else {
                        (0.33, 0.55, 0.75)
                    };
                    d_imp = if f < lo {
                        0
                    } else if f < mid {
                        1
                    } else if f < hi {
                        2
                    } else {
                        3
                    };
                }
            }
        }

        let mut d_vol = 0u32;
        if let Some(v) = vol_bps {
            let th = self.vol_threshold_bps.max(1e-9);
            let r = v / th;
            d_vol = if self.passive_depth_spread_capture_bias {
                // Slightly stricter: need more vol before leaving depth-0 contribution.
                if r <= 1.08 {
                    0
                } else if r <= 1.32 {
                    1
                } else if r <= 1.55 {
                    2
                } else {
                    3
                }
            } else if r <= 1.0 {
                0
            } else if r <= 1.25 {
                1
            } else if r <= 1.5 {
                2
            } else {
                3
            };
        }

        let dist = (imbalance - 0.5).abs();
        let (imb0, imb1) = if self.passive_depth_spread_capture_bias {
            (0.15, 0.24)
        } else {
            (0.12, 0.20)
        };
        let d_imb = if dist <= imb0 {
            0
        } else if dist <= imb1 {
            1
        } else {
            2
        };

        // Softer than max(d_imp, d_vol, d_imb): impulse+imbalance share one capped base; vol adds +1 only when clearly elevated.
        let base = d_imp.max(d_imb).min(2);
        let vol_boost: u32 = if d_vol >= 2 { 1 } else { 0 };
        (base + vol_boost).min(self.state_passive_max_depth_ticks)
    }

    /// Update [`spread_depth_hyst_state`] from [`spread_depth_regime_ewma`] (call once per quote refresh when warmed up).
    fn advance_spread_depth_hysteresis(&mut self) {
        if !self.spread_depth_regime_enabled {
            return;
        }
        let prev = self.spread_depth_hyst_state;
        if let Some(ewma) = self.spread_depth_regime_ewma {
            match self.spread_depth_hyst_state {
                0 => {
                    if ewma < self.spread_depth_bad_enter_bps {
                        self.spread_depth_hyst_state = 1;
                    } else if ewma > self.spread_depth_good_enter_bps {
                        self.spread_depth_hyst_state = 2;
                    }
                }
                1 => {
                    if ewma > self.spread_depth_bad_exit_bps {
                        self.spread_depth_hyst_state = 0;
                    }
                }
                2 => {
                    if ewma < self.spread_depth_good_exit_bps {
                        self.spread_depth_hyst_state = 0;
                    }
                }
                _ => self.spread_depth_hyst_state = 0,
            }
        }
        if prev != self.spread_depth_hyst_state {
            self.diag_spread_depth_regime_transitions += 1;
        }
        let idx = match self.spread_depth_hyst_state {
            1 => 1,
            2 => 2,
            _ => 0,
        };
        self.diag_spread_depth_regime_hist[idx] += 1;
    }

    /// Deterministic `U[0,1)` for probabilistic touch (same binary → same stream of draws).
    fn spread_depth_prob_touch_draw_u01(&mut self, ts: f64) -> f64 {
        self.spread_depth_prob_touch_u64_salt = self
            .spread_depth_prob_touch_u64_salt
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let x = self.spread_depth_prob_touch_u64_salt ^ (ts.to_bits() as u64);
        let x = x.wrapping_mul(0x2545F4914F6CDD1D);
        (x >> 11) as f64 / (1u64 << 53) as f64
    }

    fn spread_toxicity_draw_u01(&mut self, ts: f64) -> f64 {
        self.spread_toxicity_u64_salt = self
            .spread_toxicity_u64_salt
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let x = self.spread_toxicity_u64_salt ^ (ts.to_bits() as u64).rotate_left(17);
        let x = x.wrapping_mul(0x5851F42D4C957F2D);
        (x >> 11) as f64 / (1u64 << 53) as f64
    }

    fn advance_spread_toxicity_hysteresis(&mut self) {
        if !self.spread_toxicity_brake_enabled {
            return;
        }
        if let Some(ewma) = self.spread_toxicity_ewma {
            if self.spread_toxicity_extreme {
                if ewma > self.spread_toxicity_exit_bps {
                    self.spread_toxicity_extreme = false;
                }
            } else if ewma < self.spread_toxicity_enter_bps {
                self.spread_toxicity_extreme = true;
            }
        }
        if self.spread_toxicity_extreme {
            self.diag_spread_toxicity_extreme_refreshes += 1;
        }
    }

    fn advance_dynamic_conditional_touch_state(&mut self) {
        if !self.dynamic_conditional_touch_enabled {
            return;
        }
        if self.dynamic_conditional_touch_fill_count < self.dynamic_conditional_touch_min_fills as u64 {
            self.dynamic_conditional_touch_active = false;
            return;
        }
        if let Some(ewma) = self.dynamic_conditional_touch_ewma {
            let prev = self.dynamic_conditional_touch_active;
            if self.dynamic_conditional_touch_active {
                if ewma > self.dynamic_conditional_touch_exit_bps {
                    self.dynamic_conditional_touch_active = false;
                }
            } else if ewma < self.dynamic_conditional_touch_enter_bps {
                self.dynamic_conditional_touch_active = true;
            }
            if self.dynamic_conditional_touch_active != prev {
                self.diag_dynamic_conditional_touch_transitions += 1;
            }
        }
        if self.dynamic_conditional_touch_active {
            self.diag_dynamic_conditional_touch_refreshes += 1;
        }
    }

    /// Passive depth from spread regime, capped by [`state_passive_max_depth_ticks`]. [`None`] = feature off → use heuristic [`passive_depth_ticks`].
    fn spread_regime_depth_ticks(&mut self, now: f64) -> Option<u32> {
        if !self.spread_depth_regime_enabled {
            return None;
        }
        let cap = self.state_passive_max_depth_ticks.max(1);
        let mut d = if self.spread_depth_regime_fill_count < self.spread_depth_regime_min_fills as u64 {
            self.spread_depth_ticks_neutral.min(cap)
        } else {
            match self.spread_depth_hyst_state {
                1 => self.spread_depth_ticks_bad,
                2 => self.spread_depth_ticks_good,
                _ => self.spread_depth_ticks_neutral,
            }
            .min(cap)
        };

        if self.spread_depth_participation_floor_enabled {
            let w = self.spread_depth_participation_window_sec;
            if w > 0.0 {
                let t0 = now - w;
                while let Some(&front) = self.spread_depth_participation_fill_ts.front() {
                    if front < t0 {
                        self.spread_depth_participation_fill_ts.pop_front();
                    } else {
                        break;
                    }
                }
                let n = self.spread_depth_participation_fill_ts.len() as u32;
                if n < self.spread_depth_participation_min_fills && d > 0 {
                    d -= 1;
                    self.diag_spread_depth_participation_floor_pulls += 1;
                }
            }
        }

        Some(d)
    }

    /// Apply multi-tick passive: lower bid / raise ask from aggressive passive anchors.
    fn clamp_prices_with_passive_depth(
        &self,
        aggressive_bid: Decimal,
        aggressive_ask: Decimal,
        depth_ticks: u32,
    ) -> Option<(Decimal, Decimal)> {
        if self.tick_size <= Decimal::ZERO {
            return Some((aggressive_bid, aggressive_ask));
        }
        let mut d = depth_ticks;
        loop {
            let off = self.tick_size * Decimal::from(d);
            let bid = self.round_bid_to_tick(aggressive_bid - off);
            let ask = self.round_ask_to_tick(aggressive_ask + off);
            if bid < ask || d == 0 {
                return Some((bid, ask));
            }
            d -= 1;
        }
    }

    /// Bid vs ask passive step-back in ticks from combined `base` depth (microprice vs mid).
    fn split_asymmetric_depth_ticks(
        &self,
        base: u32,
        fair_price: Decimal,
        mid: Decimal,
        toxicity_forced_passive: bool,
    ) -> (u32, u32) {
        if !self.asymmetric_passive_depth_enabled
            || !(self.state_passive_depth_enabled
                || self.spread_depth_regime_enabled
                || (self.spread_toxicity_brake_enabled && toxicity_forced_passive))
        {
            return (base, base);
        }
        let cap = self.state_passive_max_depth_ticks;
        if fair_price > mid {
            // Bullish microprice: bid closer, ask further.
            let bid_d = base.saturating_sub(1);
            let ask_d = (base + 1).min(cap);
            (bid_d, ask_d)
        } else if fair_price < mid {
            let bid_d = (base + 1).min(cap);
            let ask_d = base.saturating_sub(1);
            (bid_d, ask_d)
        } else {
            (base, base)
        }
    }

    /// Multi-tick passive with separate bid/ask depth; walk back if prices would cross.
    fn clamp_prices_asymmetric_passive_depth(
        &self,
        aggressive_bid: Decimal,
        aggressive_ask: Decimal,
        bid_depth_ticks: u32,
        ask_depth_ticks: u32,
    ) -> Option<(Decimal, Decimal)> {
        if self.tick_size <= Decimal::ZERO {
            return Some((aggressive_bid, aggressive_ask));
        }
        let mut bd = bid_depth_ticks;
        let mut da = ask_depth_ticks;
        loop {
            let bid = self.round_bid_to_tick(aggressive_bid - self.tick_size * Decimal::from(bd));
            let ask = self.round_ask_to_tick(aggressive_ask + self.tick_size * Decimal::from(da));
            if bid < ask || (bd == 0 && da == 0) {
                return Some((bid, ask));
            }
            if bd >= da && bd > 0 {
                bd -= 1;
            } else if da > 0 {
                da -= 1;
            } else if bd > 0 {
                bd -= 1;
            } else {
                return Some((bid, ask));
            }
        }
    }

    /// Phase 1 (`capitalscaling.md`): smooth multiplier then clamp to `[0.5, 1.3]`.
    const IMPULSE_PHASE1_EMA_PREV: f64 = 0.8;
    const IMPULSE_PHASE1_EMA_NEW: f64 = 0.2;
    const IMPULSE_PHASE1_CLAMP_MIN: f64 = 0.5;
    const IMPULSE_PHASE1_CLAMP_MAX: f64 = 1.3;

    fn impulse_phase1_raw_bucket_multiplier(&self, abs_impulse_bps: f64) -> f64 {
        if abs_impulse_bps < 0.30 {
            self.impulse_phase1_bucket_calm
        } else if abs_impulse_bps < 0.60 {
            1.0
        } else if abs_impulse_bps < 1.00 {
            self.impulse_phase1_bucket_mid
        } else {
            self.impulse_phase1_bucket_min
        }
    }

    fn impulse_phase1_clamp(mult: f64) -> f64 {
        mult.clamp(
            Self::IMPULSE_PHASE1_CLAMP_MIN,
            Self::IMPULSE_PHASE1_CLAMP_MAX,
        )
    }

    /// Updates EMA when impulse is available; otherwise holds last smoothed value (or 1.0).
    fn impulse_phase1_effective_multiplier(
        &mut self,
        signed_microprice_impulse_bps: Option<f64>,
        warmed_up: bool,
    ) -> f64 {
        if !self.impulse_phase1_sizing_enabled || !warmed_up {
            return 1.0;
        }
        let Some(ib) = signed_microprice_impulse_bps else {
            return self.impulse_phase1_sizing_smoothed.unwrap_or(1.0);
        };
        let abs_ib = ib.abs();
        let clamped_raw = Self::impulse_phase1_clamp(self.impulse_phase1_raw_bucket_multiplier(abs_ib));
        let blended = if let Some(prev) = self.impulse_phase1_sizing_smoothed {
            Self::IMPULSE_PHASE1_EMA_PREV * prev + Self::IMPULSE_PHASE1_EMA_NEW * clamped_raw
        } else {
            clamped_raw
        };
        let next = Self::impulse_phase1_clamp(blended);
        self.impulse_phase1_sizing_smoothed = Some(next);
        self.impulse_phase1_multiplier_samples.push_back(next);
        if self.impulse_phase1_multiplier_samples.len() > 50_000 {
            self.impulse_phase1_multiplier_samples.pop_front();
        }
        next
    }

    fn record_directional_skew_sample(&mut self, signal_ticks: f64) {
        self.diag_dir_skew_refresh_samples += 1;
        let a = signal_ticks.abs();
        if a < self.directional_signal_threshold_ticks {
            self.diag_dir_skew_in_deadband += 1;
        }
        let idx = if a < 0.15 {
            0
        } else if a < 0.5 {
            1
        } else if a < 1.0 {
            2
        } else if a < 2.0 {
            3
        } else {
            4
        };
        self.diag_dir_skew_abs_hist[idx] += 1;
    }

    /// Linear ramp: 0 edge → `min_mult`, `edge >= k_bps` → `max_mult` (same bps definition as `microprice_edge_threshold`).
    pub(crate) fn microprice_edge_size_multiplier(&self, edge_bps_abs: f64) -> f64 {
        let k = self.microprice_edge_scale_k_bps.max(1e-9);
        let t = (edge_bps_abs / k).clamp(0.0, 1.0);
        let min_m = self.microprice_edge_scale_min_mult;
        let max_m = self.microprice_edge_scale_max_mult;
        (min_m + t * (max_m - min_m)).max(1e-9)
    }

    fn mp_tape_relaxation_blend(&self) -> f64 {
        if !self.microprice_edge_tape_regime_enabled {
            return 0.0;
        }
        mp_tape_relaxation_from_state(
            self.mp_tape_spread_ewma,
            self.mp_tape_regime_fill_count,
            self.microprice_edge_tape_min_fills,
            self.microprice_edge_tape_relax_low_spread_bps,
            self.microprice_edge_tape_relax_high_spread_bps,
        )
    }

    /// Mp-edge layer multiplier after optional tape-quality blend toward 1×.
    fn microprice_edge_effective_multiplier(&self, edge_bps_abs: f64) -> f64 {
        let raw = self.microprice_edge_size_multiplier(edge_bps_abs);
        let r = self.mp_tape_relaxation_blend();
        raw + r * (1.0 - raw)
    }

    /// Tape upside mult **\[1, max_mult]** from spread EMA; **1×** when disabled or cold start.
    fn tape_upside_multiplier(&self) -> f64 {
        if !self.microprice_edge_tape_upside_enabled {
            return 1.0;
        }
        if self.microprice_edge_tape_upside_use_regime_band {
            let r = mp_tape_relaxation_from_state(
                self.mp_tape_spread_ewma,
                self.mp_tape_regime_fill_count,
                self.microprice_edge_tape_upside_min_fills,
                self.microprice_edge_tape_relax_low_spread_bps,
                self.microprice_edge_tape_relax_high_spread_bps,
            );
            let max_m = self.microprice_edge_tape_upside_max_mult;
            (1.0 + r * (max_m - 1.0)).clamp(1.0, max_m)
        } else {
            mp_tape_upside_multiplier_from_state(
                self.mp_tape_spread_ewma,
                self.mp_tape_regime_fill_count,
                self.microprice_edge_tape_upside_min_fills,
                self.microprice_edge_tape_upside_low_spread_bps,
                self.microprice_edge_tape_upside_high_spread_bps,
                self.microprice_edge_tape_upside_max_mult,
            )
        }
    }

    /// Fade step count: fixed `microprice_fade_ticks` or 0/1/max from `|impulse|` buckets.
    fn effective_fade_ticks_from_impulse(&self, abs_impulse: f64) -> u32 {
        if !self.microprice_fade_impulse_bucketed {
            return self.microprice_fade_ticks;
        }
        if abs_impulse < self.microprice_fade_bucket_impulse_lo_bps {
            return 0;
        }
        if abs_impulse < self.microprice_fade_bucket_impulse_hi_bps {
            return 1;
        }
        self.microprice_fade_bucket_max_ticks.min(6)
    }
}

impl Strategy for RebateMMStrategy {
    fn name(&self) -> &str {
        "rebate_mm"
    }

    fn on_orderbook_update(
        &mut self,
        ob: &OrderBook,
        port: &Portfolio,
        ts: f64,
    ) -> Vec<OrderIntent> {
        let mut intents = Vec::new();

        let start_ts = *self.start_ts.get_or_insert(ts);
        let warmed_up = (ts - start_ts) >= self.warmup_sec;

        let fair_price = match Self::microprice(ob) {
            Some(p) => p,
            None => return intents,
        };

        let mid = ob.mid_price().unwrap_or(fair_price);
        let mid_f64 = mid.to_f64().unwrap_or(0.0);

        if mid_f64 > 0.0 {
            self.dynamic_last_mid = mid_f64;
        }
        if self.dynamic_sizing_enabled && mid_f64 > 0.0 && warmed_up {
            self.maybe_apply_dynamic_order_resize(ts);
        }

        // Update mid history for volatility
        if mid_f64 > 0.0 {
            self.mid_history.push_back(mid_f64);
            if self.mid_history.len() > self.vol_lookback + 50 {
                self.mid_history.pop_front();
            }
        }
        if let Some(mp_f64) = fair_price.to_f64() {
            if mp_f64 > 0.0 {
                self.microprice_history.push_back(mp_f64);
                let keep = self
                    .microprice_impulse_lookback
                    .max(self.microprice_drift_lookback)
                    .max(8)
                    + 16;
                if self.microprice_history.len() > keep {
                    self.microprice_history.pop_front();
                }
            }
        }

        let inventory = port.base_balance;

        // -----------------------------------------------------------------------
        // 1. Hedge: if abs(inventory) > hedge_limit, flatten with market order
        // -----------------------------------------------------------------------
        if warmed_up && ts >= self.hedge_cooldown_until {
            if inventory.abs() > self.hedge_limit {
                intents.push(Self::cancel(OrderSide::Buy, 1));
                intents.push(Self::cancel(OrderSide::Sell, 1));

                let (side, amount) = if inventory > Decimal::ZERO {
                    (OrderSide::Sell, inventory.abs())
                } else {
                    (OrderSide::Buy, inventory.abs())
                };

                if amount > Decimal::ZERO {
                    self.hedge_cooldown_until = ts + self.hedge_cooldown_sec;
                    intents.push(OrderIntent {
                        side,
                        price: mid,
                        amount,
                        order_type: OrderType::Market,
                        layer: 0,
                    });
                }
                self.clear_quote_stickiness_state();
                return intents;
            }
        }

        // -----------------------------------------------------------------------
        // 1B. Short-term impulse filter: if microprice moves too fast over the
        // configured lookback, cancel both sides and briefly pause quoting.
        // -----------------------------------------------------------------------
        let signed_microprice_impulse_bps = self.recent_microprice_impulse_bps();

        let mut impulse_kill_suppress_bid = false;
        let mut impulse_kill_suppress_ask = false;

        // 1B2. Hard impulse kill — symmetric: cancel both and return; asymmetric: suppress one side only (Phase 2);
        // hybrid (Run 3): symmetric in [threshold, extreme), one-sided for |imp| >= extreme.
        if self.impulse_kill_switch_enabled && warmed_up {
            if let Some(ib) = signed_microprice_impulse_bps {
                let abs_ib = ib.abs();
                let sym_t = self.impulse_kill_threshold_bps;

                if let Some(extreme) = self.impulse_kill_hybrid_extreme_bps {
                    if abs_ib >= extreme {
                        self.diag_impulse_kill_cancels += 1;
                        if ib >= extreme {
                            impulse_kill_suppress_ask = true;
                            self.last_sticky_ask = None;
                            self.last_sticky_ask_amount = None;
                        } else if ib <= -extreme {
                            impulse_kill_suppress_bid = true;
                            self.last_sticky_bid = None;
                        }
                    } else if abs_ib >= sym_t {
                        self.diag_impulse_kill_cancels += 1;
                        intents.push(Self::cancel(OrderSide::Buy, 1));
                        intents.push(Self::cancel(OrderSide::Sell, 1));
                        self.clear_quote_stickiness_state();
                        return intents;
                    }
                } else if self.impulse_kill_asymmetric {
                    if ib >= sym_t {
                        self.diag_impulse_kill_cancels += 1;
                        impulse_kill_suppress_ask = true;
                        self.last_sticky_ask = None;
                        self.last_sticky_ask_amount = None;
                    } else if ib <= -sym_t {
                        self.diag_impulse_kill_cancels += 1;
                        impulse_kill_suppress_bid = true;
                        self.last_sticky_bid = None;
                    }
                } else if abs_ib >= sym_t {
                    self.diag_impulse_kill_cancels += 1;
                    intents.push(Self::cancel(OrderSide::Buy, 1));
                    intents.push(Self::cancel(OrderSide::Sell, 1));
                    self.clear_quote_stickiness_state();
                    return intents;
                }
            }
        }

        if self.microprice_impulse_enabled && warmed_up {
            if ts < self.microprice_pause_until {
                intents.push(Self::cancel(OrderSide::Buy, 1));
                intents.push(Self::cancel(OrderSide::Sell, 1));
                self.clear_quote_stickiness_state();
                return intents;
            }

            if let Some(impulse_bps) = signed_microprice_impulse_bps {
                // Asymmetric impulse kill is already suppressing one side this snapshot — do not also
                // trigger symmetric microprice pause (would cancel both and undo Phase 2 behavior).
                if !impulse_kill_suppress_bid
                    && !impulse_kill_suppress_ask
                    && impulse_bps.abs() >= self.microprice_impulse_threshold_bps
                {
                    self.microprice_pause_until = ts + self.microprice_impulse_pause_sec;
                    intents.push(Self::cancel(OrderSide::Buy, 1));
                    intents.push(Self::cancel(OrderSide::Sell, 1));
                    self.clear_quote_stickiness_state();
                    return intents;
                }
            }
        }

        // -----------------------------------------------------------------------
        // 2. Volatility filter: if vol too high, stop quoting
        // -----------------------------------------------------------------------
        if let Some(vol_bps) = self.estimate_short_horizon_vol_bps() {
            if vol_bps > self.vol_threshold_bps * 2.0 {
                intents.push(Self::cancel(OrderSide::Buy, 1));
                intents.push(Self::cancel(OrderSide::Sell, 1));
                self.clear_quote_stickiness_state();
                return intents;
            }
        }

        // -----------------------------------------------------------------------
        // 3. Order book imbalance: if imbalance > 0.65, move quotes upward
        //    (imbalance = bid_vol/(bid+ask) — >0.65 means bid-heavy, lean up)
        //    Or pull quotes if extreme. Doc says "if imbalance > 0.65: move quotes upward"
        //    We'll pull quotes when heavily one-sided (>0.8) as adverse selection risk
        // -----------------------------------------------------------------------
        let imbalance = self.book_imbalance(ob);
        if imbalance > 0.80 || imbalance < 0.20 {
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));
            self.clear_quote_stickiness_state();
            return intents;
        }

        // -----------------------------------------------------------------------
        // 3B. Wide spread regime — no quotes when the book is unusually wide (toxic / unstable).
        // -----------------------------------------------------------------------
        if self.wide_spread_no_quotes_enabled && warmed_up {
            if let Some(sp_bps) = ob.spread_bps() {
                if sp_bps > self.wide_spread_max_bps {
                    self.diag_wide_spread_cancels += 1;
                    intents.push(Self::cancel(OrderSide::Buy, 1));
                    intents.push(Self::cancel(OrderSide::Sell, 1));
                    self.clear_quote_stickiness_state();
                    return intents;
                }
            }
        }

        // -----------------------------------------------------------------------
        // 3C. Microprice edge (Phase 4): quote only when |microprice−mid| is large enough (bps).
        // -----------------------------------------------------------------------
        if self.microprice_edge_threshold_bps > 0.0 && warmed_up && mid_f64 > 0.0 {
            if let Some(mp_f) = fair_price.to_f64() {
                let edge_bps = (mp_f - mid_f64) / mid_f64 * 10_000.0;
                if edge_bps.abs() < self.microprice_edge_threshold_bps {
                    intents.push(Self::cancel(OrderSide::Buy, 1));
                    intents.push(Self::cancel(OrderSide::Sell, 1));
                    self.clear_quote_stickiness_state();
                    return intents;
                }
            }
        }

        // -----------------------------------------------------------------------
        // 4. Throttle refreshes
        // -----------------------------------------------------------------------
        if ts - self.last_refresh_ts < self.order_refresh_sec {
            // Asymmetric kill: still pull suppressed side every snapshot so resting orders don't linger between refreshes.
            if impulse_kill_suppress_bid {
                intents.push(Self::cancel(OrderSide::Buy, 1));
            }
            if impulse_kill_suppress_ask {
                intents.push(Self::cancel(OrderSide::Sell, 1));
            }
            return intents;
        }
        self.last_refresh_ts = ts;

        // -----------------------------------------------------------------------
        // 5. Compute quotes
        // spread = base_spread * volatility_factor
        // inventory_adjustment = k * inventory (in bps, scaled by price)
        // bid = fair_price - spread/2 - inventory_adjustment
        // ask = fair_price + spread/2 - inventory_adjustment
        // (Stoikov: inv>0 pushes both down to favor selling)
        // -----------------------------------------------------------------------
        let vol_factor = self.volatility_factor();
        let mut spread_bps = self.base_spread_bps * vol_factor;

        // Normalize inventory skew by the configured inventory limit so a single
        // fill doesn't move quotes by tens of bps and cross the touch.
        let inv_dev = inventory - self.inventory_target;
        let inv_ratio = if self.inventory_limit > Decimal::ZERO {
            (inv_dev / self.inventory_limit)
                .to_f64()
                .unwrap_or(0.0)
                .clamp(-1.0, 1.0)
        } else {
            0.0
        };
        let adj_bps = self.inventory_skew_k * inv_ratio * self.base_spread_bps;
        let adj_dec = Decimal::from_f64_retain(adj_bps / 10000.0).unwrap_or(Decimal::ZERO);

        let best_bid = match ob.bids.first().map(|(p, _)| *p) {
            Some(p) => p,
            None => return intents,
        };
        let best_ask = match ob.asks.first().map(|(p, _)| *p) {
            Some(p) => p,
            None => return intents,
        };
        let live_spread = best_ask - best_bid;

        if self.dynamic_spread_enabled {
            let live_spread_bps = ob.spread_bps().unwrap_or(0.0);
            spread_bps = spread_bps.max(live_spread_bps);
        }

        let spread_half =
            Decimal::from_f64_retain(spread_bps / 2.0 / 10000.0).unwrap_or(dec!(0.00015));

        let desired_bid = fair_price * (dec!(1) - spread_half - adj_dec);
        let desired_ask = fair_price * (dec!(1) + spread_half - adj_dec);

        if self.spread_depth_regime_enabled && warmed_up {
            self.advance_spread_depth_hysteresis();
        }
        if self.spread_toxicity_brake_enabled && warmed_up {
            self.advance_spread_toxicity_hysteresis();
        }
        if self.dynamic_conditional_touch_enabled && warmed_up {
            self.advance_dynamic_conditional_touch_state();
        }
        let regime_depth_ticks = if warmed_up {
            self.spread_regime_depth_ticks(ts)
        } else {
            None
        };
        let mut regime_blocks_touch = regime_depth_ticks.is_some_and(|d| d > 0);
        let mut spread_depth_prob_touch_applied = false;
        if self.spread_depth_prob_touch_enabled
            && self.spread_depth_regime_enabled
            && warmed_up
            && self.spread_depth_regime_fill_count >= self.spread_depth_regime_min_fills as u64
        {
            spread_depth_prob_touch_applied = true;
            let p = match self.spread_depth_hyst_state {
                2 => self.spread_depth_prob_touch_p_good.clamp(0.0, 1.0),
                1 => self.spread_depth_prob_touch_p_bad.clamp(0.0, 1.0),
                _ => self.spread_depth_prob_touch_p_neutral.clamp(0.0, 1.0),
            };
            let u = self.spread_depth_prob_touch_draw_u01(ts);
            self.diag_spread_depth_prob_touch_rolls += 1;
            let try_touch = u < p;
            if try_touch {
                self.diag_spread_depth_prob_touch_chose_touch += 1;
            }
            regime_blocks_touch = !try_touch;
        }

        let abs_impulse_quotes = signed_microprice_impulse_bps.map(|x| x.abs()).unwrap_or(0.0);
        let abs_edge_for_touch = if mid_f64 > 0.0 {
            fair_price
                .to_f64()
                .map(|mp| ((mp - mid_f64) / mid_f64 * 10_000.0).abs())
                .unwrap_or(f64::INFINITY)
        } else {
            f64::INFINITY
        };
        let touch_brake_on = self.fill_touch_brake_enabled && ts < self.touch_brake_until_ts;
        let conditional_touch_active = self.conditional_touch_join_enabled
            && (!self.dynamic_conditional_touch_enabled || self.dynamic_conditional_touch_active);
        let conditional_touch_ok = !conditional_touch_active
            || (abs_impulse_quotes <= self.conditional_touch_max_impulse_bps
                && abs_edge_for_touch <= self.conditional_touch_max_edge_bps);
        let mut use_touch_join = self.queue_join_touch_enabled
            && conditional_touch_ok
            && !touch_brake_on
            && !regime_blocks_touch;

        let mut toxicity_forced_passive = false;
        if self.spread_toxicity_brake_enabled
            && warmed_up
            && self.spread_toxicity_fill_count >= self.spread_toxicity_min_fills as u64
            && self.spread_toxicity_extreme
            && use_touch_join
            && self.spread_toxicity_touch_mult < 1.0 - 1e-12
        {
            let u = self.spread_toxicity_draw_u01(ts);
            if u >= self.spread_toxicity_touch_mult {
                use_touch_join = false;
                toxicity_forced_passive = true;
                self.diag_spread_toxicity_touch_downgrades += 1;
            }
        }

        if self.queue_join_touch_enabled && !use_touch_join {
            self.diag_touch_join_fallback += 1;
        }

        let (aggressive_bid, aggressive_ask) = if use_touch_join {
            // Queue-aware mode: join the touch on active sides and let the
            // directional skew logic widen or suppress the opposite side.
            (best_bid, best_ask)
        } else {
            // Keep quotes passive against the live book. If spread is only 1 tick,
            // the only passive prices are the touch itself.
            let max_passive_bid = if live_spread > self.tick_size {
                best_ask - self.tick_size
            } else {
                best_bid
            };
            let min_passive_ask = if live_spread > self.tick_size {
                best_bid + self.tick_size
            } else {
                best_ask
            };

            (
                self.round_bid_to_tick(desired_bid.min(max_passive_bid)),
                self.round_ask_to_tick(desired_ask.max(min_passive_ask)),
            )
        };

        let cap_ticks = self.state_passive_max_depth_ticks.max(1);
        let raw_depth_ticks = if use_touch_join {
            0
        } else if toxicity_forced_passive {
            self.spread_toxicity_forced_passive_ticks.min(cap_ticks)
        } else if let Some(rd) = regime_depth_ticks {
            if spread_depth_prob_touch_applied && rd == 0 {
                1
            } else {
                rd
            }
        } else {
            self.passive_depth_ticks(
                self.estimate_short_horizon_vol_bps(),
                signed_microprice_impulse_bps,
                imbalance,
            )
        };
        let passive_depth_sample_path = self.state_passive_depth_enabled
            || self.spread_depth_regime_enabled
            || (self.spread_toxicity_brake_enabled && toxicity_forced_passive);

        let mut effective_depth_ticks = raw_depth_ticks;
        if self.quote_stickiness_depth_min_delta_ticks > 0
            && passive_depth_sample_path
            && !use_touch_join
        {
            if let Some(last_d) = self.last_sticky_depth_ticks {
                let jump = raw_depth_ticks.abs_diff(last_d);
                if jump < self.quote_stickiness_depth_min_delta_ticks {
                    effective_depth_ticks = last_d;
                    self.diag_sticky_depth_kept += 1;
                } else {
                    self.diag_sticky_depth_changed += 1;
                }
            } else {
                self.diag_sticky_depth_changed += 1;
            }
        }

        if passive_depth_sample_path && !use_touch_join {
            let idx = (effective_depth_ticks as usize).min(7);
            self.passive_depth_hist[idx] += 1;
        }

        let (bid_depth_ticks, ask_depth_ticks) = self.split_asymmetric_depth_ticks(
            effective_depth_ticks,
            fair_price,
            mid,
            toxicity_forced_passive,
        );

        let (mut bid_price, mut ask_price) = if !use_touch_join {
            if self.asymmetric_passive_depth_enabled
                && (self.state_passive_depth_enabled
                    || self.spread_depth_regime_enabled
                    || (self.spread_toxicity_brake_enabled && toxicity_forced_passive))
            {
                self.clamp_prices_asymmetric_passive_depth(
                    aggressive_bid,
                    aggressive_ask,
                    bid_depth_ticks,
                    ask_depth_ticks,
                )
                .unwrap_or((aggressive_bid, aggressive_ask))
            } else {
                self.clamp_prices_with_passive_depth(aggressive_bid, aggressive_ask, effective_depth_ticks)
                    .unwrap_or((aggressive_bid, aggressive_ask))
            }
        } else {
            (aggressive_bid, aggressive_ask)
        };
        let mut allow_bid = true;
        let mut allow_ask = true;
        if impulse_kill_suppress_bid {
            allow_bid = false;
        }
        if impulse_kill_suppress_ask {
            allow_ask = false;
        }

        if self.queue_join_safe_side_enabled && !use_touch_join {
            if let Some(impulse_bps) = signed_microprice_impulse_bps {
                if impulse_bps >= self.queue_join_safe_side_threshold_bps {
                    if bid_depth_ticks == 0 && self.should_join_touch(OrderSide::Buy, ob) {
                        bid_price = best_bid;
                    }
                } else if impulse_bps <= -self.queue_join_safe_side_threshold_bps {
                    if ask_depth_ticks == 0 && self.should_join_touch(OrderSide::Sell, ob) {
                        ask_price = best_ask;
                    }
                }
            }
        }

        if self.microprice_drift_enabled && warmed_up {
            if let Some(drift_bps) = self.recent_microprice_drift_bps() {
                if drift_bps >= self.microprice_drift_threshold_bps {
                    allow_ask = false;
                } else if drift_bps <= -self.microprice_drift_threshold_bps {
                    allow_bid = false;
                }
            }
        }

        if self.directional_skew_enabled && self.tick_size > Decimal::ZERO {
            let signal_ticks = ((fair_price - mid) / self.tick_size).to_f64().unwrap_or(0.0);
            self.record_directional_skew_sample(signal_ticks);
            let widen = self.tick_size * Decimal::from(self.directional_widen_ticks);

            if signal_ticks >= self.directional_signal_threshold_ticks {
                if widen > Decimal::ZERO {
                    self.diag_dir_skew_widen_ask += 1;
                    ask_price = self.round_ask_to_tick(ask_price + widen);
                }
                if signal_ticks >= self.directional_suppress_threshold_ticks {
                    self.diag_dir_skew_suppress_ask += 1;
                    allow_ask = false;
                }
            } else if signal_ticks <= -self.directional_signal_threshold_ticks {
                if widen > Decimal::ZERO {
                    self.diag_dir_skew_widen_bid += 1;
                    bid_price = self.round_bid_to_tick((bid_price - widen).max(Decimal::ZERO));
                }
                if signal_ticks <= -self.directional_suppress_threshold_ticks {
                    self.diag_dir_skew_suppress_bid += 1;
                    allow_bid = false;
                }
            }
        }

        // Microprice fade (Test 1): disadvantaged side → N ticks less aggressive (stay quoted, worse queue slot).
        if self.microprice_fade_enabled && warmed_up && self.tick_size > Decimal::ZERO {
            if let (Some(mp_f), Some(mid_dec)) = (fair_price.to_f64(), mid.to_f64()) {
                if mid_dec > 0.0 {
                    let abs_imp = signed_microprice_impulse_bps.map(|x| x.abs()).unwrap_or(0.0);
                    let impulse_ok = self.microprice_fade_impulse_min_bps <= 0.0
                        || abs_imp >= self.microprice_fade_impulse_min_bps;
                    let fade_ticks_eff = self.effective_fade_ticks_from_impulse(abs_imp);
                    if impulse_ok && fade_ticks_eff > 0 {
                        let edge_bps = (mp_f - mid_dec) / mid_dec * 10_000.0;
                        let min_e = self.microprice_fade_min_edge_bps;
                        let delta = self.tick_size * Decimal::from(fade_ticks_eff);
                        if allow_bid && edge_bps <= -min_e {
                            bid_price = self.round_bid_to_tick((bid_price - delta).max(Decimal::ZERO));
                            self.diag_microprice_fade_bid += 1;
                        }
                        if allow_ask && edge_bps >= min_e {
                            ask_price = self.round_ask_to_tick(ask_price + delta);
                            self.diag_microprice_fade_ask += 1;
                        }
                    }
                }
            }
        }

        // Sanity: don't cross. When spread is exactly 2 ticks, passive clamp maps both sides to the
        // same interior price (`best_ask - tick` == `best_bid + tick`). Without a nudge we cancel
        // every refresh → zero resting orders → zero fills (S3 backtest symptom despite healthy depth hist).
        if bid_price >= ask_price && self.tick_size > Decimal::ZERO {
            bid_price = self.round_bid_to_tick(ask_price - self.tick_size);
            if bid_price >= ask_price {
                ask_price = self.round_ask_to_tick(bid_price + self.tick_size);
            }
            if bid_price < ask_price {
                self.passive_pinch_recoveries += 1;
            }
        }
        if bid_price >= ask_price {
            self.passive_pinch_aborts += 1;
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));
            self.clear_quote_stickiness_state();
            return intents;
        }

        // Phase 1 capital scaling: global size mult from |impulse| (capitalscaling.md).
        let phase1_mult = self.impulse_phase1_effective_multiplier(
            signed_microprice_impulse_bps,
            warmed_up,
        );
        let base_clip = if self.dynamic_sizing_enabled {
            self.dynamic_current_clip
        } else {
            self.order_amount
        };
        let mut effective_base = base_clip * Decimal::from_f64_retain(phase1_mult).unwrap_or(dec!(1));

        if self.microprice_edge_size_scale_enabled && warmed_up && mid_f64 > 0.0 {
            if let Some(mp_f) = fair_price.to_f64() {
                let edge_bps = ((mp_f - mid_f64) / mid_f64 * 10_000.0).abs();
                let m = self.microprice_edge_effective_multiplier(edge_bps);
                effective_base *= Decimal::from_f64_retain(m).unwrap_or(dec!(1));
                self.microprice_edge_scale_samples.push_back(m);
                if self.microprice_edge_scale_samples.len() > 50_000 {
                    self.microprice_edge_scale_samples.pop_front();
                }
                if self.microprice_edge_tape_upside_enabled {
                    let u = self.tape_upside_multiplier();
                    effective_base *= Decimal::from_f64_retain(u).unwrap_or(dec!(1));
                    self.tape_upside_multiplier_samples.push_back(u);
                    if self.tape_upside_multiplier_samples.len() > 50_000 {
                        self.tape_upside_multiplier_samples.pop_front();
                    }
                }
            }
        }

        // Mid-regime spread penalty: reduce size when impulse in [lo, hi] and spread is tight (surgical bad-trade removal).
        if self.mid_regime_spread_penalty_enabled && warmed_up {
            let abs_imp = signed_microprice_impulse_bps.map(|ib| ib.abs()).unwrap_or(0.0);
            let live_spread_bps = ob.spread_bps().unwrap_or(f64::MAX);
            if abs_imp >= self.mid_regime_impulse_lo
                && abs_imp <= self.mid_regime_impulse_hi
                && live_spread_bps < self.mid_regime_tight_spread_bps
            {
                effective_base = effective_base
                    * Decimal::from_f64_retain(self.mid_regime_size_mult).unwrap_or(dec!(1));
            }
        }
        // Per-side size: optional impulse skew (Run 1) vs flat effective_base.
        let (bid_mult, ask_mult) = if self.impulse_size_skew_enabled && warmed_up {
            match signed_microprice_impulse_bps {
                Some(ib) if ib > 0.0 => (self.impulse_skew_favored, self.impulse_skew_lean),
                Some(ib) if ib < 0.0 => (self.impulse_skew_lean, self.impulse_skew_favored),
                _ => (1.0, 1.0),
            }
        } else {
            (1.0, 1.0)
        };
        let bid_amount = (effective_base * Decimal::from_f64_retain(bid_mult).unwrap_or(dec!(1)))
            .max(Decimal::ZERO);
        let ask_cap = effective_base * Decimal::from_f64_retain(ask_mult).unwrap_or(dec!(1));
        // Spot-only: never post asks larger than inventory on hand.
        let ask_amount = inventory.min(ask_cap).max(Decimal::ZERO);

        // Don't quote if we'd exceed inventory limit
        let inv_limit_exceeded_bid = (inventory + bid_amount) > self.inventory_limit;
        let inv_limit_exceeded_ask = ask_amount <= Decimal::ZERO;

        // --- Quote stickiness (prices): sit still so queue-depletion sim can fill us. ---
        if self.quote_stickiness_price_enabled && self.tick_size > Decimal::ZERO {
            let pre_sticky_bid = bid_price;
            let pre_sticky_ask = ask_price;
            let (nb, mh_b, hy_b) =
                self.apply_price_stickiness_one_side(ts, bid_price, self.last_sticky_bid);
            let (na, mh_a, hy_a) =
                self.apply_price_stickiness_one_side(ts, ask_price, self.last_sticky_ask);
            bid_price = nb;
            ask_price = na;
            if mh_b {
                self.diag_sticky_price_min_hold_clamps += 1;
            }
            if hy_b {
                self.diag_sticky_price_hysteresis_clamps += 1;
            }
            if mh_a {
                self.diag_sticky_price_min_hold_clamps += 1;
            }
            if hy_a {
                self.diag_sticky_price_hysteresis_clamps += 1;
            }
            if bid_price >= ask_price {
                bid_price = pre_sticky_bid;
                ask_price = pre_sticky_ask;
            }
        }

        let prev_bid = self.last_sticky_bid;
        let prev_ask = self.last_sticky_ask;
        let prev_ask_amt = self.last_sticky_ask_amount;
        let prev_depth = self.last_sticky_depth_ticks;

        let mut repriced_any = prev_depth != Some(effective_depth_ticks);

        if allow_bid && !inv_limit_exceeded_bid {
            if prev_bid == Some(bid_price) {
                self.diag_sticky_bid_kept += 1;
            } else {
                self.diag_sticky_bid_repriced += 1;
                repriced_any = true;
            }
            intents.push(OrderIntent {
                side: OrderSide::Buy,
                price: bid_price,
                amount: bid_amount,
                order_type: OrderType::Limit,
                layer: 1,
            });
        } else {
            intents.push(Self::cancel(OrderSide::Buy, 1));
            self.last_sticky_bid = None;
        }

        if allow_ask && !inv_limit_exceeded_ask {
            let ask_pair = Some((ask_price, ask_amount));
            let prev_pair = prev_ask.zip(prev_ask_amt);
            if prev_pair == ask_pair {
                self.diag_sticky_ask_kept += 1;
            } else {
                self.diag_sticky_ask_repriced += 1;
                repriced_any = true;
            }
            intents.push(OrderIntent {
                side: OrderSide::Sell,
                price: ask_price,
                amount: ask_amount,
                order_type: OrderType::Limit,
                layer: 1,
            });
        } else {
            intents.push(Self::cancel(OrderSide::Sell, 1));
            self.last_sticky_ask = None;
            self.last_sticky_ask_amount = None;
        }

        if allow_bid && !inv_limit_exceeded_bid {
            self.last_sticky_bid = Some(bid_price);
        }
        if allow_ask && !inv_limit_exceeded_ask {
            self.last_sticky_ask = Some(ask_price);
            self.last_sticky_ask_amount = Some(ask_amount);
        }
        self.last_sticky_depth_ticks = Some(effective_depth_ticks);
        if repriced_any {
            self.last_sticky_reprice_ts = ts;
        }

        intents
    }

    fn on_fill(&mut self, fill: &Fill, _port: &mut Portfolio, _ts: f64) {
        let mid = self.dynamic_last_mid;
        let px = fill.price.to_f64().unwrap_or(0.0);
        if (self.microprice_edge_tape_regime_enabled || self.microprice_edge_tape_upside_enabled)
            && mid > 0.0
            && px > 0.0
        {
            let spread_bps = match fill.side {
                OrderSide::Buy => (mid - px) / mid * 10_000.0,
                OrderSide::Sell => (px - mid) / mid * 10_000.0,
            };
            let a = self.microprice_edge_tape_spread_ewma_alpha;
            self.mp_tape_spread_ewma = Some(match self.mp_tape_spread_ewma {
                Some(prev) => a * spread_bps + (1.0 - a) * prev,
                None => spread_bps,
            });
            self.mp_tape_regime_fill_count += 1;
        }

        if self.spread_depth_regime_enabled && mid > 0.0 && px > 0.0 {
            let spread_bps_sd = match fill.side {
                OrderSide::Buy => (mid - px) / mid * 10_000.0,
                OrderSide::Sell => (px - mid) / mid * 10_000.0,
            };
            let a_sd = self.spread_depth_regime_ewma_alpha;
            self.spread_depth_regime_ewma = Some(match self.spread_depth_regime_ewma {
                Some(prev) => a_sd * spread_bps_sd + (1.0 - a_sd) * prev,
                None => spread_bps_sd,
            });
            self.spread_depth_regime_fill_count += 1;
        }

        if self.spread_toxicity_brake_enabled && mid > 0.0 && px > 0.0 {
            let spread_bps_t = match fill.side {
                OrderSide::Buy => (mid - px) / mid * 10_000.0,
                OrderSide::Sell => (px - mid) / mid * 10_000.0,
            };
            let a_t = self.spread_toxicity_ewma_alpha;
            self.spread_toxicity_ewma = Some(match self.spread_toxicity_ewma {
                Some(prev) => a_t * spread_bps_t + (1.0 - a_t) * prev,
                None => spread_bps_t,
            });
            self.spread_toxicity_fill_count += 1;
        }
        if self.dynamic_conditional_touch_enabled && mid > 0.0 && px > 0.0 {
            let spread_bps_t = match fill.side {
                OrderSide::Buy => (mid - px) / mid * 10_000.0,
                OrderSide::Sell => (px - mid) / mid * 10_000.0,
            };
            let a_dct = self.dynamic_conditional_touch_ewma_alpha;
            self.dynamic_conditional_touch_ewma = Some(match self.dynamic_conditional_touch_ewma {
                Some(prev) => a_dct * spread_bps_t + (1.0 - a_dct) * prev,
                None => spread_bps_t,
            });
            self.dynamic_conditional_touch_fill_count += 1;
        }
        if self.spread_depth_regime_enabled && self.spread_depth_participation_floor_enabled {
            self.spread_depth_participation_fill_ts.push_back(fill.timestamp);
            while self.spread_depth_participation_fill_ts.len() > 50_000 {
                self.spread_depth_participation_fill_ts.pop_front();
            }
        }

        if self.fill_touch_brake_enabled && mid > 0.0 && px > 0.0 {
            let spread_capture_bps = match fill.side {
                OrderSide::Buy => (mid - px) / mid * 10_000.0,
                OrderSide::Sell => (px - mid) / mid * 10_000.0,
            };
            if spread_capture_bps <= self.fill_touch_brake_bad_spread_bps {
                let until = fill.timestamp + self.fill_touch_brake_sec;
                self.touch_brake_until_ts = self.touch_brake_until_ts.max(until);
                self.diag_fill_touch_brake_triggers += 1;
            }
        }

        if !self.dynamic_sizing_enabled {
            return;
        }
        self.dynamic_fill_ts.push_back(fill.timestamp);
        while self.dynamic_fill_ts.len() > 50_000 {
            self.dynamic_fill_ts.pop_front();
        }
        self.dynamic_fill_count += 1;

        if mid <= 0.0 || px <= 0.0 {
            return;
        }
        let spread_bps = match fill.side {
            OrderSide::Buy => (mid - px) / mid * 10_000.0,
            OrderSide::Sell => (px - mid) / mid * 10_000.0,
        };
        let net_bps = spread_bps + self.dynamic_maker_fee_bps;

        let af = self.dynamic_edge_alpha_fast;
        self.dynamic_edge_ewma = Some(match self.dynamic_edge_ewma {
            Some(prev) => af * net_bps + (1.0 - af) * prev,
            None => net_bps,
        });
        let as_ = self.dynamic_edge_alpha_slow;
        self.dynamic_edge_ref = Some(match self.dynamic_edge_ref {
            Some(prev) => as_ * net_bps + (1.0 - as_) * prev,
            None => net_bps,
        });
    }

    fn gate_diagnostics(&self) -> Option<String> {
        let mut out = String::new();
        if self.impulse_kill_switch_enabled
            || self.wide_spread_no_quotes_enabled
            || self.asymmetric_passive_depth_enabled
            || self.impulse_size_skew_enabled
            || self.impulse_phase1_sizing_enabled
            || self.microprice_fade_enabled
            || self.spread_depth_regime_enabled
            || self.spread_toxicity_brake_enabled
        {
            out.push_str("--- Hard trade filters ---\n");
            if self.impulse_kill_switch_enabled {
                let mode = if let Some(ext) = self.impulse_kill_hybrid_extreme_bps {
                    format!(
                        "hybrid (|imp|∈[{:.2},{:.2}) flat both, |imp|≥{:.2} one-sided)",
                        self.impulse_kill_threshold_bps, ext, ext
                    )
                } else if self.impulse_kill_asymmetric {
                    "asymmetric (+→no ask, −→no bid)".to_string()
                } else {
                    "symmetric (|imp|)".to_string()
                };
                out.push_str(&format!(
                    "  impulse kill ({}): {} snapshot events (threshold {:.2} bps)\n",
                    mode, self.diag_impulse_kill_cancels, self.impulse_kill_threshold_bps
                ));
            }
            if self.impulse_size_skew_enabled {
                out.push_str(&format!(
                    "  impulse size skew: on (favored {:.2}× / lean {:.2}× vs baseline order_amount)\n",
                    self.impulse_skew_favored, self.impulse_skew_lean
                ));
            }
            if self.impulse_phase1_sizing_enabled {
                out.push_str(&format!(
                    "  impulse phase1 sizing: on (calm={:.2} mid={:.2} min={:.2}×, EMA, clamp [0.5,1.3])\n",
                    self.impulse_phase1_bucket_calm,
                    self.impulse_phase1_bucket_mid,
                    self.impulse_phase1_bucket_min,
                ));
                if !self.impulse_phase1_multiplier_samples.is_empty() {
                    let n = self.impulse_phase1_multiplier_samples.len();
                    let avg = self.impulse_phase1_multiplier_samples.iter().sum::<f64>() / n as f64;
                    let below_1 = self.impulse_phase1_multiplier_samples.iter().filter(|&&x| x < 1.0).count() as f64 / n as f64 * 100.0;
                    let mut sorted: Vec<f64> = self.impulse_phase1_multiplier_samples.iter().copied().collect();
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let p25 = sorted[((n as f64 * 0.25) as usize).min(n.saturating_sub(1))];
                    let p50 = sorted[((n as f64 * 0.50) as usize).min(n.saturating_sub(1))];
                    let p75 = sorted[((n as f64 * 0.75) as usize).min(n.saturating_sub(1))];
                    out.push_str(&format!(
                        "    avg_mult={:.3} p25={:.3} p50={:.3} p75={:.3} pct_below_1={:.1}% (n={})\n",
                        avg, p25, p50, p75, below_1, n
                    ));
                }
            }
            if self.wide_spread_no_quotes_enabled {
                out.push_str(&format!(
                    "  wide spread: {} snapshot cancels (spread_bps > {:.2} bps)\n",
                    self.diag_wide_spread_cancels, self.wide_spread_max_bps
                ));
            }
            if self.asymmetric_passive_depth_enabled {
                out.push_str("  asymmetric passive depth (bid vs ask): on\n");
            }
            if self.microprice_fade_enabled {
                let bucket_note = if self.microprice_fade_impulse_bucketed {
                    format!(
                        " bucketed lo={:.3} hi={:.3} max_ticks={}",
                        self.microprice_fade_bucket_impulse_lo_bps,
                        self.microprice_fade_bucket_impulse_hi_bps,
                        self.microprice_fade_bucket_max_ticks
                    )
                } else {
                    format!(" fade_ticks={}", self.microprice_fade_ticks)
                };
                out.push_str(&format!(
                    "  microprice fade: on (min_edge={:.4} bps imp_min={:.3} {} bid_pulls={} ask_pulls={})\n",
                    self.microprice_fade_min_edge_bps,
                    self.microprice_fade_impulse_min_bps,
                    bucket_note,
                    self.diag_microprice_fade_bid,
                    self.diag_microprice_fade_ask,
                ));
            }
            if self.conditional_touch_join_enabled && self.queue_join_touch_enabled {
                out.push_str(&format!(
                    "  conditional touch-join: |imp|≤{:.3} |fair−mid|≤{:.4} bps touch_fallback_refreshes={}\n",
                    self.conditional_touch_max_impulse_bps,
                    self.conditional_touch_max_edge_bps,
                    self.diag_touch_join_fallback,
                ));
                if self.dynamic_conditional_touch_enabled {
                    let ew = self
                        .dynamic_conditional_touch_ewma
                        .map(|x| format!("{:.4}", x))
                        .unwrap_or_else(|| "n/a".to_string());
                    out.push_str(&format!(
                        "    dynamic toggle: ewma={} bps fills={} min_fills={} active={} enter_when_ewma_lt={:.3} exit_when_ewma_gt={:.3} active_refreshes={} transitions={}\n",
                        ew,
                        self.dynamic_conditional_touch_fill_count,
                        self.dynamic_conditional_touch_min_fills,
                        self.dynamic_conditional_touch_active,
                        self.dynamic_conditional_touch_enter_bps,
                        self.dynamic_conditional_touch_exit_bps,
                        self.diag_dynamic_conditional_touch_refreshes,
                        self.diag_dynamic_conditional_touch_transitions,
                    ));
                }
            }
            if self.fill_touch_brake_enabled {
                out.push_str(&format!(
                    "  fill touch brake: spread≤{:.3} bps → no touch for {:.3}s (triggers={})\n",
                    self.fill_touch_brake_bad_spread_bps,
                    self.fill_touch_brake_sec,
                    self.diag_fill_touch_brake_triggers,
                ));
            }
            if self.spread_depth_regime_enabled {
                let ew = self
                    .spread_depth_regime_ewma
                    .map(|x| format!("{:.4}", x))
                    .unwrap_or_else(|| "n/a".to_string());
                out.push_str(&format!(
                    "  spread depth regime: ewma={} bps fills={} min_fills={} transitions={} samples Neutral/Bad/Good={}/{}/{}\n",
                    ew,
                    self.spread_depth_regime_fill_count,
                    self.spread_depth_regime_min_fills,
                    self.diag_spread_depth_regime_transitions,
                    self.diag_spread_depth_regime_hist[0],
                    self.diag_spread_depth_regime_hist[1],
                    self.diag_spread_depth_regime_hist[2],
                ));
                if self.spread_depth_participation_floor_enabled {
                    out.push_str(&format!(
                        "  spread depth participation floor: window={:.0}s min_fills_in_window={} depth_pulls={}\n",
                        self.spread_depth_participation_window_sec,
                        self.spread_depth_participation_min_fills,
                        self.diag_spread_depth_participation_floor_pulls,
                    ));
                }
                if self.spread_depth_prob_touch_enabled {
                    out.push_str(&format!(
                        "  spread depth probabilistic touch: p_good={:.2} p_neutral={:.2} p_bad={:.2} rolls={} chose_touch={}\n",
                        self.spread_depth_prob_touch_p_good,
                        self.spread_depth_prob_touch_p_neutral,
                        self.spread_depth_prob_touch_p_bad,
                        self.diag_spread_depth_prob_touch_rolls,
                        self.diag_spread_depth_prob_touch_chose_touch,
                    ));
                }
            }
            if self.spread_toxicity_brake_enabled {
                let ew = self
                    .spread_toxicity_ewma
                    .map(|x| format!("{:.4}", x))
                    .unwrap_or_else(|| "n/a".to_string());
                out.push_str(&format!(
                    "  spread toxicity brake: ewma={} bps fills={} min_fills={} extreme={} enter_when_ewma_lt={:.2} exit_when_ewma_gt={:.2} touch_mult={:.2} downgrades={} extreme_refreshes={}\n",
                    ew,
                    self.spread_toxicity_fill_count,
                    self.spread_toxicity_min_fills,
                    self.spread_toxicity_extreme,
                    self.spread_toxicity_enter_bps,
                    self.spread_toxicity_exit_bps,
                    self.spread_toxicity_touch_mult,
                    self.diag_spread_toxicity_touch_downgrades,
                    self.diag_spread_toxicity_extreme_refreshes,
                ));
            }
        }
        if self.state_passive_depth_enabled
            || self.spread_depth_regime_enabled
            || self.spread_toxicity_brake_enabled
        {
            out.push_str("--- Passive depth (quote refresh samples) ---\n");
            if self.passive_depth_spread_capture_bias {
                out.push_str("  spread-capture depth bias (Phase 3): on\n");
            }
            let total: u64 = self.passive_depth_hist.iter().sum();
            if total == 0 {
                out.push_str("  (no samples: no quote refresh reached passive-depth path)\n");
            } else {
                for i in 0..8 {
                    let n = self.passive_depth_hist[i];
                    let pct = 100.0 * (n as f64) / (total as f64);
                    if i == 7 {
                        out.push_str(&format!("  depth ≥7: {:5.1}% ({})\n", pct, n));
                    } else {
                        out.push_str(&format!("  depth {}: {:5.1}% ({})\n", i, pct, n));
                    }
                }
                out.push_str(&format!("  total: {}\n", total));
            }
        }
        out.push_str("--- Passive pinch (tight spread same-price) ---\n");
        out.push_str(&format!(
            "  recoveries (nudged bid/ask): {}\n",
            self.passive_pinch_recoveries
        ));
        out.push_str(&format!(
            "  aborts (still crossed):      {}\n",
            self.passive_pinch_aborts
        ));
        if self.dynamic_sizing_enabled {
            out.push_str("--- Dynamic order sizing ---\n");
            let ew = self
                .dynamic_edge_ewma
                .map(|x| format!("{:.3}", x))
                .unwrap_or_else(|| "n/a".to_string());
            let er = self
                .dynamic_edge_ref
                .map(|x| format!("{:.3}", x))
                .unwrap_or_else(|| "n/a".to_string());
            let fr = self
                .dynamic_flow_ref
                .map(|x| format!("{:.4}", x))
                .unwrap_or_else(|| "n/a".to_string());
            out.push_str(&format!(
                "  clip={} (min={} max={}) fills={} resize↑{} resize↓{}\n",
                self.dynamic_current_clip,
                self.dynamic_clip_min,
                self.dynamic_clip_max,
                self.dynamic_fill_count,
                self.dynamic_resize_up,
                self.dynamic_resize_down
            ));
            out.push_str(&format!(
                "  edge_ewma={} bps edge_ref={} bps flow_ref={} fills/s (window {:.0}s)\n",
                ew, er, fr, self.dynamic_flow_window_sec
            ));
        }
        if self.microprice_edge_size_scale_enabled {
            out.push_str("--- Microprice edge size scale (per quote refresh) ---\n");
            out.push_str(&format!(
                "  k_bps={:.4} min_mult={:.3} max_mult={:.3} (linear |microprice−mid| ramp)\n",
                self.microprice_edge_scale_k_bps,
                self.microprice_edge_scale_min_mult,
                self.microprice_edge_scale_max_mult,
            ));
            if self.microprice_edge_tape_regime_enabled {
                let ew = self
                    .mp_tape_spread_ewma
                    .map(|x| format!("{:.4}", x))
                    .unwrap_or_else(|| "n/a".to_string());
                let r = self.mp_tape_relaxation_blend();
                out.push_str(&format!(
                    "  tape regime: spread_ewma={} bps relax_blend={:.3} (fills {} min_fills {} low={:.4} high={:.4} α={:.4})\n",
                    ew,
                    r,
                    self.mp_tape_regime_fill_count,
                    self.microprice_edge_tape_min_fills,
                    self.microprice_edge_tape_relax_low_spread_bps,
                    self.microprice_edge_tape_relax_high_spread_bps,
                    self.microprice_edge_tape_spread_ewma_alpha,
                ));
            }
            if self.microprice_edge_tape_upside_enabled {
                let ew = self
                    .mp_tape_spread_ewma
                    .map(|x| format!("{:.4}", x))
                    .unwrap_or_else(|| "n/a".to_string());
                let u = self.tape_upside_multiplier();
                if self.microprice_edge_tape_upside_use_regime_band {
                    let r_up = mp_tape_relaxation_from_state(
                        self.mp_tape_spread_ewma,
                        self.mp_tape_regime_fill_count,
                        self.microprice_edge_tape_upside_min_fills,
                        self.microprice_edge_tape_relax_low_spread_bps,
                        self.microprice_edge_tape_relax_high_spread_bps,
                    );
                    out.push_str(&format!(
                        "  tape upside: regime_band mult={:.3} max={:.3} r={:.3} spread_ewma={} bps (fills {} min_fills {} map low={:.4} high={:.4} = tape relax)\n",
                        u,
                        self.microprice_edge_tape_upside_max_mult,
                        r_up,
                        ew,
                        self.mp_tape_regime_fill_count,
                        self.microprice_edge_tape_upside_min_fills,
                        self.microprice_edge_tape_relax_low_spread_bps,
                        self.microprice_edge_tape_relax_high_spread_bps,
                    ));
                } else {
                    out.push_str(&format!(
                        "  tape upside: absolute_band spread_ewma={} bps mult={:.3} max={:.3} fills {} min_fills {} low={:.4} high={:.4}\n",
                        ew,
                        u,
                        self.microprice_edge_tape_upside_max_mult,
                        self.mp_tape_regime_fill_count,
                        self.microprice_edge_tape_upside_min_fills,
                        self.microprice_edge_tape_upside_low_spread_bps,
                        self.microprice_edge_tape_upside_high_spread_bps,
                    ));
                }
                if !self.tape_upside_multiplier_samples.is_empty() {
                    let n = self.tape_upside_multiplier_samples.len();
                    let avg =
                        self.tape_upside_multiplier_samples.iter().sum::<f64>() / n as f64;
                    out.push_str(&format!("  tape upside mult avg={:.3} (n={})\n", avg, n));
                }
            }
            if !self.microprice_edge_scale_samples.is_empty() {
                let n = self.microprice_edge_scale_samples.len();
                let avg = self.microprice_edge_scale_samples.iter().sum::<f64>() / n as f64;
                let at_min = self
                    .microprice_edge_scale_samples
                    .iter()
                    .filter(|&&x| (x - self.microprice_edge_scale_min_mult).abs() < 1e-6)
                    .count() as f64
                    / n as f64
                    * 100.0;
                let mut sorted: Vec<f64> = self.microprice_edge_scale_samples.iter().copied().collect();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let p25 = sorted[((n as f64 * 0.25) as usize).min(n.saturating_sub(1))];
                let p50 = sorted[((n as f64 * 0.50) as usize).min(n.saturating_sub(1))];
                let p75 = sorted[((n as f64 * 0.75) as usize).min(n.saturating_sub(1))];
                out.push_str(&format!(
                    "  mult avg={:.3} p25={:.3} p50={:.3} p75={:.3} pct_at_min≈{:.1}% (n={})\n",
                    avg, p25, p50, p75, at_min, n
                ));
            }
        }
        if self.directional_skew_enabled {
            out.push_str("--- Directional skew (signal_ticks = (fair−mid)/tick at quote refresh) ---\n");
            out.push_str(&format!(
                "  signal_thresh={:.3} suppress_thresh={:.3} widen_ticks={}\n",
                self.directional_signal_threshold_ticks,
                self.directional_suppress_threshold_ticks,
                self.directional_widen_ticks
            ));
            let n = self.diag_dir_skew_refresh_samples;
            if n == 0 {
                out.push_str("  (no samples)\n");
            } else {
                let pct_dead =
                    100.0 * (self.diag_dir_skew_in_deadband as f64) / (n as f64);
                out.push_str(&format!(
                    "  samples: {} · in symmetric deadband (|sig| < signal_thresh): {:.1}% ({})\n",
                    n, pct_dead, self.diag_dir_skew_in_deadband
                ));
                let labels = [
                    "[0,0.15)",
                    "[0.15,0.5)",
                    "[0.5,1)",
                    "[1,2)",
                    "[2,∞)",
                ];
                for (i, lab) in labels.iter().enumerate() {
                    let c = self.diag_dir_skew_abs_hist[i];
                    let p = 100.0 * (c as f64) / (n as f64);
                    out.push_str(&format!("  |sig| {} {:5.1}% ({})\n", lab, p, c));
                }
                out.push_str(&format!(
                    "  widen ask/bid: {} / {} · suppress ask/bid: {} / {}\n",
                    self.diag_dir_skew_widen_ask,
                    self.diag_dir_skew_widen_bid,
                    self.diag_dir_skew_suppress_ask,
                    self.diag_dir_skew_suppress_bid
                ));
            }
        }
        if self.quote_stickiness_price_enabled || self.quote_stickiness_depth_min_delta_ticks > 0 {
            out.push_str("--- Quote stickiness (per refresh) ---\n");
            out.push_str(&format!(
                "  bid kept / repriced:  {} / {}\n",
                self.diag_sticky_bid_kept, self.diag_sticky_bid_repriced
            ));
            out.push_str(&format!(
                "  ask kept / repriced:  {} / {}\n",
                self.diag_sticky_ask_kept, self.diag_sticky_ask_repriced
            ));
            out.push_str(&format!(
                "  depth kept / changed: {} / {} (sticky depth delta {})\n",
                self.diag_sticky_depth_kept,
                self.diag_sticky_depth_changed,
                self.quote_stickiness_depth_min_delta_ticks
            ));
            out.push_str(&format!(
                "  price min-hold clamps: {}, hysteresis clamps: {}\n",
                self.diag_sticky_price_min_hold_clamps,
                self.diag_sticky_price_hysteresis_clamps
            ));
        }
        Some(out)
    }

    fn validate_config(&self) -> Result<(), StrategyError> {
        if self.order_amount <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig("order_amount must be > 0".into()));
        }
        if self.tick_size <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig("tick_size must be > 0".into()));
        }
        if self.inventory_limit <= Decimal::ZERO || self.hedge_limit <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "inventory_limit and hedge_limit must be > 0".into(),
            ));
        }
        if self.directional_signal_threshold_ticks < 0.0
            || self.directional_suppress_threshold_ticks < self.directional_signal_threshold_ticks
        {
            return Err(StrategyError::InvalidConfig(
                "directional skew thresholds must be non-negative and ordered".into(),
            ));
        }
        if self.microprice_impulse_enabled
            && (self.microprice_impulse_lookback == 0
                || self.microprice_impulse_threshold_bps <= 0.0
                || self.microprice_impulse_pause_sec <= 0.0)
        {
            return Err(StrategyError::InvalidConfig(
                "microprice impulse filter requires positive lookback, threshold, and pause".into(),
            ));
        }
        if self.microprice_drift_enabled
            && (self.microprice_drift_lookback == 0 || self.microprice_drift_threshold_bps <= 0.0)
        {
            return Err(StrategyError::InvalidConfig(
                "microprice drift filter requires positive lookback and threshold".into(),
            ));
        }
        if self.microprice_fade_enabled {
            if self.microprice_fade_min_edge_bps <= 0.0 {
                return Err(StrategyError::InvalidConfig(
                    "microprice_fade_min_edge_bps must be > 0 when microprice_fade_enabled".into(),
                ));
            }
            if self.microprice_fade_impulse_min_bps < 0.0 {
                return Err(StrategyError::InvalidConfig(
                    "microprice_fade_impulse_min_bps must be >= 0".into(),
                ));
            }
            if self.microprice_fade_impulse_bucketed {
                if self.microprice_fade_bucket_max_ticks == 0 {
                    return Err(StrategyError::InvalidConfig(
                        "microprice fade buckets require microprice_fade_bucket_max_ticks >= 1".into(),
                    ));
                }
                if self.microprice_fade_bucket_impulse_hi_bps
                    <= self.microprice_fade_bucket_impulse_lo_bps
                {
                    return Err(StrategyError::InvalidConfig(
                        "microprice fade buckets require bucket_impulse_hi_bps > bucket_impulse_lo_bps".into(),
                    ));
                }
            } else if self.microprice_fade_ticks == 0 {
                return Err(StrategyError::InvalidConfig(
                    "microprice_fade_ticks must be >= 1 when microprice_fade_enabled and not bucketed".into(),
                ));
            }
        }
        if self.conditional_touch_join_enabled {
            if self.conditional_touch_max_impulse_bps <= 0.0
                || self.conditional_touch_max_edge_bps <= 0.0
            {
                return Err(StrategyError::InvalidConfig(
                    "conditional touch-join requires positive max_impulse_bps and max_edge_bps".into(),
                ));
            }
        }
        if self.dynamic_conditional_touch_enabled {
            let a = self.dynamic_conditional_touch_ewma_alpha;
            if a <= 0.0 || a > 1.0 {
                return Err(StrategyError::InvalidConfig(
                    "dynamic_conditional_touch_ewma_alpha must be in (0, 1]".into(),
                ));
            }
            if !self.conditional_touch_join_enabled {
                return Err(StrategyError::InvalidConfig(
                    "dynamic_conditional_touch_enabled requires conditional_touch_join_enabled".into(),
                ));
            }
            if self.dynamic_conditional_touch_enter_bps >= self.dynamic_conditional_touch_exit_bps {
                return Err(StrategyError::InvalidConfig(
                    "dynamic conditional touch requires enter_bps < exit_bps (e.g. enter -0.30, exit -0.15)".into(),
                ));
            }
        }
        if self.fill_touch_brake_enabled && self.fill_touch_brake_sec <= 0.0 {
            return Err(StrategyError::InvalidConfig(
                "fill touch brake requires fill_touch_brake_sec > 0".into(),
            ));
        }
        if self.spread_depth_regime_enabled {
            let a = self.spread_depth_regime_ewma_alpha;
            if a <= 0.0 || a > 1.0 {
                return Err(StrategyError::InvalidConfig(
                    "spread_depth_regime_ewma_alpha must be in (0, 1]".into(),
                ));
            }
            if self.spread_depth_bad_enter_bps >= self.spread_depth_bad_exit_bps {
                return Err(StrategyError::InvalidConfig(
                    "spread depth regime requires bad_enter_bps < bad_exit_bps (e.g. enter -0.3, exit -0.2)".into(),
                ));
            }
            if self.spread_depth_good_exit_bps >= self.spread_depth_good_enter_bps {
                return Err(StrategyError::InvalidConfig(
                    "spread depth regime requires good_exit_bps < good_enter_bps".into(),
                ));
            }
            if self.state_passive_max_depth_ticks == 0 {
                return Err(StrategyError::InvalidConfig(
                    "spread_depth_regime_enabled requires state_passive_max_depth_ticks >= 1".into(),
                ));
            }
        }
        if self.spread_depth_participation_floor_enabled {
            if !self.spread_depth_regime_enabled {
                return Err(StrategyError::InvalidConfig(
                    "spread_depth_participation_floor_enabled requires spread_depth_regime_enabled".into(),
                ));
            }
            if self.spread_depth_participation_window_sec <= 0.0 {
                return Err(StrategyError::InvalidConfig(
                    "spread_depth_participation_window_sec must be > 0 when spread_depth_participation_floor_enabled".into(),
                ));
            }
        }
        if self.spread_depth_prob_touch_enabled {
            if !self.spread_depth_regime_enabled {
                return Err(StrategyError::InvalidConfig(
                    "spread_depth_prob_touch_enabled requires spread_depth_regime_enabled".into(),
                ));
            }
            for (name, p) in [
                ("spread_depth_prob_touch_p_good", self.spread_depth_prob_touch_p_good),
                (
                    "spread_depth_prob_touch_p_neutral",
                    self.spread_depth_prob_touch_p_neutral,
                ),
                ("spread_depth_prob_touch_p_bad", self.spread_depth_prob_touch_p_bad),
            ] {
                if !(0.0..=1.0).contains(&p) {
                    return Err(StrategyError::InvalidConfig(format!(
                        "{name} must be in [0, 1]"
                    )));
                }
            }
        }
        if self.spread_toxicity_brake_enabled {
            let a = self.spread_toxicity_ewma_alpha;
            if a <= 0.0 || a > 1.0 {
                return Err(StrategyError::InvalidConfig(
                    "spread_toxicity_ewma_alpha must be in (0, 1]".into(),
                ));
            }
            if self.spread_toxicity_enter_bps >= self.spread_toxicity_exit_bps {
                return Err(StrategyError::InvalidConfig(
                    "spread toxicity brake requires spread_toxicity_enter_bps < spread_toxicity_exit_bps (e.g. enter -0.7, exit -0.4)".into(),
                ));
            }
            if !(0.0..=1.0).contains(&self.spread_toxicity_touch_mult) {
                return Err(StrategyError::InvalidConfig(
                    "spread_toxicity_touch_mult must be in [0, 1]".into(),
                ));
            }
            if self.state_passive_max_depth_ticks == 0 {
                return Err(StrategyError::InvalidConfig(
                    "spread_toxicity_brake_enabled requires state_passive_max_depth_ticks >= 1".into(),
                ));
            }
            if self.spread_toxicity_forced_passive_ticks < 1 {
                return Err(StrategyError::InvalidConfig(
                    "spread_toxicity_forced_passive_ticks must be >= 1".into(),
                ));
            }
        }
        if self.microprice_edge_threshold_bps < 0.0 {
            return Err(StrategyError::InvalidConfig(
                "microprice_edge_threshold_bps must be >= 0".into(),
            ));
        }
        if self.microprice_edge_size_scale_enabled {
            if self.microprice_edge_scale_k_bps <= 0.0 {
                return Err(StrategyError::InvalidConfig(
                    "microprice_edge_scale_k_bps must be > 0 when microprice_edge_size_scale_enabled".into(),
                ));
            }
            if self.microprice_edge_scale_min_mult <= 0.0
                || self.microprice_edge_scale_max_mult < self.microprice_edge_scale_min_mult
            {
                return Err(StrategyError::InvalidConfig(
                    "microprice edge size scale requires 0 < min_mult <= max_mult".into(),
                ));
            }
        }
        if self.microprice_edge_tape_regime_enabled {
            if !self.microprice_edge_size_scale_enabled {
                return Err(StrategyError::InvalidConfig(
                    "microprice_edge_tape_regime_enabled requires microprice_edge_size_scale_enabled".into(),
                ));
            }
            let a = self.microprice_edge_tape_spread_ewma_alpha;
            if a <= 0.0 || a > 1.0 {
                return Err(StrategyError::InvalidConfig(
                    "microprice_edge_tape_spread_ewma_alpha must be in (0, 1]".into(),
                ));
            }
            if self.microprice_edge_tape_relax_high_spread_bps
                <= self.microprice_edge_tape_relax_low_spread_bps
            {
                return Err(StrategyError::InvalidConfig(
                    "tape regime requires microprice_edge_tape_relax_high_spread_bps > microprice_edge_tape_relax_low_spread_bps".into(),
                ));
            }
        }
        if self.microprice_edge_tape_upside_enabled {
            if !self.microprice_edge_size_scale_enabled {
                return Err(StrategyError::InvalidConfig(
                    "microprice_edge_tape_upside_enabled requires microprice_edge_size_scale_enabled".into(),
                ));
            }
            let max_u = self.microprice_edge_tape_upside_max_mult;
            if max_u <= 1.0 {
                return Err(StrategyError::InvalidConfig(
                    "tape upside requires microprice_edge_tape_upside_max_mult > 1.0".into(),
                ));
            }
            if max_u > 2.0 {
                return Err(StrategyError::InvalidConfig(
                    "tape upside microprice_edge_tape_upside_max_mult must be <= 2.0".into(),
                ));
            }
            if self.microprice_edge_tape_upside_use_regime_band {
                if self.microprice_edge_tape_relax_high_spread_bps
                    <= self.microprice_edge_tape_relax_low_spread_bps
                {
                    return Err(StrategyError::InvalidConfig(
                        "tape upside (regime_band) requires microprice_edge_tape_relax_high_spread_bps > relax_low_spread_bps".into(),
                    ));
                }
            } else if self.microprice_edge_tape_upside_high_spread_bps
                <= self.microprice_edge_tape_upside_low_spread_bps
            {
                return Err(StrategyError::InvalidConfig(
                    "tape upside (absolute_band) requires high_spread_bps > low_spread_bps".into(),
                ));
            }
            let a = self.microprice_edge_tape_spread_ewma_alpha;
            if a <= 0.0 || a > 1.0 {
                return Err(StrategyError::InvalidConfig(
                    "microprice_edge_tape_spread_ewma_alpha must be in (0, 1] when tape upside is enabled".into(),
                ));
            }
        }
        if self.queue_join_safe_side_enabled && self.queue_join_safe_side_threshold_bps <= 0.0 {
            return Err(StrategyError::InvalidConfig(
                "safe-side touch join requires a positive threshold".into(),
            ));
        }
        if self.queue_join_safe_side_max_touch_qty < Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "queue-aware safe-side touch join requires a non-negative max touch qty".into(),
            ));
        }
        if self.state_passive_depth_enabled && self.state_passive_max_depth_ticks == 0 {
            return Err(StrategyError::InvalidConfig(
                "state-dependent multi-tick passive requires max_depth_ticks >= 1".into(),
            ));
        }
        if self.quote_stickiness_price_enabled {
            if self.quote_stickiness_min_hold_sec < 0.0 {
                return Err(StrategyError::InvalidConfig(
                    "quote stickiness min_hold_sec must be >= 0".into(),
                ));
            }
            if self.quote_stickiness_min_hold_sec == 0.0
                && self.quote_stickiness_hysteresis_ticks == 0
            {
                return Err(StrategyError::InvalidConfig(
                    "quote stickiness requires min_hold_sec > 0 and/or hysteresis_ticks > 0".into(),
                ));
            }
        }
        if self.impulse_kill_asymmetric && !self.impulse_kill_switch_enabled {
            return Err(StrategyError::InvalidConfig(
                "impulse_kill_asymmetric requires impulse kill switch (with_impulse_kill_switch)".into(),
            ));
        }
        if self.impulse_kill_asymmetric && self.impulse_kill_hybrid_extreme_bps.is_some() {
            return Err(StrategyError::InvalidConfig(
                "impulse_kill_asymmetric and impulse_kill_hybrid_extreme_bps are mutually exclusive".into(),
            ));
        }
        if self.impulse_kill_switch_enabled {
            if self.impulse_kill_threshold_bps <= 0.0 {
                return Err(StrategyError::InvalidConfig(
                    "impulse kill switch requires impulse_kill_threshold_bps > 0".into(),
                ));
            }
            if self.microprice_impulse_lookback == 0 {
                return Err(StrategyError::InvalidConfig(
                    "impulse kill switch requires microprice impulse lookback > 0 (use with_microprice_impulse_filter)".into(),
                ));
            }
            if let Some(ext) = self.impulse_kill_hybrid_extreme_bps {
                if ext <= self.impulse_kill_threshold_bps {
                    return Err(StrategyError::InvalidConfig(
                        "impulse_kill_hybrid_extreme_bps must be > impulse_kill_threshold_bps".into(),
                    ));
                }
            }
        }
        if self.impulse_size_skew_enabled {
            if self.microprice_impulse_lookback == 0 {
                return Err(StrategyError::InvalidConfig(
                    "impulse size skew requires microprice impulse lookback > 0 (use with_microprice_impulse_filter)".into(),
                ));
            }
            if self.impulse_skew_favored <= 0.0 || self.impulse_skew_lean <= 0.0 {
                return Err(StrategyError::InvalidConfig(
                    "impulse size skew multipliers must be > 0".into(),
                ));
            }
        }
        if self.impulse_phase1_sizing_enabled {
            if self.microprice_impulse_lookback == 0 {
                return Err(StrategyError::InvalidConfig(
                    "impulse phase1 sizing requires microprice impulse lookback > 0 (use with_microprice_impulse_filter)".into(),
                ));
            }
        }
        if self.wide_spread_no_quotes_enabled && self.wide_spread_max_bps <= 0.0 {
            return Err(StrategyError::InvalidConfig(
                "wide spread filter requires wide_spread_max_bps > 0".into(),
            ));
        }
        if self.dynamic_sizing_enabled {
            if self.dynamic_clip_min <= Decimal::ZERO || self.dynamic_clip_max < self.dynamic_clip_min {
                return Err(StrategyError::InvalidConfig(
                    "dynamic_order_sizing requires dynamic_clip_min > 0 and dynamic_clip_max >= dynamic_clip_min".into(),
                ));
            }
            if self.dynamic_clip_min > self.inventory_limit {
                return Err(StrategyError::InvalidConfig(
                    "dynamic_order_sizing: dynamic_order_min must be <= inventory_limit (spot bid sizing)".into(),
                ));
            }
            if self.order_amount < self.dynamic_clip_min || self.order_amount > self.dynamic_clip_max {
                return Err(StrategyError::InvalidConfig(
                    "dynamic_order_sizing: order_amount must be within [dynamic_order_min, effective max (min of YAML max and inventory_limit)]".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
impl RebateMMStrategy {
    pub(crate) fn test_seed_spread_depth_regime(
        &mut self,
        ewma: Option<f64>,
        fill_count: u64,
        hyst_state: u8,
    ) {
        self.spread_depth_regime_ewma = ewma;
        self.spread_depth_regime_fill_count = fill_count;
        self.spread_depth_hyst_state = hyst_state;
    }

    pub(crate) fn advance_spread_depth_hysteresis_for_test(&mut self) {
        self.advance_spread_depth_hysteresis();
    }

    pub(crate) fn spread_depth_hyst_state_for_test(&self) -> u8 {
        self.spread_depth_hyst_state
    }

    pub(crate) fn spread_regime_depth_ticks_for_test(&mut self, now: f64) -> Option<u32> {
        self.spread_regime_depth_ticks(now)
    }

    pub(crate) fn test_push_participation_fill_ts(&mut self, ts: f64) {
        self.spread_depth_participation_fill_ts.push_back(ts);
    }

    pub(crate) fn test_seed_dynamic_conditional_touch(
        &mut self,
        ewma: Option<f64>,
        fill_count: u64,
        active: bool,
    ) {
        self.dynamic_conditional_touch_ewma = ewma;
        self.dynamic_conditional_touch_fill_count = fill_count;
        self.dynamic_conditional_touch_active = active;
    }

    pub(crate) fn advance_dynamic_conditional_touch_state_for_test(&mut self) {
        self.advance_dynamic_conditional_touch_state();
    }

    pub(crate) fn dynamic_conditional_touch_active_for_test(&self) -> bool {
        self.dynamic_conditional_touch_active
    }
}

#[cfg(test)]
mod tests {
    use super::{mp_tape_relaxation_from_state, mp_tape_upside_multiplier_from_state, RebateMMStrategy};
    use mm_core::market_data::{OrderBook, OrderSide};
    use mm_core::portfolio::Portfolio;
    use mm_core::strategy::{Fill, Strategy};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn book(ts: f64, best_bid: Decimal, bid_qty: Decimal, best_ask: Decimal, ask_qty: Decimal) -> OrderBook {
        OrderBook {
            bids: vec![(best_bid, bid_qty), (best_bid - dec!(0.01), bid_qty)],
            asks: vec![(best_ask, ask_qty), (best_ask + dec!(0.01), ask_qty)],
            timestamp: ts,
        }
    }

    fn buy_quote_price(intents: &[mm_core::strategy::OrderIntent]) -> Decimal {
        intents
            .iter()
            .find(|intent| intent.side == OrderSide::Buy && intent.amount > Decimal::ZERO)
            .map(|intent| intent.price)
            .expect("expected buy quote")
    }

    fn ask_quote_price(intents: &[mm_core::strategy::OrderIntent]) -> Decimal {
        intents
            .iter()
            .find(|intent| intent.side == OrderSide::Sell && intent.amount > Decimal::ZERO)
            .map(|intent| intent.price)
            .expect("expected ask quote")
    }

    fn has_live_quote(intents: &[mm_core::strategy::OrderIntent], side: OrderSide) -> bool {
        intents
            .iter()
            .any(|intent| intent.side == side && intent.amount > Decimal::ZERO)
    }

    #[test]
    fn spread_depth_regime_validate_rejects_bad_threshold_order() {
        let bad = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_state_dependent_multi_tick_passive(2)
            .with_spread_depth_regime(
                true,
                0.02,
                50,
                -0.2,
                -0.3,
                0.1,
                0.05,
                2,
                1,
                0,
            );
        assert!(bad.validate_config().is_err());
    }

    #[test]
    fn spread_depth_hysteresis_neutral_enters_bad_and_exits() {
        let mut s = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_state_dependent_multi_tick_passive(2)
            .with_spread_depth_regime(
                true,
                0.02,
                0,
                -0.3,
                -0.2,
                0.1,
                0.05,
                2,
                1,
                0,
            );
        s.test_seed_spread_depth_regime(Some(-0.35), 100, 0);
        s.advance_spread_depth_hysteresis_for_test();
        assert_eq!(s.spread_depth_hyst_state_for_test(), 1);
        s.test_seed_spread_depth_regime(Some(-0.15), 100, 1);
        s.advance_spread_depth_hysteresis_for_test();
        assert_eq!(s.spread_depth_hyst_state_for_test(), 0);
    }

    #[test]
    fn spread_depth_participation_floor_pulls_depth_when_starved() {
        let mut s = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_state_dependent_multi_tick_passive(2)
            .with_spread_depth_regime(
                true,
                0.02,
                0,
                -0.3,
                -0.2,
                0.1,
                0.05,
                2,
                1,
                0,
            )
            .with_spread_depth_participation_floor(true, 100.0, 2);
        assert!(s.validate_config().is_ok());
        s.test_seed_spread_depth_regime(Some(-0.5), 100, 1);
        assert_eq!(s.spread_regime_depth_ticks_for_test(1000.0), Some(1));
        s.test_push_participation_fill_ts(950.0);
        s.test_push_participation_fill_ts(960.0);
        assert_eq!(s.spread_regime_depth_ticks_for_test(1000.0), Some(2));
    }

    #[test]
    fn spread_depth_participation_floor_validate_requires_regime() {
        let bad = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_state_dependent_multi_tick_passive(2)
            .with_spread_depth_participation_floor(true, 300.0, 1);
        assert!(bad.validate_config().is_err());
    }

    #[test]
    fn spread_depth_prob_touch_validate_requires_regime() {
        let bad = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_state_dependent_multi_tick_passive(2)
            .with_spread_depth_probabilistic_touch(true, 0.85, 0.5, 0.05);
        assert!(bad.validate_config().is_err());
    }

    #[test]
    fn spread_toxicity_brake_validate_requires_enter_below_exit() {
        let bad = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_state_dependent_multi_tick_passive(2)
            .with_spread_toxicity_brake(true, 0.02, 50, -0.4, -0.7, 0.65, 1);
        assert!(bad.validate_config().is_err());
    }

    #[test]
    fn spread_toxicity_brake_validate_ok() {
        let s = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_state_dependent_multi_tick_passive(2)
            .with_spread_toxicity_brake(true, 0.02, 50, -0.7, -0.4, 0.65, 1);
        assert!(s.validate_config().is_ok());
    }

    #[test]
    fn dynamic_conditional_touch_validate_requires_enter_below_exit() {
        let bad = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_conditional_touch_join(true, 0.4, 0.012)
            .with_dynamic_conditional_touch(true, 0.02, 50, -0.15, -0.30);
        assert!(bad.validate_config().is_err());
    }

    #[test]
    fn dynamic_conditional_touch_enters_and_exits_with_hysteresis() {
        let mut s = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_conditional_touch_join(true, 0.4, 0.012)
            .with_dynamic_conditional_touch(true, 0.02, 50, -0.30, -0.15);
        s.test_seed_dynamic_conditional_touch(Some(-0.35), 100, false);
        s.advance_dynamic_conditional_touch_state_for_test();
        assert!(s.dynamic_conditional_touch_active_for_test());
        s.test_seed_dynamic_conditional_touch(Some(-0.10), 100, true);
        s.advance_dynamic_conditional_touch_state_for_test();
        assert!(!s.dynamic_conditional_touch_active_for_test());
    }

    #[test]
    fn spread_depth_prob_touch_validate_rejects_p_out_of_range() {
        let bad = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_state_dependent_multi_tick_passive(2)
            .with_spread_depth_regime(
                true,
                0.02,
                50,
                -0.3,
                -0.2,
                0.1,
                0.05,
                2,
                1,
                0,
            )
            .with_spread_depth_probabilistic_touch(true, 1.2, 0.5, 0.05);
        assert!(bad.validate_config().is_err());
    }

    #[test]
    fn microprice_edge_size_multiplier_linear_ramp() {
        let s = RebateMMStrategy::new(dec!(1.0), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_size_scale(true, 0.05, 0.3, 1.0);
        assert!(s.validate_config().is_ok());
        assert!((s.microprice_edge_size_multiplier(0.0) - 0.3).abs() < 1e-9);
        assert!((s.microprice_edge_size_multiplier(0.025) - 0.65).abs() < 1e-9);
        assert!((s.microprice_edge_size_multiplier(0.05) - 1.0).abs() < 1e-9);
        assert!((s.microprice_edge_size_multiplier(0.1) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn mp_tape_relaxation_from_state_mapping() {
        assert!((mp_tape_relaxation_from_state(Some(0.09), 100, 50, -0.02, 0.08) - 1.0).abs() < 1e-9);
        assert!((mp_tape_relaxation_from_state(Some(-0.02), 100, 50, -0.02, 0.08) - 0.0).abs() < 1e-9);
        assert!((mp_tape_relaxation_from_state(Some(0.03), 100, 50, -0.02, 0.08) - 0.5).abs() < 1e-9);
        assert!((mp_tape_relaxation_from_state(Some(0.03), 10, 50, -0.02, 0.08) - 0.0).abs() < 1e-9);
        assert!((mp_tape_relaxation_from_state(None, 100, 0, -0.02, 0.08) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn microprice_edge_tape_regime_config() {
        let bad = RebateMMStrategy::new(dec!(1.0), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_tape_regime(true, 0.02, -0.02, 0.08, 50);
        assert!(bad.validate_config().is_err());

        let ok = RebateMMStrategy::new(dec!(1.0), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_size_scale(true, 0.02, 0.3, 1.0)
            .with_microprice_edge_tape_regime(true, 0.02, -0.02, 0.08, 50);
        assert!(ok.validate_config().is_ok());
    }

    #[test]
    fn mp_tape_upside_multiplier_from_state_mapping() {
        assert!(
            (mp_tape_upside_multiplier_from_state(Some(0.10), 100, 50, 0.10, 0.20, 1.2) - 1.0).abs()
                < 1e-9
        );
        assert!(
            (mp_tape_upside_multiplier_from_state(Some(0.20), 100, 50, 0.10, 0.20, 1.2) - 1.2).abs()
                < 1e-9
        );
        assert!(
            (mp_tape_upside_multiplier_from_state(Some(0.15), 100, 50, 0.10, 0.20, 1.2) - 1.1).abs()
                < 1e-9
        );
        assert!(
            (mp_tape_upside_multiplier_from_state(Some(0.15), 10, 50, 0.10, 0.20, 1.2) - 1.0).abs()
                < 1e-9
        );
        assert!(
            (mp_tape_upside_multiplier_from_state(None, 100, 0, 0.10, 0.20, 1.2) - 1.0).abs() < 1e-9
        );
        assert!(
            (mp_tape_upside_multiplier_from_state(Some(0.50), 100, 50, 0.10, 0.20, 1.0) - 1.0).abs()
                < 1e-9
        );
    }

    #[test]
    fn microprice_edge_tape_upside_config() {
        let no_scale = RebateMMStrategy::new(dec!(1.0), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_tape_upside(true, 1.15, 0.10, 0.20, 50);
        assert!(no_scale.validate_config().is_err());

        let bad_max = RebateMMStrategy::new(dec!(1.0), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_size_scale(true, 0.02, 0.3, 1.0)
            .with_microprice_edge_tape_upside(true, 1.0, 0.10, 0.20, 50);
        assert!(bad_max.validate_config().is_err());

        let ok_regime_band = RebateMMStrategy::new(dec!(1.0), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_size_scale(true, 0.02, 0.3, 1.0)
            .with_microprice_edge_tape_upside(true, 1.2, 0.08, 0.18, 40);
        assert!(ok_regime_band.validate_config().is_ok());

        let ok_absolute = RebateMMStrategy::new(dec!(1.0), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_size_scale(true, 0.02, 0.3, 1.0)
            .with_microprice_edge_tape_upside(true, 1.2, 0.08, 0.18, 40)
            .with_microprice_edge_tape_upside_use_regime_band(false);
        assert!(ok_absolute.validate_config().is_ok());

        let bad_absolute_order = RebateMMStrategy::new(dec!(1.0), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_size_scale(true, 0.02, 0.3, 1.0)
            .with_microprice_edge_tape_upside(true, 1.2, 0.20, 0.10, 40)
            .with_microprice_edge_tape_upside_use_regime_band(false);
        assert!(bad_absolute_order.validate_config().is_err());

        let bad_relax_for_upside = RebateMMStrategy::new(dec!(1.0), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_size_scale(true, 0.02, 0.3, 1.0)
            .with_microprice_edge_tape_regime(false, 0.02, 0.06, 0.05, 50)
            .with_microprice_edge_tape_upside(true, 1.2, 0.10, 0.20, 40);
        assert!(bad_relax_for_upside.validate_config().is_err());
    }

    #[test]
    fn tape_upside_regime_band_aug_style_ewma_gives_boost() {
        let r = mp_tape_relaxation_from_state(Some(-0.0006), 65700, 50, -0.02, 0.08);
        assert!((r - 0.194).abs() < 0.002);
        let max_m = 1.15_f64;
        let u = (1.0 + r * (max_m - 1.0)).clamp(1.0, max_m);
        assert!((u - 1.0291).abs() < 0.002);
    }

    #[test]
    fn microprice_edge_size_scale_validates_k_and_bounds() {
        let bad_k = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_size_scale(true, 0.0, 0.3, 1.0);
        assert!(bad_k.validate_config().is_err());
        let bad_order = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_edge_size_scale(true, 0.05, 1.0, 0.3);
        assert!(bad_order.validate_config().is_err());
    }

    #[test]
    fn dynamic_order_sizing_clamps_initial_clip_into_min_max() {
        let s = RebateMMStrategy::new(dec!(0.45), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_dynamic_order_sizing(
                dec!(0.4),
                dec!(0.5),
                -0.75,
                1.1,
                0.9,
                60.0,
                0.15,
                0.02,
                0.15,
                5,
                120.0,
                0.85,
                0.05,
                1,
                0.005,
            );
        assert_eq!(s.effective_base_clip(), dec!(0.45));
        assert!(s.validate_config().is_ok());

        let t = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_dynamic_order_sizing(
                dec!(0.4),
                dec!(0.5),
                -0.75,
                1.1,
                0.9,
                60.0,
                0.15,
                0.02,
                0.15,
                5,
                120.0,
                0.85,
                0.05,
                1,
                0.005,
            );
        assert_eq!(t.effective_base_clip(), dec!(0.5));
        assert!(t.validate_config().is_ok());
    }

    #[test]
    fn dynamic_order_sizing_rejects_order_amount_outside_bounds() {
        let s = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_dynamic_order_sizing(
                dec!(0.6),
                dec!(2.0),
                -0.75,
                1.1,
                0.9,
                60.0,
                0.15,
                0.02,
                0.15,
                5,
                120.0,
                0.85,
                0.05,
                1,
                0.005,
            );
        assert!(s.validate_config().is_err());
    }

    #[test]
    fn dynamic_order_sizing_caps_clip_max_at_inventory_limit() {
        let s = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(0.75), dec!(0.75))
            .with_dynamic_order_sizing(
                dec!(0.1),
                dec!(2.0),
                -0.75,
                1.1,
                0.9,
                60.0,
                0.15,
                0.02,
                0.15,
                5,
                120.0,
                0.85,
                0.05,
                1,
                0.005,
            );
        assert_eq!(s.dynamic_clip_cap(), dec!(0.75));
        assert!(s.validate_config().is_ok());
    }

    #[test]
    fn queue_aware_touch_join_joins_when_touch_queue_is_small() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_impulse_filter(1, 1000.0, 1.0)
            .with_queue_aware_safe_side_touch_join(0.5, dec!(5.0))
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(Decimal::ZERO, dec!(100000.0));

        let _ = strategy.on_orderbook_update(
            &book(0.0, dec!(100.00), dec!(2.0), dec!(100.02), dec!(2.0)),
            &portfolio,
            0.0,
        );
        let intents = strategy.on_orderbook_update(
            &book(1.0, dec!(100.02), dec!(2.0), dec!(100.04), dec!(2.0)),
            &portfolio,
            1.0,
        );

        assert_eq!(buy_quote_price(&intents), dec!(100.02));
    }

    #[test]
    fn queue_aware_touch_join_stays_passive_when_touch_queue_is_deep() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_impulse_filter(1, 1000.0, 1.0)
            .with_queue_aware_safe_side_touch_join(0.5, dec!(5.0))
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(Decimal::ZERO, dec!(100000.0));

        let _ = strategy.on_orderbook_update(
            &book(0.0, dec!(100.00), dec!(2.0), dec!(100.02), dec!(2.0)),
            &portfolio,
            0.0,
        );
        let intents = strategy.on_orderbook_update(
            &book(1.0, dec!(100.02), dec!(6.0), dec!(100.04), dec!(4.0)),
            &portfolio,
            1.0,
        );

        assert!(buy_quote_price(&intents) < dec!(100.02));
    }

    #[test]
    fn microprice_fade_pulls_bid_one_tick_when_fair_below_mid_touch_join() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_queue_join_touch(true)
            .with_microprice_fade(true, 0.001, 1)
            .with_refresh(0.0)
            .with_warmup(0.0);
        assert!(strategy.validate_config().is_ok());
        let portfolio = Portfolio::new(dec!(1.0), dec!(100000.0));
        // TOB sizes → microprice < mid; deeper levels balance bid/ask notional so imbalance filter stays mid.
        let ob = OrderBook {
            bids: vec![(dec!(100.00), dec!(1.0)), (dec!(99.99), dec!(10.0))],
            asks: vec![(dec!(100.02), dec!(10.0)), (dec!(100.03), dec!(1.0))],
            timestamp: 0.0,
        };
        let intents = strategy.on_orderbook_update(&ob, &portfolio, 0.0);
        assert_eq!(buy_quote_price(&intents), dec!(99.99));
        assert_eq!(ask_quote_price(&intents), dec!(100.02));
    }

    #[test]
    fn microprice_fade_pulls_ask_one_tick_when_fair_above_mid_touch_join() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_queue_join_touch(true)
            .with_microprice_fade(true, 0.001, 1)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(dec!(1.0), dec!(100000.0));
        let ob = OrderBook {
            bids: vec![(dec!(100.00), dec!(10.0)), (dec!(99.99), dec!(1.0))],
            asks: vec![(dec!(100.02), dec!(1.0)), (dec!(100.03), dec!(10.0))],
            timestamp: 0.0,
        };
        let intents = strategy.on_orderbook_update(&ob, &portfolio, 0.0);
        assert_eq!(buy_quote_price(&intents), dec!(100.00));
        assert_eq!(ask_quote_price(&intents), dec!(100.03));
    }

    #[test]
    fn fill_touch_brake_forces_passive_until_expiry() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_queue_join_touch(true)
            .with_fill_touch_brake(true, -0.5, 0.2)
            .with_refresh(0.0)
            .with_warmup(0.0);
        assert!(strategy.validate_config().is_ok());
        let mut portfolio = Portfolio::new(dec!(1.0), dec!(100000.0));
        let ob = book(0.0, dec!(100.00), dec!(2.0), dec!(100.02), dec!(2.0));
        let _ = strategy.on_orderbook_update(&ob, &portfolio, 0.0);
        let fill = Fill {
            order_id: "t".into(),
            side: OrderSide::Sell,
            price: dec!(99.99),
            amount: dec!(0.1),
            timestamp: 0.0,
            layer: 1,
        };
        strategy.on_fill(&fill, &mut portfolio, 0.0);
        let intents_brake = strategy.on_orderbook_update(&ob, &portfolio, 0.05);
        assert!(
            buy_quote_price(&intents_brake) < dec!(100.00),
            "expected passive interior while brake active"
        );
        let intents_after = strategy.on_orderbook_update(&ob, &portfolio, 0.25);
        assert_eq!(
            buy_quote_price(&intents_after),
            dec!(100.00),
            "touch-join resumes after brake window"
        );
    }

    #[test]
    fn conditional_touch_join_drops_to_passive_on_large_impulse() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_impulse_filter(1, 1000.0, 1.0)
            .with_queue_join_touch(true)
            .with_conditional_touch_join(true, 5.0, 10.0)
            .with_refresh(0.0)
            .with_warmup(0.0);
        assert!(strategy.validate_config().is_ok());
        let portfolio = Portfolio::new(dec!(1.0), dec!(100000.0));
        let ob_calm = book(0.0, dec!(100.00), dec!(10.0), dec!(100.02), dec!(10.0));
        let _ = strategy.on_orderbook_update(&ob_calm, &portfolio, 0.0);
        let intents_touch = strategy.on_orderbook_update(&ob_calm, &portfolio, 1.0);
        assert_eq!(buy_quote_price(&intents_touch), dec!(100.00));

        let ob_jump = book(1.0, dec!(100.06), dec!(10.0), dec!(100.08), dec!(10.0));
        let intents_passive = strategy.on_orderbook_update(&ob_jump, &portfolio, 2.0);
        assert!(
            buy_quote_price(&intents_passive) < dec!(100.06),
            "large |impulse| should disable touch-join"
        );
    }

    #[test]
    fn microprice_drift_filter_disables_ask_on_upward_drift() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_drift_filter(2, 0.4)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(dec!(1.0), dec!(100000.0));

        let _ = strategy.on_orderbook_update(
            &book(0.0, dec!(100.00), dec!(1.0), dec!(100.02), dec!(3.0)),
            &portfolio,
            0.0,
        );
        let _ = strategy.on_orderbook_update(
            &book(1.0, dec!(100.01), dec!(1.0), dec!(100.03), dec!(3.0)),
            &portfolio,
            1.0,
        );
        let intents = strategy.on_orderbook_update(
            &book(2.0, dec!(100.02), dec!(1.0), dec!(100.04), dec!(4.0)),
            &portfolio,
            2.0,
        );

        assert!(has_live_quote(&intents, OrderSide::Buy));
        assert!(!has_live_quote(&intents, OrderSide::Sell));
    }

    #[test]
    fn microprice_drift_filter_disables_bid_on_downward_drift() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_drift_filter(2, 0.4)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(dec!(1.0), dec!(100000.0));

        let _ = strategy.on_orderbook_update(
            &book(0.0, dec!(100.00), dec!(3.0), dec!(100.02), dec!(1.0)),
            &portfolio,
            0.0,
        );
        let _ = strategy.on_orderbook_update(
            &book(1.0, dec!(99.99), dec!(3.0), dec!(100.01), dec!(1.0)),
            &portfolio,
            1.0,
        );
        let intents = strategy.on_orderbook_update(
            &book(2.0, dec!(99.98), dec!(4.0), dec!(100.00), dec!(1.0)),
            &portfolio,
            2.0,
        );

        assert!(!has_live_quote(&intents, OrderSide::Buy));
        assert!(has_live_quote(&intents, OrderSide::Sell));
    }

    #[test]
    fn dynamic_spread_widens_quotes_when_market_spread_is_wide() {
        let portfolio = Portfolio::new(dec!(1.0), dec!(100000.0));
        let book = book(0.0, dec!(100.00), dec!(1.0), dec!(100.04), dec!(1.0));

        let mut baseline = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_base_spread(1.0)
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_refresh(0.0)
            .with_warmup(0.0);
        let baseline_intents = baseline.on_orderbook_update(&book, &portfolio, 0.0);

        let mut dynamic = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_base_spread(1.0)
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_dynamic_spread()
            .with_refresh(0.0)
            .with_warmup(0.0);
        let dynamic_intents = dynamic.on_orderbook_update(&book, &portfolio, 0.0);

        assert!(buy_quote_price(&dynamic_intents) < buy_quote_price(&baseline_intents));
        assert!(ask_quote_price(&dynamic_intents) > ask_quote_price(&baseline_intents));
    }

    /// Top-of-book sizes give microprice ≈ mid; deeper levels skew imbalance (~0.72) without
    /// tripping the >0.80 imbalance cancel.
    fn book_imbalance_skewed_top_balanced(ts: f64) -> OrderBook {
        OrderBook {
            bids: vec![
                (dec!(100.00), dec!(10.0)),
                (dec!(99.99), dec!(31.0)),
                (dec!(99.98), dec!(31.0)),
            ],
            asks: vec![
                (dec!(100.02), dec!(10.0)),
                (dec!(100.03), dec!(9.0)),
                (dec!(100.04), dec!(9.0)),
            ],
            timestamp: ts,
        }
    }

    #[test]
    fn state_dependent_multi_tick_passive_pulls_quotes_when_book_skewed() {
        // Need base inventory > 0 so the ask leg is posted (spot-style cap).
        let portfolio = Portfolio::new(dec!(1.0), dec!(100000.0));

        let mut baseline = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_volatility(5, 2.0, 4.0)
            .with_refresh(0.0)
            .with_warmup(0.0);

        let mut depth_on = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_volatility(5, 2.0, 4.0)
            .with_state_dependent_multi_tick_passive(2)
            .with_refresh(0.0)
            .with_warmup(0.0);

        for i in 0..8 {
            let ts = i as f64;
            let ob = book_imbalance_skewed_top_balanced(ts);
            let _ = baseline.on_orderbook_update(&ob, &portfolio, ts);
            let _ = depth_on.on_orderbook_update(&ob, &portfolio, ts);
        }

        let ob = book_imbalance_skewed_top_balanced(9.0);
        let b_intents = baseline.on_orderbook_update(&ob, &portfolio, 9.0);
        let d_intents = depth_on.on_orderbook_update(&ob, &portfolio, 9.0);

        assert!(
            buy_quote_price(&d_intents) < buy_quote_price(&b_intents),
            "expected lower bid when passive depth is on"
        );
        assert!(
            ask_quote_price(&d_intents) > ask_quote_price(&b_intents),
            "expected higher ask when passive depth is on"
        );
    }

    #[test]
    fn passive_pinch_nudge_separates_collapsed_quotes() {
        // When passive clamp maps bid and ask to the same tick (common for 2-tick-wide books).
        let s = RebateMMStrategy::new(dec!(0.5), dec!(0.01)).with_inventory_limits(dec!(10.0), dec!(10.0));
        let ask = dec!(100.01);
        let mut bid_price = dec!(100.01);
        let mut ask_price = ask;
        if bid_price >= ask_price && s.tick_size > Decimal::ZERO {
            bid_price = s.round_bid_to_tick(ask_price - s.tick_size);
            if bid_price >= ask_price {
                ask_price = s.round_ask_to_tick(bid_price + s.tick_size);
            }
        }
        assert!(bid_price < ask_price);
        assert_eq!(bid_price, dec!(100.00));
        assert_eq!(ask_price, dec!(100.01));
    }

    #[test]
    fn state_dependent_depth_blocks_safe_side_touch_join() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_volatility(5, 2.0, 4.0)
            .with_microprice_impulse_filter(4, 1.0, 1.0)
            .with_queue_aware_safe_side_touch_join(0.5, dec!(100.0))
            .with_state_dependent_multi_tick_passive(2)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(Decimal::ZERO, dec!(100000.0));

        for i in 0..8 {
            let ts = i as f64;
            let _ = strategy.on_orderbook_update(&book_imbalance_skewed_top_balanced(ts), &portfolio, ts);
        }

        // Microprice ticks up ~1 bps (safe-side join) but stays below impulse pause threshold;
        // book skew ⇒ passive depth > 0 ⇒ safe-side must not join the bid touch.
        let ob = OrderBook {
            bids: vec![
                (dec!(100.01), dec!(10.0)),
                (dec!(100.00), dec!(31.0)),
                (dec!(99.99), dec!(31.0)),
            ],
            asks: vec![
                (dec!(100.03), dec!(10.0)),
                (dec!(100.04), dec!(9.0)),
                (dec!(100.05), dec!(9.0)),
            ],
            timestamp: 9.0,
        };
        let intents = strategy.on_orderbook_update(&ob, &portfolio, 9.0);

        assert!(buy_quote_price(&intents) < dec!(100.01));
    }

    #[test]
    fn impulse_kill_switch_cancels_when_impulse_exceeds_threshold() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_impulse_filter(2, 5.0, 1.0)
            .with_impulse_kill_switch(1.5)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(Decimal::ZERO, dec!(100000.0));

        // Seed microprice history: need lookback+1 points; large jump ⇒ |impulse| huge
        for i in 0..4 {
            let _ = strategy.on_orderbook_update(
                &book(i as f64, dec!(100.00), dec!(2.0), dec!(100.02), dec!(2.0)),
                &portfolio,
                i as f64,
            );
        }
        let intents = strategy.on_orderbook_update(
            &book(5.0, dec!(100.10), dec!(2.0), dec!(100.12), dec!(2.0)),
            &portfolio,
            5.0,
        );
        assert!(
            intents.iter().all(|i| i.order_type == mm_core::strategy::OrderType::Cancel),
            "expected only cancels, got {:?}",
            intents
        );
        assert!(strategy.diag_impulse_kill_cancels >= 1);
    }

    #[test]
    fn impulse_kill_asymmetric_positive_impulse_suppresses_ask_only() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            // Pause at 1 bps — asymmetric kill must not be overridden by symmetric pause when |imp| ≥ kill (1.5).
            .with_microprice_impulse_filter(2, 1.0, 1.0)
            .with_impulse_kill_switch(1.5)
            .with_impulse_kill_asymmetric(true)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(Decimal::ZERO, dec!(100000.0));

        for i in 0..4 {
            let _ = strategy.on_orderbook_update(
                &book(i as f64, dec!(100.00), dec!(2.0), dec!(100.02), dec!(2.0)),
                &portfolio,
                i as f64,
            );
        }
        // Large upward jump → positive impulse; asymmetric: bid can quote, ask cancelled only.
        let intents = strategy.on_orderbook_update(
            &book(5.0, dec!(100.10), dec!(2.0), dec!(100.12), dec!(2.0)),
            &portfolio,
            5.0,
        );
        assert!(
            has_live_quote(&intents, OrderSide::Buy),
            "expected buy quote when positive impulse kills ask only, got {:?}",
            intents
        );
        assert!(
            !has_live_quote(&intents, OrderSide::Sell),
            "expected no sell quote, got {:?}",
            intents
        );
        assert!(
            intents
                .iter()
                .any(|i| i.side == OrderSide::Sell && i.order_type == mm_core::strategy::OrderType::Cancel),
            "expected sell cancel, got {:?}",
            intents
        );
        assert!(strategy.diag_impulse_kill_cancels >= 1);
    }

    #[test]
    fn impulse_kill_hybrid_symmetric_band_flattens_both() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_impulse_filter(2, 5.0, 1.0)
            .with_impulse_kill_switch(1.5)
            .with_impulse_kill_hybrid_extreme_bps(2.5)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(Decimal::ZERO, dec!(100000.0));

        let flat = book(0.0, dec!(100.00), dec!(2.0), dec!(100.02), dec!(2.0));
        for i in 0..3 {
            let _ = strategy.on_orderbook_update(&flat, &portfolio, i as f64);
        }
        // mp 100.01 → 100.032 ⇒ ~2.2 bps vs lookback-2 ref — inside [1.5, 2.5) ⇒ symmetric flat.
        let intents = strategy.on_orderbook_update(
            &book(3.0, dec!(100.022), dec!(2.0), dec!(100.042), dec!(2.0)),
            &portfolio,
            3.0,
        );
        assert!(
            intents.iter().all(|i| i.order_type == mm_core::strategy::OrderType::Cancel),
            "expected only cancels, got {:?}",
            intents
        );
        assert!(strategy.diag_impulse_kill_cancels >= 1);
    }

    #[test]
    fn impulse_kill_hybrid_extreme_suppresses_ask_only() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_microprice_impulse_filter(2, 5.0, 1.0)
            .with_impulse_kill_switch(1.5)
            .with_impulse_kill_hybrid_extreme_bps(2.5)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(Decimal::ZERO, dec!(100000.0));

        let flat = book(0.0, dec!(100.00), dec!(2.0), dec!(100.02), dec!(2.0));
        for i in 0..3 {
            let _ = strategy.on_orderbook_update(&flat, &portfolio, i as f64);
        }
        let intents = strategy.on_orderbook_update(
            &book(3.0, dec!(100.04), dec!(2.0), dec!(100.06), dec!(2.0)),
            &portfolio,
            3.0,
        );
        assert!(
            has_live_quote(&intents, OrderSide::Buy),
            "expected buy quote when hybrid extreme kills ask only, got {:?}",
            intents
        );
        assert!(
            !has_live_quote(&intents, OrderSide::Sell),
            "expected no sell quote, got {:?}",
            intents
        );
        assert!(strategy.diag_impulse_kill_cancels >= 1);
    }

    #[test]
    fn wide_spread_filter_cancels_when_spread_wide() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_wide_spread_no_quotes(5.0)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(Decimal::ZERO, dec!(100000.0));

        // mid ~100, spread 2.0 ⇒ 20 bps — still under 5? 2/100*10000 = 200 bps actually
        // best_bid 99, best_ask 101 => spread 2, mid 100 => 200 bps > 5
        let wide = OrderBook {
            bids: vec![(dec!(99.0), dec!(1.0)), (dec!(98.0), dec!(1.0))],
            asks: vec![(dec!(101.0), dec!(1.0)), (dec!(102.0), dec!(1.0))],
            timestamp: 0.0,
        };
        let intents = strategy.on_orderbook_update(&wide, &portfolio, 0.0);
        assert!(
            intents.iter().all(|i| i.order_type == mm_core::strategy::OrderType::Cancel),
            "expected only cancels, got {:?}",
            intents
        );
        assert!(strategy.diag_wide_spread_cancels >= 1);
    }

    #[test]
    fn quote_stickiness_keeps_bid_when_touch_moves_less_than_hysteresis_ticks() {
        let mut strategy = RebateMMStrategy::new(dec!(0.5), dec!(0.01))
            .with_inventory_limits(dec!(10.0), dec!(10.0))
            .with_quote_stickiness(0.0, 3)
            .with_refresh(0.0)
            .with_warmup(0.0);
        let portfolio = Portfolio::new(Decimal::ZERO, dec!(100000.0));

        let intents0 = strategy.on_orderbook_update(
            &book(0.0, dec!(100.00), dec!(2.0), dec!(100.04), dec!(2.0)),
            &portfolio,
            0.0,
        );
        let p0 = buy_quote_price(&intents0);

        // Move both sides up 1 tick; candidate bid moves ~1 tick — hysteresis 3 ⇒ keep prior price.
        let intents1 = strategy.on_orderbook_update(
            &book(0.2, dec!(100.01), dec!(2.0), dec!(100.05), dec!(2.0)),
            &portfolio,
            0.2,
        );
        let p1 = buy_quote_price(&intents1);
        assert_eq!(p0, p1);
    }
}

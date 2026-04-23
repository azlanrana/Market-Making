use mm_core_types::{FeeModel, Fill, FillReason};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub struct SimpleFeeModel {
    pub maker_bps: Decimal,
    pub taker_bps: Decimal,
    /// When set, fills with [`FillReason::CrossedBook`] pay this convex combo of fees (not `is_taker`):
    /// `weight * taker_bps + (1 - weight) * maker_bps`. Other fills unchanged.
    pub crossed_book_taker_weight: Option<Decimal>,
}

impl SimpleFeeModel {
    pub fn new(maker_bps: Decimal, taker_bps: Decimal) -> Self {
        Self {
            maker_bps,
            taker_bps,
            crossed_book_taker_weight: None,
        }
    }

    pub fn with_crossed_book_taker_weight(mut self, weight: Decimal) -> Self {
        self.crossed_book_taker_weight = Some(weight);
        self
    }
}

impl FeeModel for SimpleFeeModel {
    fn fee_bps(&self, fill: &Fill) -> Decimal {
        if let (Some(w), Some(FillReason::CrossedBook)) =
            (self.crossed_book_taker_weight, fill.fill_reason)
        {
            return w * self.taker_bps + (Decimal::ONE - w) * self.maker_bps;
        }
        if fill.is_taker {
            self.taker_bps
        } else {
            self.maker_bps
        }
    }
}

impl Default for SimpleFeeModel {
    fn default() -> Self {
        Self {
            maker_bps: dec!(-0.75),
            taker_bps: dec!(1.5),
            crossed_book_taker_weight: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core_types::Side;

    fn fill_crossed() -> Fill {
        Fill {
            order_id: "o1".into(),
            side: Side::Buy,
            price: dec!(100),
            amount: dec!(1),
            remaining: dec!(0),
            is_taker: false,
            fill_reason: Some(FillReason::CrossedBook),
            timestamp: 0.0,
            layer: 0,
        }
    }

    #[test]
    fn crossed_book_half_taker_blends_fees() {
        let m =
            SimpleFeeModel::new(dec!(-0.75), dec!(1.5)).with_crossed_book_taker_weight(dec!(0.5));
        let f = fill_crossed();
        let bps = m.fee_bps(&f);
        assert_eq!(bps, dec!(0.375));
    }
}

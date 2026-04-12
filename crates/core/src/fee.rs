use rust_decimal::Decimal;

use crate::fill::Fill;

pub trait FeeModel: Send + Sync {
    fn fee_bps(&self, fill: &Fill) -> Decimal;
}

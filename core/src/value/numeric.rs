use std::cmp::Ordering;

use serde_json::Number;

pub(crate) enum Numeric {
    Integer(i128),
    Real(f64),
}

impl Numeric {
    fn from_json(number: &Number) -> Option<Self> {
        if let Some(value) = number.as_i64() {
            Some(Self::Integer(i128::from(value)))
        } else if let Some(value) = number.as_u64() {
            Some(Self::Integer(i128::from(value)))
        } else {
            number.as_f64().map(Self::Real)
        }
    }

    pub(crate) fn compare(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => Some(left.cmp(&right)),
            (Self::Real(left), Self::Real(right)) => left.partial_cmp(&right),
            (Self::Real(left), Self::Integer(right)) => Self::Integer(right).compare(Self::Real(left)).map(Ordering::reverse),
            (Self::Integer(integer), Self::Real(real)) => {
                if real >= i128::MAX as f64 {
                    return Some(Ordering::Less);
                }
                if real < i128::MIN as f64 {
                    return Some(Ordering::Greater);
                }
                let fraction = real.fract().partial_cmp(&0.0)?;
                Some(integer.cmp(&(real as i128)).then(fraction.reverse()))
            }
        }
    }
}

pub(crate) fn compare_numbers(left: &Number, right: &Number) -> Option<Ordering> {
    Numeric::from_json(left)?.compare(Numeric::from_json(right)?)
}

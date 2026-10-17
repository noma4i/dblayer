use super::{OrderKeyError, ORDER_ALPHABET};

pub const ORDER_KEY_LIMIT: usize = 24;

const SMALLEST_INTEGER: &str = "A00000000000000000000000000";

fn digit(byte: u8) -> Result<usize, OrderKeyError> {
    ORDER_ALPHABET.bytes().position(|candidate| candidate == byte).ok_or(OrderKeyError::InvalidBounds)
}

fn digit_at(index: usize) -> u8 {
    ORDER_ALPHABET.as_bytes()[index]
}

fn integer_length(head: u8) -> Result<usize, OrderKeyError> {
    match head {
        b'a'..=b'z' => Ok(usize::from(head - b'a') + 2),
        b'A'..=b'Z' => Ok(usize::from(b'Z' - head) + 2),
        _ => Err(OrderKeyError::InvalidBounds),
    }
}

fn integer_part(key: &str) -> Result<&str, OrderKeyError> {
    let length = integer_length(*key.as_bytes().first().ok_or(OrderKeyError::InvalidBounds)?)?;
    key.get(..length).ok_or(OrderKeyError::InvalidBounds)
}

fn checked(key: &str) -> Result<(), OrderKeyError> {
    let integer = integer_part(key)?;
    let valid = key != SMALLEST_INTEGER && !key[integer.len()..].ends_with('0') && key.bytes().all(|byte| digit(byte).is_ok());
    if valid { Ok(()) } else { Err(OrderKeyError::InvalidBounds) }
}

fn midpoint(lower: &str, upper: Option<&str>) -> Result<String, OrderKeyError> {
    let zero = digit_at(0);
    if upper.is_some_and(|upper| lower >= upper) || lower.ends_with('0') || upper.is_some_and(|upper| upper.ends_with('0')) {
        return Err(OrderKeyError::InvalidBounds);
    }
    if let Some(upper) = upper {
        let shared = (0..).take_while(|index| lower.as_bytes().get(*index).copied().unwrap_or(zero) == upper.as_bytes().get(*index).copied().unwrap_or(0)).count();
        if shared > 0 {
            return Ok(format!("{}{}", &upper[..shared], midpoint(lower.get(shared..).unwrap_or(""), Some(&upper[shared..]))?));
        }
    }
    let low = lower.bytes().next().map_or(Ok(0), digit)?;
    let high = upper.and_then(|upper| upper.bytes().next()).map_or(Ok(ORDER_ALPHABET.len()), digit)?;
    if high - low > 1 {
        return Ok(char::from(digit_at((low + high + 1) / 2)).to_string());
    }
    match upper {
        Some(upper) if upper.len() > 1 => Ok(upper[..1].to_string()),
        _ => Ok(format!("{}{}", char::from(digit_at(low)), midpoint(lower.get(1..).unwrap_or(""), None)?)),
    }
}

fn incremented(integer: &str) -> Result<Option<String>, OrderKeyError> {
    let (head, mut digits) = (integer.as_bytes()[0], integer.as_bytes()[1..].to_vec());
    let mut carry = true;
    for slot in digits.iter_mut().rev() {
        let next = digit(*slot)? + 1;
        if next == ORDER_ALPHABET.len() {
            *slot = digit_at(0);
        } else {
            *slot = digit_at(next);
            carry = false;
            break;
        }
    }
    if !carry {
        return Ok(Some(format!("{}{}", char::from(head), String::from_utf8_lossy(&digits))));
    }
    match head {
        b'Z' => Ok(Some("a0".to_string())),
        b'z' => Ok(None),
        _ => {
            let next = head + 1;
            if next > b'a' { digits.push(digit_at(0)) } else { digits.pop(); }
            Ok(Some(format!("{}{}", char::from(next), String::from_utf8_lossy(&digits))))
        }
    }
}

fn decremented(integer: &str) -> Result<Option<String>, OrderKeyError> {
    let (head, mut digits) = (integer.as_bytes()[0], integer.as_bytes()[1..].to_vec());
    let last = digit_at(ORDER_ALPHABET.len() - 1);
    let mut borrow = true;
    for slot in digits.iter_mut().rev() {
        match digit(*slot)?.checked_sub(1) {
            Some(previous) => {
                *slot = digit_at(previous);
                borrow = false;
                break;
            }
            None => *slot = last,
        }
    }
    if !borrow {
        return Ok(Some(format!("{}{}", char::from(head), String::from_utf8_lossy(&digits))));
    }
    match head {
        b'a' => Ok(Some(format!("Z{}", char::from(last)))),
        b'A' => Ok(None),
        _ => {
            let previous = head - 1;
            if previous < b'Z' { digits.push(last) } else { digits.pop(); }
            Ok(Some(format!("{}{}", char::from(previous), String::from_utf8_lossy(&digits))))
        }
    }
}

pub fn fractional_between(lower: Option<&str>, upper: Option<&str>) -> Result<String, OrderKeyError> {
    lower.map(checked).transpose()?;
    upper.map(checked).transpose()?;
    if lower.zip(upper).is_some_and(|(lower, upper)| lower >= upper) {
        return Err(OrderKeyError::InvalidBounds);
    }
    match (lower, upper) {
        (None, None) => Ok(format!("a{}", char::from(digit_at(0)))),
        (None, Some(upper)) => {
            let integer = integer_part(upper)?;
            if integer == SMALLEST_INTEGER {
                return Ok(format!("{integer}{}", midpoint("", Some(&upper[integer.len()..]))?));
            }
            if integer < upper {
                return Ok(integer.to_string());
            }
            decremented(integer)?.ok_or(OrderKeyError::InvalidBounds)
        }
        (Some(lower), None) => {
            let integer = integer_part(lower)?;
            match incremented(integer)? {
                Some(next) => Ok(next),
                None => Ok(format!("{integer}{}", midpoint(&lower[integer.len()..], None)?)),
            }
        }
        (Some(lower), Some(upper)) => {
            let (low_integer, high_integer) = (integer_part(lower)?, integer_part(upper)?);
            if low_integer == high_integer {
                return Ok(format!("{low_integer}{}", midpoint(&lower[low_integer.len()..], Some(&upper[high_integer.len()..]))?));
            }
            let next = incremented(low_integer)?.ok_or(OrderKeyError::InvalidBounds)?;
            if next.as_str() < upper {
                return Ok(next);
            }
            Ok(format!("{low_integer}{}", midpoint(&lower[low_integer.len()..], None)?))
        }
    }
}

pub fn fractional_keys_between(lower: Option<&str>, upper: Option<&str>, count: usize) -> Result<Vec<String>, OrderKeyError> {
    match (count, lower, upper) {
        (0, ..) => Ok(Vec::new()),
        (1, ..) => Ok(vec![fractional_between(lower, upper)?]),
        (_, _, None) => {
            let mut keys = vec![fractional_between(lower, upper)?];
            for _ in 1..count {
                keys.push(fractional_between(keys.last().map(String::as_str), None)?);
            }
            Ok(keys)
        }
        (_, None, Some(_)) => {
            let mut keys = vec![fractional_between(lower, upper)?];
            for _ in 1..count {
                keys.push(fractional_between(None, keys.last().map(String::as_str))?);
            }
            keys.reverse();
            Ok(keys)
        }
        _ => {
            let middle = fractional_between(lower, upper)?;
            let below = fractional_keys_between(lower, Some(&middle), count / 2)?;
            let above = fractional_keys_between(Some(&middle), upper, count - count / 2 - 1)?;
            Ok(below.into_iter().chain([middle]).chain(above).collect())
        }
    }
}

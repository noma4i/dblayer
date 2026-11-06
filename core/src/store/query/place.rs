use rusqlite::types::Value;

const END: [u8; 2] = [0, 0];

fn float_bits(number: f64) -> [u8; 8] {
    let bits = if number == 0.0 { 0.0_f64 } else { number }.to_bits();
    (if bits >> 63 == 1 { !bits } else { bits | (1 << 63) }).to_be_bytes()
}

fn number(out: &mut Vec<u8>, approximate: f64, rest: i16) {
    out.push(1);
    out.extend(float_bits(approximate));
    out.extend((rest as u16 ^ 0x8000).to_be_bytes());
}

fn bytes(out: &mut Vec<u8>, tag: u8, value: &[u8]) {
    out.push(tag);
    for byte in value {
        out.push(*byte);
        if *byte == 0 {
            out.push(0xFF);
        }
    }
    out.extend(END);
}

fn cell(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        Value::Null => out.push(0),
        Value::Integer(integer) => {
            let approximate = *integer as f64;
            number(&mut out, approximate, (i128::from(*integer) - approximate as i128) as i16);
        }
        Value::Real(real) => number(&mut out, *real, 0),
        Value::Text(text) => bytes(&mut out, 2, text.as_bytes()),
        Value::Blob(blob) => bytes(&mut out, 3, blob),
    }
    out
}

pub fn place_of(cells: &[Value], descending: &[bool]) -> String {
    let mut place = Vec::new();
    for (value, descending) in cells.iter().zip(descending) {
        let encoded = cell(value);
        place.extend(encoded.into_iter().map(|byte| if *descending { !byte } else { byte }));
    }
    place.iter().map(|byte| format!("{byte:02x}")).collect()
}

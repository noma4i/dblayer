use super::vectors::*;
use super::*;

#[test]
fn a_key_is_trusted_only_with_the_matching_pin_and_at_least_3072_bits() {
    assert_eq!(ServerKey::parse(PUBLIC_KEY_DER_B64, PIN).unwrap().pin(), PIN);
    let cases = [
        (PUBLIC_KEY_DER_B64, SHORT_KEY_PIN),
        (PUBLIC_KEY_DER_B64, &PIN.to_uppercase()),
        (PUBLIC_KEY_DER_B64, ""),
        (SHORT_KEY_DER_B64, SHORT_KEY_PIN),
        ("not base64!", PIN),
        ("AAAA", PIN),
    ];
    for (key, pin) in cases {
        assert!(ServerKey::parse(key, pin).is_err(), "{key:.12} {pin:.12}");
    }
}

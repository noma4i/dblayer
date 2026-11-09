use super::vectors_tests::*;
use super::*;

#[test]
fn a_key_is_trusted_only_with_the_matching_pin_and_at_least_3072_bits() {
    assert_eq!(ServerKey::parse(public_key_der_b64(), pin()).unwrap().pin(), pin());
    let cases = [
        (public_key_der_b64(), short_key_pin()),
        (public_key_der_b64(), &pin().to_uppercase()),
        (public_key_der_b64(), ""),
        (short_key_der_b64(), short_key_pin()),
        ("not base64!", pin()),
        ("AAAA", pin()),
    ];
    for (key, pin) in cases {
        assert!(matches!(ServerKey::parse(key, pin), Err(crate::transport::encrypted::key::KeyError::Untrusted(_))), "{key:.12} {pin:.12}");
    }
}

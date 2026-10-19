use super::*;

#[test]
fn graphql_code_is_read_only_from_a_string_extension() {
    let cases = [
        ("string code", r#"{"message":"x","extensions":{"code":"SIGNATURE_EXPIRED"}}"#, Some("SIGNATURE_EXPIRED")),
        ("no extensions", r#"{"message":"x"}"#, None),
        ("numeric code", r#"{"message":"x","extensions":{"code":5}}"#, None),
    ];
    for (case, body, expected) in cases {
        let error: GraphQLError = serde_json::from_str(body).unwrap();
        assert_eq!(error.code(), expected, "{case}");
    }
}

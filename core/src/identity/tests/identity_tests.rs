use super::*;

#[test]
fn row_id_normalizes_text_and_integers_and_rejects_blank() {
    assert_eq!(RowId::new("abc").unwrap().as_str(), "abc");
    assert_eq!(RowId::new("  abc \n").unwrap().as_str(), "abc");
    assert_eq!(RowId::from_integer(42).as_str(), "42");
    assert_eq!(RowId::new(""), Err(IdentityError::MissingId));
    assert_eq!(RowId::new("   "), Err(IdentityError::MissingId));
}

#[test]
fn inv_id_07_temp_ids_are_recognised_and_unique() {
    let plain = RowId::generate_temp(None);
    let prefixed = RowId::generate_temp(Some("message"));
    assert!(plain.is_temp());
    assert!(plain.as_str().starts_with("temp-"));
    assert!(prefixed.as_str().starts_with("temp-message-"));
    assert_ne!(plain, RowId::generate_temp(None));
    assert!(!RowId::new("server-1").unwrap().is_temp());
    assert!(!RowId::new("temp-123").unwrap().is_temp());
    let many: std::collections::BTreeSet<RowId> = (0..10_000).map(|_| RowId::generate_temp(None)).collect();
    assert_eq!(many.len(), 10_000);
    assert!(many.iter().all(RowId::is_temp));
}

#[test]
fn row_id_serializes_as_string_and_deserializes_string_or_integer_but_not_blank() {
    let id = RowId::new("m1").unwrap();
    assert_eq!(serde_json::to_string(&id).unwrap(), "\"m1\"");
    assert_eq!(serde_json::from_str::<RowId>("\"m1\"").unwrap().as_str(), "m1");
    assert_eq!(serde_json::from_str::<RowId>("42").unwrap().as_str(), "42");
    assert_eq!(serde_json::from_str::<RowId>("\"   \"").unwrap_err().to_string(), "RowId must not be blank at line 1 column 5");
    assert_eq!(serde_json::from_str::<RowId>("1.5").unwrap_err().to_string(), "invalid type: floating point `1.5`, expected a string or integer row id at line 1 column 3");
    assert_eq!(serde_json::from_str::<RowId>("true").unwrap_err().to_string(), "invalid type: boolean `true`, expected a string or integer row id at line 1 column 4");
}

#[test]
fn scope_key_encodes_segments_with_utf16_length() {
    assert_eq!(ListKey::compose(&["chats", "42"]).as_str(), "5:chats2:42");
    assert_eq!(ListKey::compose(&[""]).as_str(), "0:");
    assert_eq!(ListKey::compose::<&str>(&[]).as_str(), "");
    assert_eq!(ListKey::compose(&["🚀"]).as_str(), "2:🚀");
}

#[test]
fn scope_key_round_trips_any_content_and_keeps_ambiguous_splits_apart() {
    let cases: Vec<Vec<&str>> = vec![vec!["chats", "42"], vec!["a:b"], vec!["3:xyz"], vec!["🚀"], vec![], vec![""], vec!["a", "bc"], vec!["a:b", "c"]];
    let keys: Vec<ListKey> = cases.iter().map(|segments| ListKey::compose(segments)).collect();
    for (segments, key) in cases.iter().zip(&keys) {
        assert_eq!(key.decompose(), Some(segments.iter().map(|s| s.to_string()).collect()));
    }
    for (i, left) in keys.iter().enumerate() {
        for right in &keys[i + 1..] {
            assert_ne!(left, right);
        }
    }
}

#[test]
fn scope_key_rejects_malformed_text() {
    for text in ["x", "5:ab", "-1:a", "a:b", "2"] {
        assert_eq!(ListKey::from_raw(text).decompose(), None, "{text}");
    }
}

use super::*;
use serde_json::json;

fn row_id(value: &str) -> RowId {
    RowId::new(value).unwrap()
}

fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[test]
fn scalar_kinds_convert_supported_values_only() {
    assert_eq!(read_str(&json!("a")), Some("a".to_string()));
    assert_eq!(read_str(&json!(2)), None);
    assert_eq!(read_str(&Value::Null), None);
    assert_eq!(read_num(&json!(1.5)), Some(1.5));
    assert_eq!(read_num(&json!("2.5")), Some(2.5));
    assert_eq!(read_num(&json!("")), None);
    assert_eq!(read_num(&json!(true)), None);
    assert_eq!(read_int(&json!(3)), Some(3));
    assert_eq!(read_int(&json!(3.9)), None);
    assert_eq!(read_int(&json!("7")), Some(7));
    assert_eq!(read_int(&json!("x")), None);
    assert_eq!(read_bool(&json!(false)), Some(false));
    assert_eq!(read_bool(&json!("true")), Some(true));
    assert_eq!(read_bool(&json!("yes")), None);
    assert_eq!(read_bool(&json!(1)), None);
    assert_eq!(read_date(&json!("2021-05-05T10:00:00Z")), Some("2021-05-05T10:00:00Z".to_string()));
    assert_eq!(read_date(&json!(0)), Some("1970-01-01T00:00:00.000Z".to_string()));
    assert_eq!(read_date(&json!(true)), None);
}

#[test]
fn number_edge_cases_normalize() {
    assert_eq!(read_num(&json!(-0.0)).map(f64::is_sign_negative), Some(false));
    assert_eq!(read_num(&json!("inf")), None);
    assert_eq!(read_num(&json!("NaN")), None);
    assert_eq!(read_int(&json!(9_007_199_254_740_993_i64)), Some(9_007_199_254_740_993));
    assert_eq!(read_int(&json!(1e30)), None);
    assert_eq!(read_date(&json!(1_700_000_000_123_i64)), Some("2023-11-14T22:13:20.123Z".to_string()));
}

#[test]
fn identifier_accepts_non_empty_strings_and_integral_numbers() {
    assert_eq!(read_identifier(&json!("abc")), Some("abc".to_string()));
    assert_eq!(read_identifier(&json!("")), None);
    assert_eq!(read_identifier(&json!(7)), Some("7".to_string()));
    assert_eq!(read_identifier(&json!(-12)), Some("-12".to_string()));
    assert_eq!(read_identifier(&json!(7.0)), Some("7".to_string()));
    assert_eq!(read_identifier(&json!(7.5)), None);
    assert_eq!(read_identifier(&json!(true)), None);
    assert_eq!(read_identifier(&Value::Null), None);
}

#[test]
fn required_values_report_missing_and_rejected() {
    assert_eq!(require(None, "chatId", read_id), Err(ScalarError::MissingValue { context: "chatId".into() }));
    assert_eq!(
        require(Some(&Value::Null), "chatId", read_id),
        Err(ScalarError::MissingValue { context: "chatId".into() })
    );
    assert_eq!(
        require(Some(&json!("")), "chatId", read_id),
        Err(ScalarError::RejectedValue { context: "chatId".into() })
    );
    assert_eq!(
        require(Some(&json!(7.5)), "chatId", read_id),
        Err(ScalarError::RejectedValue { context: "chatId".into() })
    );
    assert_eq!(
        require(Some(&json!("yes")), "flag", read_bool),
        Err(ScalarError::RejectedValue { context: "flag".into() })
    );
}

#[test]
fn enum_and_raw_values_convert_supported_values_only() {
    let allowed = vec!["image".to_string(), "video".to_string()];
    assert_eq!(read_enum(&json!("image"), &allowed), Some("image".to_string()));
    assert_eq!(read_enum(&json!("audio"), &allowed), None);
    assert_eq!(read_enum(&Value::Null, &allowed), None);
    assert_eq!(read_raw(&json!("a")), Some(json!("a")));
    assert_eq!(read_raw(&json!([])), Some(json!([])));
    assert_eq!(read_raw(&Value::Null), None);
}

#[test]
fn array_and_object_read_only_their_own_container() {
    let list = json!(["1", "x", "2"]);
    let record = json!({"id": "7"});
    assert_eq!(read_array(&list, read_int), Some(vec![1, 2]));
    assert_eq!(read_array(&json!([]), read_int), Some(vec![]));
    assert_eq!(read_array(&json!("a"), read_int), None);
    assert_eq!(read_array(&Value::Null, read_int), None);
    assert_eq!(read_object(&record, |map| map.get("id").and_then(read_str)), Some("7".to_string()));
    assert_eq!(read_object(&json!([]), |_| Some("x")), None);
}

#[test]
fn scalar_kind_decodes_into_json_values() {
    assert_eq!(read_scalar(&json!("42"), ScalarKind::Int), Some(json!(42)));
    assert_eq!(read_scalar(&json!("abc"), ScalarKind::Int), None);
    assert_eq!(read_scalar(&json!(2), ScalarKind::Num), Some(json!(2.0)));
    assert_eq!(read_scalar(&json!(""), ScalarKind::Id), None);
    assert_eq!(read_scalar(&json!(7), ScalarKind::Id), Some(json!("7")));
    assert_eq!(read_scalar(&json!("true"), ScalarKind::Bool), Some(json!(true)));
    assert_eq!(read_scalar(&json!("x"), ScalarKind::Str), Some(json!("x")));
    let date = read_scalar(&json!(1_700_000_000_000_i64), ScalarKind::Date).unwrap();
    assert!(date.as_str().unwrap().starts_with("2023-"));
}

#[test]
fn null_semantics_treat_absent_and_null_as_missing() {
    let list = vec![json!("a")];
    let a = json!("a");
    let b = json!("b");
    assert!(null_equals(None, None));
    assert!(null_equals(Some(&Value::Null), None));
    assert!(!null_equals(Some(&a), None));
    assert!(null_equals(Some(&a), Some(&a)));
    assert!(null_not_equals(Some(&a), Some(&b)));
    assert!(!null_not_equals(None, None));
    assert!(!null_is_in(None, &list));
    assert!(null_is_in(Some(&a), &list));
    assert!(!null_is_in(Some(&Value::Null), &list));
    assert!(null_is_not_in(None, &list));
    assert!(!null_is_not_in(Some(&a), &list));
    assert!(null_is_not_in(Some(&b), &list));
    assert!(null_is_not_in(None, &[]));
}

#[test]
fn null_semantics_compare_numbers_strings_and_absent_values() {
    let five = json!(5);
    let three = json!(3);
    assert!(null_compare(Some(&five), &three, true, false));
    assert!(!null_compare(Some(&five), &five, true, false));
    assert!(null_compare(Some(&five), &five, true, true));
    assert!(!null_compare(None, &five, true, false));
    assert!(null_compare(Some(&json!("b")), &json!("a"), true, false));
    assert!(null_compare(Some(&three), &five, false, false));
    assert!(!null_compare(Some(&json!("b")), &five, true, false));
    assert!(null_equals(Some(&json!(1)), Some(&json!(1.0))));
}

#[test]
fn order_key_numbers_and_strings_keep_order_per_direction() {
    let id = row_id("row");
    let values = [-10.0, -1.0, 0.0, 1.0, 10.0];
    let ascending: Vec<String> =
        values.iter().map(|number| order_key_encoded(&[OrderValue::Number(*number)], &[SortDirection::Asc], &id)).collect();
    let descending: Vec<String> =
        values.iter().map(|number| order_key_encoded(&[OrderValue::Number(*number)], &[SortDirection::Desc], &id)).collect();
    let strings: Vec<String> = ["a", "ab", "b"]
        .iter()
        .map(|text| order_key_encoded(&[OrderValue::Text(text.to_string())], &[SortDirection::Asc], &id))
        .collect();
    assert!(ascending.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(descending.windows(2).all(|pair| pair[0] > pair[1]));
    assert!(strings[0] < strings[1]);
    assert!(strings[1] < strings[2]);
}

#[test]
fn order_key_missing_sorts_last_in_both_directions_and_nan_is_largest() {
    let id = row_id("row");
    let key = |value: OrderValue, direction: SortDirection| order_key_encoded(&[value], &[direction], &id);
    let number = key(OrderValue::Number(1.0), SortDirection::Asc);
    let nan = key(OrderValue::Number(f64::NAN), SortDirection::Asc);
    let missing = key(OrderValue::Missing, SortDirection::Asc);
    assert!(number < nan);
    assert!(nan < missing);
    assert!(key(OrderValue::Number(1.0), SortDirection::Desc) < key(OrderValue::Missing, SortDirection::Desc));
}

#[test]
fn order_key_tie_break_uses_id_and_fields_compare_lexicographically() {
    let id = row_id("row");
    let with_id = |value: &str| order_key_encoded(&[OrderValue::Number(1.0)], &[SortDirection::Asc], &row_id(value));
    let pair = |number: f64, text: &str| {
        order_key_encoded(
            &[OrderValue::Number(number), OrderValue::Text(text.to_string())],
            &[SortDirection::Asc, SortDirection::Asc],
            &id,
        )
    };
    assert_ne!(with_id("1"), with_id("2"));
    assert!(with_id("1") < with_id("2"));
    assert!(pair(1.0, "a") < pair(1.0, "b"));
    assert!(pair(1.0, "z") < pair(2.0, "a"));
}

#[test]
fn fractional_keys_stay_ordered_unique_and_never_end_with_zero() {
    let ordered = |case: &str, keys: &[String]| {
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]), "{case}: keys out of order");
        assert_eq!(keys.iter().collect::<std::collections::HashSet<_>>().len(), keys.len(), "{case}: duplicate keys");
        assert!(keys.iter().all(|key| !key.is_empty() && !key.ends_with('0')), "{case}: empty or zero-ended key");
    };

    let unbounded = fractional_between(None, None).unwrap();
    let before = fractional_between(None, Some("V")).unwrap();
    let after = fractional_between(Some("V"), None).unwrap();
    ordered("unbounded", std::slice::from_ref(&unbounded));
    ordered("open ends around V", &[before, "V".to_string(), after]);

    let mut lowers: Vec<String> = Vec::new();
    let mut uppers: Vec<String> = Vec::new();
    let mut state = 17_u32;
    for _ in 0..20 {
        let key = fractional_between(lowers.last().map(String::as_str), uppers.last().map(String::as_str)).unwrap();
        state = (state * 73 + 19) % 997;
        if state.is_multiple_of(2) {
            lowers.push(key);
        } else {
            uppers.push(key);
        }
    }
    lowers.extend(uppers.into_iter().rev());
    ordered("bisected gap", &lowers);

    let mut keys: Vec<String> = Vec::new();
    state = 37;
    for _ in 0..100 {
        state = (state * 73 + 19) % 997;
        let index = state as usize % (keys.len() + 1);
        let lower = if index == 0 { None } else { Some(keys[index - 1].as_str()) };
        let upper = if index == keys.len() { None } else { Some(keys[index].as_str()) };
        let key = fractional_between(lower, upper).unwrap();
        keys.insert(index, key);
    }
    ordered("random positions", &keys);

    let mut heads: Vec<String> = Vec::new();
    for _ in 0..5000 {
        let key = fractional_between(None, heads.last().map(String::as_str)).unwrap();
        heads.push(key);
    }
    heads.reverse();
    ordered("5000 inserts before the head", &heads);

    let lower = unbounded;
    let upper = fractional_between(Some(&lower), None).unwrap();
    let mut between = vec![lower];
    for _ in 0..5000 {
        let key = fractional_between(between.last().map(String::as_str), Some(&upper)).unwrap();
        between.push(key);
    }
    between.push(upper);
    ordered("5000 inserts between fixed bounds", &between);

    let mut appended: Vec<String> = Vec::new();
    for _ in 0..200 {
        let key = fractional_between(appended.last().map(String::as_str), None).unwrap();
        appended.push(key);
    }
    ordered("200 appends", &appended);
}

#[test]
fn fractional_rejects_inverted_bounds() {
    assert_eq!(fractional_between(Some("b"), Some("a")), Err(OrderKeyError::InvalidBounds));
    assert_eq!(fractional_between(Some("!"), Some("a")), Err(OrderKeyError::InvalidBounds));
}

#[test]
fn null_relation_value_is_its_own_scope() {
    let kinds = BTreeMap::from([("vibeId".to_string(), ScalarKind::Id)]);
    let none = relation_scope_key_from_values("moment", "feed", &object(json!({"vibeId": null})), &kinds).unwrap();
    let some = relation_scope_key_from_values("moment", "feed", &object(json!({"vibeId": "1"})), &kinds).unwrap();
    assert_ne!(none, some);
    assert_eq!(none, relation_scope_key_from_values("moment", "feed", &object(json!({"vibeId": null})), &kinds).unwrap());
}

#[test]
fn relation_values_normalize_to_equal_params_and_keys() {
    let kinds = BTreeMap::from([("chatId".to_string(), ScalarKind::Id)]);
    let from_string = normalize_relation_params(&object(json!({"chatId": "1"})), &kinds).unwrap();
    let from_number = normalize_relation_params(&object(json!({"chatId": 1})), &kinds).unwrap();
    assert_eq!(from_string, BTreeMap::from([("chatId".to_string(), "1".to_string())]));
    assert_eq!(from_number, from_string);
    let count = BTreeMap::from([("count".to_string(), ScalarKind::Int)]);
    assert_eq!(normalize_relation_params(&object(json!({"count": "42"})), &count).unwrap()["count"], "42");
    let flag = BTreeMap::from([("flag".to_string(), ScalarKind::Bool)]);
    assert_eq!(normalize_relation_params(&object(json!({"flag": true})), &flag).unwrap()["flag"], "true");
    let text = BTreeMap::from([("q".to_string(), ScalarKind::Str), ("at".to_string(), ScalarKind::Date)]);
    let both = normalize_relation_params(&object(json!({"q": "hi", "at": "2021-01-01T00:00:00Z"})), &text).unwrap();
    assert_eq!(both["q"], "hi");
    assert_eq!(both["at"], "2021-01-01T00:00:00Z");
    let from_string_key = relation_scope_key_from_values("message", "thread", &object(json!({"chatId": "42"})), &kinds).unwrap();
    let from_number_key = relation_scope_key_from_values("message", "thread", &object(json!({"chatId": 42})), &kinds).unwrap();
    assert_eq!(from_string_key, from_number_key);
    let empty_values = relation_scope_key_from_values("message", "thread", &Map::new(), &BTreeMap::new()).unwrap();
    assert_eq!(empty_values, relation_scope_key("message", "thread", &BTreeMap::new()).unwrap());
}

#[test]
fn relation_params_report_unknown_field_and_rejected_id() {
    let kinds = BTreeMap::from([("chatId".to_string(), ScalarKind::Id)]);
    assert_eq!(
        normalize_relation_params(&object(json!({"authorId": "1"})), &kinds),
        Err(RelationParamsError::UnknownField("authorId".into()))
    );
    assert_eq!(
        normalize_relation_params(&object(json!({"chatId": ""})), &kinds),
        Err(RelationParamsError::Scalar(ScalarError::RejectedValue { context: "chatId".into() }))
    );
}

#[test]
fn duplicates_return_values_in_first_repeat_order() {
    let values: Vec<String> = ["a", "b", "a", "c", "b"].iter().map(|text| text.to_string()).collect();
    assert_eq!(duplicates(&values), vec!["a".to_string(), "b".to_string()]);
    assert_eq!(duplicates(&["a", "a", "a"]), vec!["a"]);
    assert!(duplicates(&["a", "b"]).is_empty());
}

#[test]
fn relation_scope_keys_match_spec() {
    let relation = relation_scope_key("message", "thread", &BTreeMap::from([("chatId".to_string(), "42".to_string())])).unwrap();
    let sorted = relation_scope_key(
        "message",
        "thread",
        &BTreeMap::from([("b".to_string(), "2".to_string()), ("a".to_string(), "1".to_string())]),
    )
    .unwrap();
    let empty = relation_scope_key("message", "thread", &BTreeMap::new()).unwrap();
    assert_eq!(relation.as_str(), "7:message6:thread15:{\"chatId\":\"42\"}");
    assert_eq!(relation.decompose().unwrap(), vec!["message", "thread", "{\"chatId\":\"42\"}"]);
    assert_eq!(sorted.decompose().unwrap(), vec!["message", "thread", "{\"a\":\"1\",\"b\":\"2\"}"]);
    assert_eq!(empty.decompose().unwrap(), vec!["message", "thread", "{}"]);
}

#[test]
fn relation_scope_keys_reject_empty_segments_and_unknown_fields() {
    assert_eq!(relation_scope_key("", "thread", &BTreeMap::new()), Err(ScopeKeyError::MissingSegment));
    assert_eq!(relation_scope_key("message", "", &BTreeMap::new()), Err(ScopeKeyError::MissingSegment));
    let kinds = BTreeMap::from([("chatId".to_string(), ScalarKind::Id)]);
    assert_eq!(
        relation_scope_key_from_values("message", "thread", &object(json!({"authorId": "1"})), &kinds),
        Err(ScopeKeyError::Params(RelationParamsError::UnknownField("authorId".into())))
    );
}

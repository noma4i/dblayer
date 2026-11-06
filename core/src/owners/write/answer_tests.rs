use super::answer::{decode_answer, pair_by_key, DecodeJob, Decoded};
use crate::store::Store;
use crate::schema_fixtures::{moments_spec, users_spec};

fn job(body: &str) -> DecodeJob { DecodeJob { op_id: 1, model: "moments".into(), action: "star".into(), temp: None, batch: None, body: body.into() } }

fn decoded(body: &str) -> Decoded {
    let db = Store::open_models(None, vec![moments_spec(), users_spec()], None).map(std::sync::Arc::new).unwrap();
    decode_answer(&db, &|_| Some("starMoment".to_string()), &job(body))
}

/// out_landing_committed_result: the fields of the returns block come from the root of the answer, a field the answer lacks is null, and an answer without the row still carries them.
#[test]
fn the_returns_block_is_read_from_the_root_of_the_answer() {
    let Decoded::Land { result, .. } = decoded(r#"{"data":{"starMoment":{"moment":{"id":"m1"},"viewer":{"id":"u1"},"successMessage":"Starred","other":1}}}"#) else { panic!("the answer lands") };
    assert_eq!(serde_json::from_str::<serde_json::Value>(&result.unwrap()).unwrap(), serde_json::json!({"successMessage": "Starred", "copies": null}));
    let Decoded::Land { rows, result, .. } = decoded(r#"{"data":{"starMoment":{"moment":null,"viewer":null,"successMessage":null,"copies":null}}}"#) else { panic!("the answer lands") };
    assert!(rows.is_empty());
    assert_eq!(serde_json::from_str::<serde_json::Value>(&result.unwrap()).unwrap(), serde_json::json!({"successMessage": null, "copies": null}));
}

fn object(text: &str) -> serde_json::Map<String, serde_json::Value> { serde_json::from_str(text).unwrap() }

/// out_sending_response_batch_items: every element row of the answer replaces the one waiting temporary row with its key, in the order of the answer; an element the answer lacks and an ambiguous key keep their temporary rows.
#[test]
fn out_sending_response_batch_items() {
    let waiting = vec![
        ("temp-a".to_string(), object(r#"{"clientKey":"a"}"#)),
        ("temp-b".to_string(), object(r#"{"clientKey":"b"}"#)),
        ("temp-c".to_string(), object(r#"{"clientKey":"c"}"#)),
        ("temp-d".to_string(), object(r#"{"clientKey":"c"}"#)),
    ];
    let correlate = vec!["clientKey".to_string()];
    let answer = vec![object(r#"{"id":"2","clientKey":"b"}"#), object(r#"{"id":"1","clientKey":"a"}"#), object(r#"{"id":"3","clientKey":"c"}"#), object(r#"{"id":"9","clientKey":"zz"}"#)];
    assert_eq!(pair_by_key(&correlate, &waiting, &answer), vec![("temp-b".to_string(), "2".to_string()), ("temp-a".to_string(), "1".to_string())]);
}

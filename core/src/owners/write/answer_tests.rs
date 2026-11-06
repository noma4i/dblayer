use super::answer::{decode_answer, DecodeJob, Decoded};
use crate::db::Db;
use crate::schema_fixtures::{moments_spec, users_spec};

fn job(body: &str) -> DecodeJob { DecodeJob { op_id: 1, model: "moments".into(), action: "star".into(), temp: None, body: body.into() } }

fn decoded(body: &str) -> Decoded {
    let db = Db::open_in_memory(vec![moments_spec(), users_spec()], None).unwrap();
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

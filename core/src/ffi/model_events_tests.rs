use super::{joined, DbError};

#[tokio::test]
async fn a_panicking_read_task_reaches_the_reader_as_an_error_not_as_the_end_of_the_stream() {
    let task = tokio::spawn(async { if true { panic!("read task failed") } None::<u8> });
    assert!(matches!(joined(task).await, Err(DbError::Invalid { .. })));
    let finished = tokio::spawn(async { Some(7u8) });
    assert!(matches!(joined(finished).await, Ok(Some(7))));
}

#[tokio::test]
async fn opening_a_refused_event_feed_preserves_the_error_kind_and_details() {
    use std::sync::Arc;
    use crate::transport::{MockTransport, SubscriptionEvent, SubscriptionFailure, TransportError};
    let cases = [
        (TransportError::Http { status: 403, body: "denied".into() }, DbError::Http { status: 403, detail: "denied".into() }),
        (TransportError::Decode("bad event".into()), DbError::Invalid { detail: "bad event".into() }),
        (TransportError::Network("disconnected".into()), DbError::Network { detail: "disconnected".into() }),
    ];
    for (error, expected) in cases {
        let transport = Arc::new(MockTransport::new());
        transport.script_subscription("Signal", vec![SubscriptionEvent::Failed(SubscriptionFailure::from_error(error))]);
        let mut model = crate::schema::ModelSpec::new("Notice", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
        model.listens.push(serde_json::from_value(serde_json::json!({"subscription":"Signal"})).unwrap());
        let db = super::Database::start_with(
            super::super::Config { path: None, data_version: None, cache_rows: 8 },
            serde_json::to_string(&vec![model]).unwrap(), None, Arc::new(crate::pacing::SystemClock),
            |_, _| Ok((Some(transport), None)),
        ).unwrap();
        db.register_operations(r#"[{"name":"Signal","document":"subscription Signal { signal { id } }","rootPath":"signal"}]"#.into()).unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), db.open_model_events("Notice".into())).await.unwrap();
        assert!(matches!(result, Err(ref error) if *error == expected), "expected {expected:?}");
    }
}

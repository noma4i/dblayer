use super::*;
use serde_json::{json, Value};

fn author(name: &str) -> super::Author {
    match name {
        "outbox" => super::Author::Outbox,
        "window" => super::Author::Window,
        other => panic!("the contract table has no author {other}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mock_contract_pairs_table() {
    let cases: Vec<Value> = serde_json::from_str(&std::fs::read_to_string("../fixtures/mock-contract/pairs.json").unwrap()).unwrap();
    for case in cases {
        let mock = MockTransport::new();
        for (operation, replies) in case["replies"].as_object().unwrap() {
            for reply in replies.as_array().unwrap() { mock.enqueue(operation, Reply::data(reply.clone())); }
        }
        let mut seen = Vec::new();
        for call in case["calls"].as_array().unwrap() {
            let request = GraphQLRequest::new(format!("mutation {0} {{ x }}", call[2].as_str().unwrap()), call[2].as_str().unwrap(), call[3].clone()).by(super::WireIds::new(author(call[0].as_str().unwrap()), call[1].as_u64().unwrap(), 1));
            let mock = mock.clone();
            seen.push(match tokio::spawn(async move { mock.mutate(request).await }).await {
                Ok(Ok(response)) => format!("data:{}", response.data.unwrap_or(json!(null))),
                Ok(Err(_)) | Err(_) => "refused".to_string(),
            });
        }
        assert_eq!(json!(seen), case["expect"], "{}", case["name"]);
    }
}

/// C0: rows of the server contract that the pairs table does not carry: the reply follows the operation, not the order of the calls; a PUT nobody scripted is refused; an error without a code reaches the client unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn mock_contract_operation_put_and_extensions_rows() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!(1)));
    mock.enqueue("Pong", Reply::data(json!(2)));
    let call = |name: &str, id: u64| GraphQLRequest::new(format!("mutation {name} {{ x }}"), name, json!({})).by(super::WireIds::new(super::Author::Outbox, id, 1));
    let pong = mock.mutate(call("Pong", 1)).await.unwrap();
    let ping = mock.mutate(call("Ping", 2)).await.unwrap();
    assert_eq!((pong.data, ping.data), (Some(json!(2)), Some(json!(1))), "a reply belongs to its operation, whatever the call order");

    mock.enqueue("Bare", Reply::graphql_error_without_code("no code"));
    let answer = mock.mutate(call("Bare", 3)).await.unwrap();
    assert_eq!(answer.errors.len(), 1);
    assert!(answer.errors[0].extensions.is_none() && answer.errors[0].code().is_none(), "the double neither invents nor drops extensions");

    let file = std::env::temp_dir().join(format!("c0-put-{}", uuid::Uuid::new_v4()));
    std::fs::write(&file, b"x").unwrap();
    let request = PutRequest {
        url: "https://s3/unscripted".into(),
        headers: Vec::new(),
        file: PutFile { path: file.to_string_lossy().into_owned(), offset: 0, length: 1 },
        identity: TransferIdentity { operation_id: 1, item_id: "i".into(), step: TransferStep::Part { number: 1 } },
        cancel: CancelToken::new(),
        sent: std::sync::Arc::new(|_| {}),
    };
    assert!(matches!(mock.put(request).await, Err(TransportError::Unscripted(_))), "a PUT nobody scripted is refused");
}

/// P-SESS-23, sess_pair_document_diff: the pair carries the document in its body, so a second attempt of the same id and variables with another document is two bodies, not a repeat.
#[tokio::test(flavor = "multi_thread")]
async fn sess_pair_document_diff() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!(1)));
    let first = GraphQLRequest::new("query Ping { a }", "Ping", json!({ "n": 1 })).by(super::WireIds::new(super::Author::Outbox, 1, 1));
    assert_eq!(mock.mutate(first).await.unwrap().data, Some(json!(1)), "the first body of the pair is answered");
    let second = GraphQLRequest::new("query Ping { b }", "Ping", json!({ "n": 1 })).by(super::WireIds::new(super::Author::Outbox, 1, 2));
    let mock = mock.clone();
    let outcome = tokio::spawn(async move { mock.mutate(second).await }).await;
    assert!(outcome.is_err(), "sess_pair_document_diff: a second document under one pair is two bodies, not the stored reply: {outcome:?}");
}

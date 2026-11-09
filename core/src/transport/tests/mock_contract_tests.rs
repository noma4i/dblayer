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

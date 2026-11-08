use super::*;
use serde_json::json;

fn ping(n: u64) -> GraphQLRequest {
    GraphQLRequest::new("query Ping { ping }", "Ping", json!({"n": n}))
}

#[tokio::test]
async fn responder_answers_by_request_and_falls_back_to_the_queue_when_it_declines() {
    let mock = MockTransport::new();
    mock.enqueue("Ping", Reply::data(json!("queued")));
    mock.respond_with(|request| (request.variables["n"] == json!(1)).then(|| Reply::data(json!("answered"))));
    assert_eq!(mock.query(ping(1)).await.unwrap().data, Some(json!("answered")));
    assert_eq!(mock.query(ping(2)).await.unwrap().data, Some(json!("queued")));
    assert_eq!(mock.call_count("Ping"), 2);
}

#[tokio::test]
async fn responder_runs_for_every_request_so_a_stateful_reply_sees_each_one() {
    let mock = MockTransport::new();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();
    mock.respond_with(move |request| {
        sink.lock().unwrap().push(request.variables["n"].as_u64().unwrap());
        Some(Reply::data(json!(sink.lock().unwrap().len())))
    });
    for n in [5, 6, 7] {
        mock.mutate(ping(n)).await.unwrap();
    }
    assert_eq!(*seen.lock().unwrap(), [5, 6, 7]);
}

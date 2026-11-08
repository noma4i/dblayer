use crate::ffi::FakeTransport;
use super::mock::Mock;
use crate::ffi::TransportRequest;
use crate::transport::NetworkPurpose;

fn request() -> TransportRequest {
    TransportRequest { document: "query Q { q }".into(), operation_name: "Q".into(), variables_json: "{}".into(), purpose: NetworkPurpose::Standard }
}

/// Requirement: the mock answers a request only on the input of the trace. Observable: the result of the waiting call and the announced request. Assert: pending until answered, then the body; one announcement.
#[tokio::test]
async fn a_request_waits_until_the_trace_answers_it() {
    let mock = Mock::new();
    let waiting = { let mock = mock.clone(); tokio::spawn(async move { mock.query(request()).await }) };
    crate::test_wait::signalled(mock.arrived(), "the request reached the mock", || mock.count() != 0).await;
    assert!(!waiting.is_finished());
    assert_eq!(mock.announce().len(), 1);
    assert!(mock.announce().is_empty());
    assert!(mock.answer("r1", "{}"));
    assert_eq!(waiting.await.unwrap().unwrap(), "{}");
}

/// Requirement: a request the mock announced is already answerable, whatever the thread order. Observable: the result of `answer` for each announced id. Assert: every announced request is reached and its call returns the body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_announced_request_is_answerable_at_once() {
    let mock = Mock::new();
    let calls: Vec<_> = (0..400).map(|_| { let mock = mock.clone(); tokio::spawn(async move { mock.query(request()).await }) }).collect();
    let mut answered = 0;
    crate::test_wait::signalled(mock.arrived(), "every request was answered", || {
        for seen in mock.announce() {
            let id = seen.value["id"].as_str().unwrap().to_owned();
            assert!(mock.answer(&id, "{}"), "announced {id} was not answerable");
            answered += 1;
        }
        answered == calls.len()
    }).await;
    for call in calls { assert_eq!(call.await.unwrap().unwrap(), "{}"); }
}

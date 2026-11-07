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
    crate::test_wait::yield_until("the request reached the mock", || mock.count() != 0).await;
    assert!(!waiting.is_finished());
    assert_eq!(mock.announce().len(), 1);
    assert!(mock.announce().is_empty());
    assert!(mock.answer("r1", "{}"));
    assert_eq!(waiting.await.unwrap().unwrap(), "{}");
}

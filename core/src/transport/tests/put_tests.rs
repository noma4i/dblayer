use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;

fn request(url: &str, body: &[u8], cancel: &CancelToken, ticks: &Arc<Mutex<Vec<u64>>>) -> PutRequest {
    let sink = ticks.clone();
    let path = std::env::temp_dir().join(format!("put-test-{}", uuid::Uuid::new_v4()));
    std::fs::write(&path, body).unwrap();
    PutRequest {
        url: url.to_string(),
        headers: vec![("content-type".to_string(), "video/mp4".to_string())],
        file: PutFile { path: path.to_string_lossy().into_owned(), offset: 0, length: body.len() as u64 },
        identity: TransferIdentity { operation_id: 1, item_id: "item".into(), step: TransferStep::Part { number: 1 } },
        cancel: cancel.clone(),
        sent: Arc::new(move |bytes| sink.lock().unwrap().push(bytes)),
    }
}

#[test]
fn put_records_the_range_headers_and_reports_progress_in_steps() { crate::test_wait::paused(async {
    let mock = MockTransport::new();
    let ticks = Arc::new(Mutex::new(Vec::new()));
    mock.enqueue_put("https://s3/p1", PutScript::etag("abc").after(Duration::from_secs(4)));
    let response = mock.put(request("https://s3/p1", &[7; 8], &CancelToken::new(), &ticks)).await.unwrap();
    assert_eq!(response.etag.as_deref(), Some("abc"));
    assert_eq!(*ticks.lock().unwrap(), [2, 4, 6, 8]);
    let calls = mock.put_calls();
    assert_eq!(calls[0].body, [7; 8]);
    assert_eq!(calls[0].headers[0].0, "content-type");
}) }

#[test]
fn bridge_prop_strict_scripts_each_put_consumes_one_script_and_an_unscripted_put_fails() { crate::test_wait::paused(async {
    let mock = MockTransport::new();
    let ticks = Arc::default();
    mock.enqueue_put("u", PutScript::status(503));
    mock.enqueue_put("u", PutScript::etag("ok"));
    let cancel = CancelToken::new();
    assert!(matches!(mock.put(request("u", b"a", &cancel, &ticks)).await, Err(TransportError::Http { status: 503, .. })));
    assert_eq!(mock.put(request("u", b"a", &cancel, &ticks)).await.unwrap().etag.as_deref(), Some("ok"));
    assert!(matches!(mock.put(request("u", b"a", &cancel, &ticks)).await, Err(TransportError::Unscripted(url)) if url == "u"));
    assert!(matches!(mock.put(request("other", b"a", &cancel, &ticks)).await, Err(TransportError::Unscripted(url)) if url == "other"));
    assert_eq!(mock.put_count("u"), 3);
    assert_eq!(mock.put_count("other"), 1);
}) }

#[test]
fn cancel_interrupts_a_slow_put_and_parallel_puts_are_counted() { crate::test_wait::paused(async {
    let mock = MockTransport::new();
    let ticks = Arc::default();
    mock.enqueue_put("slow", PutScript::etag("x").after(Duration::from_secs(60)));
    mock.enqueue_put("slow", PutScript::etag("y").after(Duration::from_secs(60)));
    let cancel = CancelToken::new();
    let first = tokio::spawn({
        let (mock, request) = (mock.clone(), request("slow", b"abcd", &cancel, &ticks));
        async move { mock.put(request).await }
    });
    let second = tokio::spawn({
        let (mock, request) = (mock.clone(), request("slow", b"abcd", &cancel, &ticks));
        async move { mock.put(request).await }
    });
    crate::test_wait::signalled(mock.signal(), "both puts started", || mock.parallel_puts_peak() == 2).await;
    cancel.cancel();
    assert!(matches!(first.await.unwrap(), Err(TransportError::Interrupted(_))));
    assert!(matches!(second.await.unwrap(), Err(TransportError::Interrupted(_))));
    assert_eq!(mock.parallel_puts_peak(), 2);
}) }

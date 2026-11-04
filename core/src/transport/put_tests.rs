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
        tag: "1.0.1".to_string(),
        cancel: cancel.clone(),
        sent: Arc::new(move |bytes| sink.lock().unwrap().push(bytes)),
    }
}

#[tokio::test(start_paused = true)]
async fn put_records_the_range_headers_and_reports_progress_in_steps() {
    let mock = MockTransport::new();
    let ticks = Arc::new(Mutex::new(Vec::new()));
    mock.enqueue_put("https://s3/p1", PutReply::etag("abc").after(Duration::from_secs(4)));
    let response = mock.put(&super::test_admission(), request("https://s3/p1", &[7; 8], &CancelToken::new(), &ticks)).await.unwrap();
    assert_eq!(response.etag.as_deref(), Some("abc"));
    assert_eq!(*ticks.lock().unwrap(), [2, 4, 6, 8]);
    let calls = mock.put_calls();
    assert_eq!(calls[0].body, [7; 8]);
    assert_eq!(calls[0].headers[0].0, "content-type");
}

#[tokio::test(start_paused = true)]
async fn queued_replies_play_in_order_then_repeat_the_last_and_default_answers_with_an_etag() {
    let mock = MockTransport::new();
    let ticks = Arc::default();
    mock.enqueue_put("u", PutReply::status(503));
    mock.enqueue_put("u", PutReply::etag("ok"));
    let cancel = CancelToken::new();
    assert!(matches!(mock.put(&super::test_admission(), request("u", b"a", &cancel, &ticks)).await, Err(TransportError::Http { status: 503, .. })));
    assert_eq!(mock.put(&super::test_admission(), request("u", b"a", &cancel, &ticks)).await.unwrap().etag.as_deref(), Some("ok"));
    assert_eq!(mock.put(&super::test_admission(), request("u", b"a", &cancel, &ticks)).await.unwrap().etag.as_deref(), Some("ok"));
    assert_eq!(mock.put(&super::test_admission(), request("other", b"a", &cancel, &ticks)).await.unwrap().etag.as_deref(), Some("etag:other"));
    assert_eq!(mock.put_count("u"), 3);
}

#[tokio::test(start_paused = true)]
async fn cancel_interrupts_a_slow_put_and_parallel_puts_are_counted() {
    let mock = MockTransport::new();
    let ticks = Arc::default();
    mock.enqueue_put("slow", PutReply::etag("x").after(Duration::from_secs(60)));
    let cancel = CancelToken::new();
    let first = tokio::spawn({
        let (mock, request) = (mock.clone(), request("slow", b"abcd", &cancel, &ticks));
        async move { mock.put(&super::test_admission(), request).await }
    });
    let second = tokio::spawn({
        let (mock, request) = (mock.clone(), request("slow", b"abcd", &cancel, &ticks));
        async move { mock.put(&super::test_admission(), request).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    cancel.cancel();
    assert!(matches!(first.await.unwrap(), Err(TransportError::Interrupted(_))));
    assert!(second.await.unwrap().is_err());
    assert_eq!(mock.parallel_puts_peak(), 2);
}

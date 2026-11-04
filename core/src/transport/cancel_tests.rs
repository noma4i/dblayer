use super::*;

#[tokio::test]
async fn cancelled_resolves_after_cancel_and_for_late_waiters() {
    let token = CancelToken::new();
    assert!(!token.is_cancelled());
    let waiter = tokio::spawn({
        let token = token.clone();
        async move { token.cancelled().await }
    });
    token.cancel();
    waiter.await.unwrap();
    token.cancelled().await;
    assert!(token.is_cancelled());
}

#[test]
fn offline_blocks_until_online() { crate::test_wait::paused(async {
    let link = Connectivity::new(true);
    assert!(link.is_online());
    link.set_online(false);
    let waiting = tokio::spawn({
        let link = link.clone();
        async move { link.online().await }
    });
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    assert!(!waiting.is_finished());
    link.set_online(true);
    waiting.await.unwrap();
}) }

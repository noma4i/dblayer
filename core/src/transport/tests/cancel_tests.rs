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
    tokio::select! {
        biased;
        () = link.online() => panic!("online() returned while the link is offline"),
        () = std::future::ready(()) => {}
    }
    link.set_online(true);
    link.online().await;
}) }

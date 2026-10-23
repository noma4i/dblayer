use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::live::{follow, QueryListener};
use super::live_feed::LiveFeed;
use super::{DbError, LiveChange};
use crate::sync::Failures;
use crate::transport::{SessionCode, SubscriptionFailure, SubscriptionFailureKind, TransportError};

struct Tape(Mutex<Vec<LiveChange>>);

impl QueryListener for Tape {
    fn changed(&self, change: LiveChange) {
        self.0.lock().unwrap().push(change);
    }
}

fn failure() -> SubscriptionFailure {
    SubscriptionFailure { kind: SubscriptionFailureKind::Transport, error: TransportError::Session { code: SessionCode::Unauthenticated, detail: "session_unknown".into() }, close: None }
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_20_a_lagged_failure_channel_resyncs_from_the_subscription_status() {
    let tape = Arc::new(Tape(Mutex::new(Vec::new())));
    let feed = LiveFeed::new(tape.clone());
    feed.rows(LiveChange::whole(1, Vec::new()));
    let (sender, receiver) = tokio::sync::broadcast::channel(4);
    let recheck: Arc<dyn Fn() -> Option<SubscriptionFailure> + Send + Sync> = Arc::new(|| Some(failure()));
    let _task = follow(Failures { current: None, recheck, receiver, sources: vec!["mine".into()] }, feed);
    for number in 0..10 {
        sender.send(Some((format!("other{number}"), failure()))).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    let last = tape.0.lock().unwrap().last().cloned().unwrap();
    assert!(matches!(last.error, Some(DbError::Network { .. } | DbError::Session { .. } | DbError::Sync { .. })), "error after lag: {:?}", last.error);
    assert_eq!(last.error_kind.as_deref(), Some("subscription"));
}

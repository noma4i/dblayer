use std::sync::Arc;
use std::time::Duration;

use super::stream::tape::{Reader, Rows, Silent};
use super::stream::{follow, Frame, Request, Stream, StreamTable};
use super::DbError;
use crate::sync::Failures;
use crate::transport::{SessionCode, SubscriptionFailure, SubscriptionFailureKind, TransportError};

fn failure() -> SubscriptionFailure {
    SubscriptionFailure { kind: SubscriptionFailureKind::Transport, error: TransportError::Session { code: SessionCode::Unauthenticated, detail: "session_unknown".into() }, close: None }
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_20_a_lagged_failure_channel_resyncs_from_the_subscription_status() {
    let rows = Arc::new(Rows::default());
    let request = Request { model: "User".into(), relation: None, params_json: "{}".into(), entity: None };
    let stream = Stream::new(1, 0, Arc::default(), &Arc::<StreamTable>::default(), Arc::new(Silent), request);
    stream.rows(Frame::whole(1, Vec::new()));
    let (sender, receiver) = tokio::sync::broadcast::channel(4);
    let recheck: Arc<dyn Fn() -> Option<SubscriptionFailure> + Send + Sync> = Arc::new(|| Some(failure()));
    let _task = follow(Failures { current: None, recheck, receiver, sources: vec!["mine".into()] }, stream.clone());
    for number in 0..10 {
        sender.send(Some((format!("other{number}"), failure()))).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    rows.read_all(&stream);
    let state = rows.state().unwrap();
    assert!(matches!(&state.error, Some(DbError::Session { code, detail }) if *code == SessionCode::Unauthenticated && detail == "session_unknown"), "error after lag: {:?}", state.error);
    assert_eq!(state.error_kind.as_deref(), Some("subscription"));
}

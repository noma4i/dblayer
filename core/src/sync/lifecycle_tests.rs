use serde_json::json;

use super::rig::*;
use crate::operation::OperationStatus;
use crate::transport::Reply;

#[tokio::test(start_paused = true)]
async fn an_ended_session_parks_the_outbox_until_the_next_session_starts() {
    let rig = rig();
    let row = rig.db.create("moments", params(json!({"text": "unsent"}))).unwrap();
    rig.sync.session_ended();
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m1", "text": "unsent", "userId": null}})));
    rig.sync.flush().await.unwrap();
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert_eq!(rig.db.outbox().unwrap()[0].operation.status, OperationStatus::Pending);
    rig.sync.session_committed();
    rig.sync.flush().await.unwrap();
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert!(rig.db.find("moments", &row.id).unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn repeating_a_session_online_or_listening_event_adds_no_request() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.sync.load("moments", "feed", &Default::default(), Default::default()).await.unwrap();
    let _watch = rig.sync.watch("moments", "feed", &Default::default()).unwrap();
    rig.transport.set_session(Some("first"));
    rig.sync.session_committed();
    rig.sync.resume_settled().await;
    let settled = (rig.transport.call_count("MomentsFeed"), rig.transport.call_count("MomentListen"));
    for _ in 0..2 {
        rig.sync.session_committed();
        rig.sync.set_online(true);
        rig.sync.set_listening(true);
    }
    rig.sync.resume_settled().await;
    assert_eq!((rig.transport.call_count("MomentsFeed"), rig.transport.call_count("MomentListen")), settled);
}

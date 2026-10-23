use serde_json::json;

use super::super::rig::*;
use super::rig::*;
use crate::db::Db;
use crate::transport::Reply;

async fn displaced_chat(rig: &Rig) {
    load_first_page(rig).await;
    rig.db.create("messages", params(json!({"chatId": CHAT, "text": "mine", "createdAt": "2026-01-01T00:00:09.000Z"}))).unwrap();
    rig.transport.enqueue("CreateMessage", Reply::data(json!({"createMessage": message("m9", Some(3), 9)})));
    rig.sync.flush().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_shown_order_survives_closing_and_opening_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("chat.sqlite");
    {
        let rig = chat_rig_on(Db::open(&path, vec![messages_spec(true)], Some("v1")).unwrap());
        enqueue_page(&rig, vec![message("m1", Some(1), 1), message("m2", Some(2), 2), message("m5", Some(5), 5)], false, "c");
        displaced_chat(&rig).await;
        let (log, _window) = watch_thread(&rig, 50);
        assert_eq!(shown(&log), ["m1", "m2", "m5", "m9"]);
    }
    let reopened = chat_rig_on(Db::open(&path, vec![messages_spec(true)], Some("v1")).unwrap());
    let (log, _window) = watch_thread(&reopened, 50);
    assert_eq!(shown(&log), ["m1", "m2", "m5", "m9"]);
}

#[tokio::test(start_paused = true)]
async fn a_reset_gives_the_order_of_the_server_back() {
    let rig = chat_rig(true);
    enqueue_page(&rig, vec![message("m1", Some(1), 1), message("m2", Some(2), 2), message("m5", Some(5), 5)], false, "c");
    enqueue_page(&rig, vec![message("m1", Some(1), 1), message("m2", Some(2), 2), message("m9", Some(3), 9), message("m5", Some(5), 5)], false, "c");
    displaced_chat(&rig).await;
    let (log, _window) = watch_thread(&rig, 50);
    assert_eq!(shown(&log), ["m1", "m2", "m5", "m9"]);

    rig.db.reset_all().unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!(shown(&log), Vec::<String>::new());

    load_first_page(&rig).await;
    assert_eq!(shown(&log), ["m1", "m2", "m9", "m5"]);
}

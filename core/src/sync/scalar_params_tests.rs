use super::error::SyncError;
use super::load::LoadOptions;
use super::rig::*;
use serde_json::json;

#[tokio::test(start_paused = true)]
async fn int_and_bool_params_go_out_as_json_values() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    let since = params(json!({"afterSequence": 120, "pinned": true}));
    rig.sync.load("moments", "since", &since, LoadOptions::default()).await.unwrap();
    assert_eq!(rig.transport.calls()[0].variables, json!({"afterSequence": 120, "pinned": true, "first": 2}));
}

#[tokio::test(start_paused = true)]
async fn typed_params_reject_text_of_the_wrong_kind() {
    let rig = rig();
    for wrong in [json!({"afterSequence": "120", "pinned": true}), json!({"afterSequence": 120, "pinned": "yes"}), json!({"afterSequence": 1.5, "pinned": true})] {
        let result = rig.sync.load("moments", "since", &params(wrong.clone()), LoadOptions::default()).await;
        assert!(matches!(result, Err(SyncError::Params(_))), "{wrong}: {result:?}");
    }
    assert_eq!(rig.transport.call_count("MomentsFeed"), 0);
}

#[tokio::test(start_paused = true)]
async fn each_typed_value_keys_its_own_window() {
    let rig = rig();
    let at = |n: i64| params(json!({"afterSequence": n, "pinned": false}));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["b"], false, "c2"));
    rig.sync.load("moments", "since", &at(5), LoadOptions::default()).await.unwrap();
    rig.sync.load("moments", "since", &at(6), LoadOptions::default()).await.unwrap();
    assert_eq!(member_ids(&rig, "moments", "since", &at(5)), ["a"]);
    assert_eq!(member_ids(&rig, "moments", "since", &at(6)), ["b"]);
    assert_ne!(rig.sync.window_key("moments", "since", &at(5)).unwrap(), rig.sync.window_key("moments", "since", &at(6)).unwrap());
}

#[tokio::test(start_paused = true)]
async fn ascending_and_descending_windows_never_share_a_cursor() {
    let rig = rig();
    let same = params(json!({"afterSequence": 5, "pinned": false}));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a1", "a2"], true, "up2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["d1", "d2"], true, "down2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a3"], false, "up3"));
    rig.sync.load("moments", "since", &same, LoadOptions::default()).await.unwrap();
    rig.sync.load("moments", "tail", &same, LoadOptions::default()).await.unwrap();
    rig.sync.load_more("moments", "since", &same).await.unwrap();
    let calls = rig.transport.calls();
    assert_eq!(calls[1].variables, json!({"afterSequence": 5, "pinned": false, "last": 2}));
    assert_eq!(calls[2].variables, json!({"afterSequence": 5, "pinned": false, "first": 2, "after": "up2"}));
    let up = rig.sync.window_key("moments", "since", &same).unwrap();
    let down = rig.sync.window_key("moments", "tail", &same).unwrap();
    assert!(up.ends_with(":forward") && down.ends_with(":backward"), "{up} {down}");
    assert_eq!(rig.db.shared.store.window(&down).unwrap().unwrap().cursor.as_deref(), Some("down2"));
}

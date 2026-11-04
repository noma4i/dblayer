use super::error::SyncError;
use super::load::LoadOptions;
use super::rig::*;
use crate::transport::Reply;
use serde_json::json;

#[test]
fn int_and_bool_params_go_out_as_json_values() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    let since = params(json!({"afterSequence": 120, "pinned": true}));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "since", &since, LoadOptions::default()).await.unwrap();
    assert_eq!(rig.transport.calls()[0].variables, json!({"afterSequence": 120, "pinned": true, "first": 2}));
}) }

#[test]
fn typed_params_reject_text_of_the_wrong_kind() { crate::test_wait::paused(async {
    let rig = rig();
    for wrong in [json!({"afterSequence": "120", "pinned": true}), json!({"afterSequence": 120, "pinned": "yes"}), json!({"afterSequence": 1.5, "pinned": true})] {
        let result = rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "since", &params(wrong.clone()), LoadOptions::default()).await;
        assert!(matches!(result, Err(SyncError::Params(_))), "{wrong}: {result:?}");
    }
    assert_eq!(rig.transport.call_count("MomentsFeed"), 0);
}) }

#[test]
fn each_typed_value_keys_its_own_window() { crate::test_wait::paused(async {
    let rig = rig();
    let at = |n: i64| params(json!({"afterSequence": n, "pinned": false}));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["b"], false, "c2"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "since", &at(5), LoadOptions::default()).await.unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "since", &at(6), LoadOptions::default()).await.unwrap();
    assert_eq!(member_ids(&rig, "moments", "since", &at(5)), ["a"]);
    assert_eq!(member_ids(&rig, "moments", "since", &at(6)), ["b"]);
    assert_ne!(rig.sync.window_key("moments", "since", &at(5)).unwrap(), rig.sync.window_key("moments", "since", &at(6)).unwrap());
}) }

#[test]
fn ascending_and_descending_windows_never_share_a_cursor() { crate::test_wait::paused(async {
    let rig = rig();
    let same = params(json!({"afterSequence": 5, "pinned": false}));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a1", "a2"], true, "up2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["d1", "d2"], true, "down2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a3"], false, "up3"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "since", &same, LoadOptions::default()).await.unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "tail", &same, LoadOptions::default()).await.unwrap();
    rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "since", &same).await.unwrap();
    let calls = rig.transport.calls();
    assert_eq!(calls[1].variables, json!({"afterSequence": 5, "pinned": false, "last": 2}));
    assert_eq!(calls[2].variables, json!({"afterSequence": 5, "pinned": false, "first": 2, "after": "up2"}));
    let up = rig.sync.window_key("moments", "since", &same).unwrap();
    let down = rig.sync.window_key("moments", "tail", &same).unwrap();
    assert!(up.ends_with(":forward") && down.ends_with(":backward"), "{up} {down}");
    assert_eq!(rig.db.shared.store.window(&down).unwrap().unwrap().cursor.as_deref(), Some("down2"));
}) }

#[test]
fn a_typed_param_beside_a_column_keys_the_window_and_fills_the_column() { crate::test_wait::paused(async {
    let rig = rig();
    let at = |n: i64| params(json!({"userId": "u1", "afterSequence": n}));
    rig.transport.enqueue("MomentsByUser", Reply::data(json!({"moments": [{"id": "a", "text": "a"}]})));
    rig.transport.enqueue("MomentsByUser", Reply::data(json!({"moments": [{"id": "b", "text": "b"}]})));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "user_since", &at(5), LoadOptions::default()).await.unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "user_since", &at(6), LoadOptions::default()).await.unwrap();
    assert_eq!(rig.transport.calls()[0].variables, json!({"userId": "u1", "afterSequence": 5}));
    assert_eq!(member_ids(&rig, "moments", "user_since", &at(5)), ["a"]);
    assert_eq!(member_ids(&rig, "moments", "user_since", &at(6)), ["b"]);
    let row = rig.db.find("moments", &crate::identity::RowId::new("a").unwrap()).unwrap().unwrap();
    assert_eq!(row.text("userId"), Some("u1"));
}) }

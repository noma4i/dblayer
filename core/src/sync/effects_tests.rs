use serde_json::{json, Value};

use crate::db::spec_fixtures::{join, members, scope_key, specs, ROOM_ORDER};
use crate::db::Db;
use crate::identity::RowId;
use crate::syncstate::SyncState;
use crate::transport::{MockTransport, Reply, TransportError};

use super::rig::{params, put, rig_without_operations, Rig};
use super::{OperationDef, SyncConfig};

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn effects_rig() -> Rig {
    let rig = rig_without_operations(Db::open_in_memory(specs(), None).unwrap(), MockTransport::new(), SyncConfig::default());
    for (name, root) in [("BlockPerson", "blockPerson"), ("UnblockPerson", "unblockPerson"), ("LeaveRoom", "leaveRoom"), ("AddMembers", "addMembers"), ("RemoveMembers", "removeMembers"), ("ForwardNote", "forwardNote"), ("SendGift", "sendGift")] {
        rig.sync.register(OperationDef::new(name, &format!("operation {name} {{ x }}"), root));
    }
    rig
}

fn act(rig: &Rig, model: &str, action: &str, id: &str, args: Value) -> i64 {
    rig.db.act(model, action, &rid(id), params(json!({})), params(args)).unwrap().operation
}

fn blocked(rig: &Rig) -> Vec<String> {
    members(&rig.db, "people", &scope_key(&rig.db, "people", "blocked", json!({})))
}

fn thread(rig: &Rig, room: &str) -> Vec<String> {
    members(&rig.db, "notes", &scope_key(&rig.db, "notes", "thread", json!({"roomId": room})))
}

fn balance(rig: &Rig) -> Option<Value> {
    rig.db.find("wallets", &rid("w1")).unwrap().unwrap().get("balance").cloned()
}

fn unreached() -> Reply {
    Reply::fail(TransportError::Interrupted("reset".into()))
}

fn refusal() -> Reply {
    Reply::graphql_error("not allowed", "FORBIDDEN")
}

#[tokio::test(start_paused = true)]
async fn a_membership_effect_is_there_before_the_answer_and_a_refusal_takes_back_only_that_action() {
    let rig = effects_rig();
    put(&rig, "people", json!({"id": "p1", "name": "a"}));
    put(&rig, "people", json!({"id": "p2", "name": "b"}));
    act(&rig, "people", "block", "p1", json!({}));
    act(&rig, "people", "block", "p2", json!({}));
    assert_eq!(blocked(&rig), ["p2", "p1"]);
    rig.transport.enqueue("BlockPerson", refusal());
    rig.transport.enqueue("BlockPerson", Reply::data(json!({"blockPerson": {"ok": true}})));
    rig.sync.flush().await.unwrap();
    assert_eq!(blocked(&rig), ["p2"]);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_membership_effect_stays_when_the_delivery_is_unknown_and_when_the_server_answers() {
    for (case, reply, state, open) in [("unknown", unreached(), SyncState::DeliveryUnknown, 1), ("answered", Reply::data(json!({"blockPerson": {"ok": true}})), SyncState::Synced, 0)] {
        let rig = effects_rig();
        put(&rig, "people", json!({"id": "p1", "name": "a"}));
        act(&rig, "people", "block", "p1", json!({}));
        rig.transport.enqueue("BlockPerson", reply);
        rig.sync.flush().await.unwrap();
        assert_eq!(blocked(&rig), ["p1"], "{case}");
        assert_eq!(rig.db.sync_info("people", &rid("p1")).unwrap().state, state, "{case}");
        assert_eq!(rig.db.outbox().unwrap().len(), open, "{case}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_refused_unblock_puts_the_member_back_with_the_parameters_of_its_selection() {
    let rig = effects_rig();
    put(&rig, "rooms", json!({"id": "r1", "title": "t"}));
    put(&rig, "people", json!({"id": "p1", "name": "a"}));
    let roster = scope_key(&rig.db, "people", "roster", json!({"roomId": "r1", "order": ROOM_ORDER}));
    join(&rig.db, "people", &roster, "p1", "a");
    act(&rig, "rooms", "remove_members", "r1", json!({"personIds": ["p1"]}));
    assert!(members(&rig.db, "people", &roster).is_empty());
    rig.transport.enqueue("RemoveMembers", refusal());
    rig.sync.flush().await.unwrap();
    assert_eq!(members(&rig.db, "people", &roster), ["p1"]);
}

#[tokio::test(start_paused = true)]
async fn a_created_row_of_another_model_is_there_before_the_answer_and_the_answer_row_replaces_it() {
    for (case, reply) in [("refused", refusal()), ("unknown", unreached()), ("answered", Reply::data(json!({"forwardNote": {"note": {"id": "n9", "roomId": "b", "body": "hello"}}})))] {
        let rig = effects_rig();
        put(&rig, "notes", json!({"id": "n1", "roomId": "a", "body": "hello"}));
        act(&rig, "notes", "forward", "n1", json!({"targetRoom": "b"}));
        let before = thread(&rig, "b");
        assert!(before.len() == 1 && rid(&before[0]).is_temp(), "{case}");
        rig.transport.enqueue("ForwardNote", reply);
        rig.sync.flush().await.unwrap();
        match case {
            "refused" => assert!(thread(&rig, "b").is_empty() && rig.db.find("notes", &rid(&before[0])).unwrap().is_none()),
            "unknown" => assert_eq!(thread(&rig, "b"), before),
            _ => {
                assert_eq!(thread(&rig, "b"), ["n9"]);
                assert!(rig.db.find("notes", &rid(&before[0])).unwrap().is_none());
            }
        }
        assert_eq!(rig.db.find("notes", &rid("n1")).unwrap().unwrap().text("body"), Some("hello"), "{case}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_gift_takes_the_price_and_adds_a_note_before_the_answer_and_the_text_comes_from_the_answer_only() {
    let args = json!({"giftId": "g", "roomId": "a", "price": 30});
    let sent = Reply::data(json!({"sendGift": {"person": {"id": "p1", "name": "a"}, "note": {"id": "n7", "roomId": "a", "body": "gift"}, "successMessage": "Thanks"}}));
    for (case, reply) in [("refused", refusal()), ("unknown", unreached()), ("answered", sent)] {
        let rig = effects_rig();
        put(&rig, "people", json!({"id": "p1", "name": "a"}));
        put(&rig, "wallets", json!({"id": "w1", "balance": 100}));
        join(&rig.db, "wallets", &scope_key(&rig.db, "wallets", "current", json!({})), "w1", "a");
        let operation = act(&rig, "people", "send_gift", "p1", args.clone());
        assert_eq!(balance(&rig), Some(json!(70)), "{case}");
        assert_eq!(thread(&rig, "a").len(), 1, "{case}");
        rig.transport.enqueue("SendGift", reply);
        let answer = rig.sync.answer("people", operation).await;
        match case {
            "refused" => assert!(answer.is_err() && balance(&rig) == Some(json!(100)) && thread(&rig, "a").is_empty()),
            "unknown" => assert!(answer.is_err() && balance(&rig) == Some(json!(70)) && thread(&rig, "a").len() == 1),
            _ => {
                assert_eq!(answer.unwrap(), json!({"successMessage": "Thanks"}));
                assert_eq!((balance(&rig), thread(&rig, "a")), (Some(json!(70)), vec!["n7".to_string()]));
            }
        }
    }
}

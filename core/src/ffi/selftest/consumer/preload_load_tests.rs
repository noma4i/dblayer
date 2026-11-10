//! holes.md D-166 against window.md l.103/113/153/206: `load()` of a window the preload loaded or loads sends no second request, in every order of landing and load.
use serde_json::{json, Value};

use super::stand::Stand;

fn connection(count: bool) -> Value {
    let mut value = json!({"edges": [], "pageInfo": {"hasNextPage": false, "endCursor": null}});
    if count { value["totalCount"] = json!(0); }
    value
}

const KEYS: [(&str, &str, &str, &str, &str); 12] = [
    ("Campaign", "active", "{}", "activeCampaigns", ""),
    ("Moment", "owned", "{}", "myMoments", ""),
    ("Chat", "list", r#"{"list":"PRIMARY"}"#, "chats", "list"),
    ("Chat", "list", r#"{"list":"SECONDARY"}"#, "chats", "list"),
    ("Gift", "catalog", "{}", "gifts", ""),
    ("Moment", "compass", "{}", "meCompass", ""),
    ("Vibe", "list", "{}", "vibesList", ""),
    ("Moment", "feed", "{}", "feed", ""),
    ("WalletTransaction", "by_kind", r#"{"kind":"DEBIT"}"#, "transactions", "kindFilter"),
    ("WalletTransaction", "by_kind", r#"{"kind":"CREDIT"}"#, "transactions", "kindFilter"),
    ("User", "blocked", "{}", "blockedUsers", ""),
    ("User", "friends", "{}", "friendUsers", ""),
];

fn answers(stand: &Stand) {
    stand.answer("activeCampaigns", json!({"eligibleCampaigns": []}));
    stand.answer("vibesList", json!({"vibes": []}));
    stand.answer("gifts", json!({"gifts": connection(false)}));
    stand.answer("feed", json!({"feed": connection(false)}));
    stand.answer("myMoments", json!({"viewer": {"moments": connection(false)}}));
    stand.answer("meCompass", json!({"viewer": {"compassMoments": connection(false)}}));
    stand.answer("chats", json!({"viewer": {"chats": connection(true)}}));
    stand.answer("transactions", json!({"viewer": {"wallet": {"transactions": connection(false)}}}));
    stand.answer("blockedUsers", json!({"viewer": {"blockedUsers": connection(true)}}));
    stand.answer("friendUsers", json!({"viewer": {"friends": connection(true)}}));
}

fn asked_keys(stand: &Stand) -> Vec<String> {
    let mut keys: Vec<String> = stand.asked().iter().map(|(operation, variables)| {
        let tell = KEYS.iter().find(|key| key.3 == operation).map_or("", |key| key.4);
        format!("{operation}:{}", variables[tell].as_str().unwrap_or(""))
    }).collect();
    keys.sort();
    keys
}

fn expected() -> Vec<String> {
    let mut keys: Vec<String> = KEYS.iter().map(|(_, _, params, operation, tell)| format!("{operation}:{}", if tell.is_empty() { String::new() } else { serde_json::from_str::<Value>(params).unwrap().as_object().unwrap().values().next().unwrap().as_str().unwrap().to_string() })).collect();
    keys.sort();
    keys
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_fresh_load_orders() {
    const ALL: [&str; 10] = ["activeCampaigns", "myMoments", "chats", "gifts", "meCompass", "vibesList", "feed", "transactions", "blockedUsers", "friendUsers"];
    const ALTERNATE: [&str; 5] = ["activeCampaigns", "chats", "meCompass", "feed", "blockedUsers"];
    let held_by_order: [(&str, &[&str]); 4] = [("landed first", &[]), ("held", &ALL), ("half", &ALL[..6]), ("alternate", &ALTERNATE)];
    let orders = held_by_order.iter().flat_map(|order| [(order.0, order.1, false), (order.0, order.1, true)]);
    for (order, held, reversed) in orders {
        let order = format!("{order}{}", if reversed { ", loads reversed" } else { "" });
        let stand = Stand::prepare().await;
        answers(&stand);
        stand.hold(held);
        stand.sign_in().await;
        if held.is_empty() {
            stand.settle().await;
        } else if held.len() < ALL.len() {
            let unheld = KEYS.iter().filter(|key| !held.contains(&key.3)).count();
            stand.answered(unheld).await;
            crate::ffi::owners_drained(&stand.database).await.unwrap();
        }
        let epoch = stand.database.session_epoch();
        let mut keys = KEYS.to_vec();
        if reversed { keys.reverse(); }
        let loads: Vec<_> = keys.iter().map(|(model, relation, params, _, _)| stand.database.begin_load(epoch, model.to_string(), Some(relation.to_string()), params.to_string()).unwrap()).collect();
        stand.release(held);
        let mut outcomes = Vec::new();
        for load in &loads { outcomes.push(crate::ffi::selftest::rig_tests::bound(load.result()).await.is_ok()); }
        stand.settle().await;
        assert_eq!(outcomes, vec![true; 12], "{order}: every load() is Loaded");
        assert_eq!(asked_keys(&stand), expected(), "{order}: one request per key: {:?}", stand.asked());
    }
}

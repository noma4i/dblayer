use serde_json::json;

use super::flow_fixtures::{add_chat, add_lists};
use super::rig_tests::{bound, Rig, World};

/// P-WIN-fresh: a window that is fresh asks nothing, and a server event that names its relation marks it stale so that the next load asks again.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_event_that_rereads_a_relation_makes_its_next_load_ask_again() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| {
        add_chat(models);
        add_lists(models);
        let chat = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Chat").unwrap();
        chat["listens"].as_array_mut().unwrap().push(json!({"subscription": "RigEvents", "root": "rigEvents.Chat", "apply": {"effect": "reread", "via": "list"}}));
    }).await;
    let epoch = rig.database.session_epoch();
    let load = || bound(rig.database.load(epoch, "Chat".into(), "list".into(), json!({"list": "SECONDARY"}).to_string()));
    load().await.unwrap();
    load().await.unwrap();
    assert_eq!(rig.world.count("ChatList"), 1, "the fresh window asks nothing");
    rig.land("Chat", json!({"id": "SECONDARY"})).await;
    load().await.unwrap();
    assert_eq!(rig.world.count("ChatList"), 2, "the event marked the window stale");
}

//! D-495 (window.md, conditional preload): `preload when: { of:, field: }` keeps a preload scope silent while the stored singleton flag is not true and loads it once when the flag becomes true.

use serde_json::json;

use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};

/// A singleton `Viewer` whose `registered` flag gates the preload of `Note.feed`.
fn gated_feed(models: &mut serde_json::Value) {
    use crate::schema::{Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape};
    let mut viewer = ModelSpec::new("Viewer", vec![FieldSpec::scalar("id", crate::value::ScalarKind::Id), FieldSpec::scalar("registered", crate::value::ScalarKind::Bool).nullable().optional()]);
    let mut me = RelationSpec::global("me");
    me.remote = Some(crate::schema_fixtures::remote("ViewerMe", RemoteShape::Single, "5.minutes"));
    viewer.relations.push(me);
    let mut note = ModelSpec::new("Note", vec![FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(crate::schema_fixtures::remote("NotesFeed", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    feed.preload = Some(Default::default());
    note.relations.push(feed);
    let mut note = serde_json::to_value(note).unwrap();
    let relations = note["relations"].as_array_mut().unwrap();
    let feed = relations.iter_mut().find(|relation| relation["name"] == "feed").unwrap();
    feed["preload_when"] = json!({"of": "Viewer", "field": "registered"});
    let models = models.as_array_mut().unwrap();
    models.push(serde_json::to_value(viewer).unwrap());
    models.push(note);
}

/// P-WIN-40, P-WIN-41: no feed request while the flag is missing or false (also across a network return), exactly 1 when it becomes true, none after it turns false again. The network return stays inside the 5 minute freshness, so a fresh window adds no request.
#[tokio::test(flavor = "multi_thread")]
async fn preload_when_loads_once_after_the_singleton_flag_turns_true() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(gated_feed)).await;
    let mut operations: Vec<serde_json::Value> = serde_json::from_str(&super::rig_models::plain_operations()).unwrap();
    operations.push(json!({"name": "ViewerMe", "document": "query ViewerMe { viewer { id registered } }"}));
    operations.push(json!({"name": "NotesFeed", "document": "query NotesFeed { notes { id } }"}));
    rig.world.answer_with("NotesFeed", json!({"data": {"notes": []}}));
    bound(rig.database.register_operations(serde_json::to_string(&operations).unwrap())).await.unwrap();
    let epoch = rig.database.session_epoch();
    rig.settle().await;
    let mut seen = vec![("no viewer row", rig.world.count("NotesFeed"))];
    for registered in [false, true, false] {
        rig.clock.advance(1_000);
        rig.world.answer_with("ViewerMe", json!({"data": {"viewer": {"__typename": "Viewer", "id": "v1", "registered": registered}}}));
        bound(rig.database.refresh(epoch, "Viewer".into(), Some("me".into()), "{}".into())).await.unwrap();
        rig.settle().await;
        bound(rig.database.set_online(false)).await.unwrap();
        rig.clock.advance(60_000);
        bound(rig.database.set_online(true)).await.unwrap();
        rig.settle().await;
        seen.push((if registered { "registered" } else { "not registered" }, rig.world.count("NotesFeed")));
    }
    assert_eq!(seen, vec![("no viewer row", 0), ("not registered", 0), ("registered", 1), ("not registered", 1)], "feed requests after each stored flag and a network return");
    rig.database.close_now();
}

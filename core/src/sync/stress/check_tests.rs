use std::sync::Arc;

use serde_json::json;

use crate::db::Record;
use crate::sync::rig::*;
use crate::syncstate::SyncInfo;

use super::check;
use super::findings::Findings;
use super::observers::Universe;

fn tags(findings: &Findings) -> Vec<String> {
    findings.all().iter().map(|finding| format!("{}]", finding.split(']').next().unwrap_or_default())).collect()
}

fn universe_of(ids: &[&str]) -> Universe {
    let universe = Universe::default();
    universe.lock().unwrap().extend(ids.iter().map(|id| ("moments".to_string(), id.to_string())));
    universe
}

#[test]
fn an_untouched_database_passes_every_check() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "one"));
    let local = rig.db.as_app().create("moments", params(json!({"text": "two"}))).unwrap();
    rig.db.find("moments", &local.id).unwrap();
    let findings = Findings::default();
    check::queue(&rig.db, &findings);
    check::lists(&rig.db, &findings);
    check::cache(&rig.db, &universe_of(&["m1", local.id.as_str()]), &findings);
    assert_eq!(findings.all(), Vec::<String>::new());
}

#[test]
fn a_lost_row_sync_entry_turns_the_queue_check_red() {
    let rig = rig();
    rig.db.as_app().create("moments", params(json!({"text": "two"}))).unwrap();
    rig.db.execute_for_test("DELETE FROM row_sync");
    let findings = Findings::default();
    check::queue(&rig.db, &findings);
    assert_eq!(tags(&findings), ["[queue]"]);
}

#[test]
fn a_temp_row_without_its_operation_turns_the_queue_check_red() {
    let rig = rig();
    put(&rig, "moments", moment("temp-orphan", "left behind"));
    let findings = Findings::default();
    check::queue(&rig.db, &findings);
    assert_eq!(tags(&findings), ["[queue]"]);
}

#[test]
fn a_list_member_without_a_row_turns_the_lists_check_red() {
    let rig = rig();
    rig.db.execute_for_test("INSERT INTO scope_member (model, scope_key, row_id, order_key) VALUES ('moments', 'feed', 'ghost', 'a')");
    let findings = Findings::default();
    check::lists(&rig.db, &findings);
    assert_eq!(tags(&findings), ["[lists]"]);
}

#[test]
fn a_stale_cached_row_turns_the_cache_check_red() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "one"));
    let fresh = rig.db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap().unwrap();
    let mut data = fresh.data.clone();
    data.insert("text".to_string(), json!("stale"));
    let stale = Record { id: fresh.id.clone(), revision: fresh.revision - 1, data, sync: SyncInfo::default(), ephemeral: 0 };
    rig.db.shared.cache.lock().unwrap().put("moments", Arc::new(stale), fresh.revision);
    let findings = Findings::default();
    check::cache(&rig.db, &universe_of(&["m1"]), &findings);
    assert_eq!(tags(&findings), ["[cache]"]);
}

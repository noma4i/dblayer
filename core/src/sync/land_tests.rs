use super::land::{Landed, Landing};
use super::load::DiscardReason;
use super::normalize::Normalized;
use super::rig::*;
use crate::commit::ScopeRef;
use crate::identity::ModelKey;
use crate::transport::FetchAttempt;

fn landing(rig: &Rig, issued: i64, ids: &[&str]) -> Landing {
    let key = rig.sync.window_key("moments", "feed", &serde_json::Map::new()).unwrap();
    let scope_key = key.trim_end_matches(":forward").to_string();
    let roots = ids.iter().map(|id| moment(id, &format!("issued {issued}")).as_object().unwrap().clone()).collect();
    Landing {
        model: "moments".to_string(),
        scope: ScopeRef::new(ModelKey::new("moments"), crate::identity::ScopeKey::from_raw(&scope_key)),
        window_key: key.clone(),
        normalized: Normalized { roots, related: Vec::new(), dropped: 0, edges: Default::default() },
        is_first_page: true,
        plans_as_first: true,
        coverage: crate::schema::Coverage::Complete,
        edge_fields: Vec::new(),
        requested_cursor: None,
        page_info: None,
        attempt: FetchAttempt::new(key, issued, rig.db.generation(), rig.db.revision_seq().unwrap()),
    }
}

#[test]
fn newer_attempt_wins_when_responses_arrive_out_of_order() {
    let rig = rig();
    assert_eq!(rig.sync.inner.land(landing(&rig, 200, &["m1"])).unwrap(), Landed::Applied { rows: 1, invalid: 0 });
    let late = rig.sync.inner.land(landing(&rig, 100, &["m1", "m2"])).unwrap();
    assert_eq!(late, Landed::Discarded(DiscardReason::Superseded));
    assert_eq!(text_of(&rig, "moments", "m1"), Some("issued 200".to_string()));
    assert_eq!(text_of(&rig, "moments", "m2"), None);
    assert_eq!(member_ids(&rig, "moments", "feed", &serde_json::Map::new()), ["m1"]);
}

#[test]
fn later_attempt_replaces_an_earlier_one() {
    let rig = rig();
    rig.sync.inner.land(landing(&rig, 100, &["m1"])).unwrap();
    assert_eq!(rig.sync.inner.land(landing(&rig, 200, &["m1", "m2"])).unwrap(), Landed::Applied { rows: 2, invalid: 0 });
    assert_eq!(text_of(&rig, "moments", "m1"), Some("issued 200".to_string()));
}

#[tokio::test(start_paused = true)]
async fn inv_merge_03_a_response_read_before_the_server_got_the_edit_does_not_undo_a_confirmed_edit() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    let id = crate::identity::RowId::new("m1").unwrap();
    rig.db.update("moments", &id, params(serde_json::json!({"text": "mine"}))).unwrap();
    let stale = landing(&rig, 100, &["m1"]);
    rig.transport.enqueue("UpdateMoment", crate::transport::Reply::data(serde_json::json!({"updateMoment": {"id": "m1", "text": "mine", "userId": null}})));
    rig.sync.flush().await.unwrap();
    rig.sync.inner.land(stale).unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), Some("mine".to_string()));
}

#[tokio::test(start_paused = true)]
async fn inv_merge_03_a_response_does_not_undo_an_edit_that_is_still_unconfirmed() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    let id = crate::identity::RowId::new("m1").unwrap();
    rig.db.update("moments", &id, params(serde_json::json!({"text": "mine"}))).unwrap();
    rig.sync.inner.land(landing(&rig, 100, &["m1"])).unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), Some("mine".to_string()));
}

#[tokio::test(start_paused = true)]
async fn a_list_response_started_before_create_preserves_the_confirmed_member() {
    for coverage in [crate::schema::Coverage::Page, crate::schema::Coverage::Complete] {
        let rig = rig();
        let query_params = params(serde_json::json!({"userId": "u42"}));
        let key = rig.sync.window_key("moments", "by_user", &query_params).unwrap();
        let scope = ScopeRef::new(ModelKey::new("moments"), crate::identity::ScopeKey::from_raw(key.trim_end_matches(":forward")));
        let mut old = moment("m1", "old").as_object().unwrap().clone();
        old.insert("userId".into(), serde_json::json!("u42"));
        rig.db.replace(&crate::db::ReplaceRequest::new(scope.clone(), coverage), vec![old.clone()]).unwrap();
        let response = |issued| {
            let mut reply = landing(&rig, issued, &[]);
            reply.scope = scope.clone();
            reply.window_key = key.clone();
            reply.coverage = coverage;
            reply.normalized.roots = vec![old.clone()];
            reply.attempt = FetchAttempt::new(&key, issued, rig.db.generation(), rig.db.revision_seq().unwrap());
            reply
        };
        let stale = response(100);
        rig.db.create("moments", params(serde_json::json!({"text": "mine", "userId": "u42"}))).unwrap();
        rig.transport.enqueue("CreateMoment", crate::transport::Reply::data(serde_json::json!({"createMoment": {"id": "m2", "text": "mine", "userId": "u42"}})));
        rig.sync.flush().await.unwrap();
        assert_eq!(member_ids(&rig, "moments", "by_user", &query_params), ["m2", "m1"]);
        rig.sync.inner.land(stale).unwrap();
        assert_eq!(member_ids(&rig, "moments", "by_user", &query_params), ["m2", "m1"], "{coverage:?}");
        rig.sync.inner.land(response(200)).unwrap();
        assert_eq!(member_ids(&rig, "moments", "by_user", &query_params), ["m1"], "{coverage:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_11_a_response_read_before_the_server_got_the_destroy_does_not_bring_the_row_back() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.destroy("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap();
    let stale = landing(&rig, 100, &["m1"]);
    rig.transport.enqueue("DeleteMoment", crate::transport::Reply::data(serde_json::json!({"deleteMoment": {"id": "m1"}})));
    rig.sync.flush().await.unwrap();
    rig.sync.inner.land(stale).unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[test]
fn inv_merge_09_a_failing_window_write_leaves_no_landed_rows() {
    let rig = rig();
    rig.db.execute_for_test("CREATE TRIGGER refuse BEFORE INSERT ON query_window BEGIN SELECT RAISE(ABORT, 'refused'); END");
    assert!(rig.sync.inner.land(landing(&rig, 100, &["m1"])).is_err());
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    assert!(member_ids(&rig, "moments", "feed", &serde_json::Map::new()).is_empty());
}

#[test]
fn a_row_created_by_another_landing_after_the_fetch_began_still_joins_the_listed_scope() {
    let rig = rig();
    let listing = landing(&rig, 100, &["m1"]);
    put(&rig, "moments", moment("m1", "from another landing"));
    assert_eq!(rig.sync.inner.land(listing).unwrap(), Landed::Applied { rows: 1, invalid: 0 });
    assert_eq!(text_of(&rig, "moments", "m1"), Some("from another landing".to_string()));
    assert_eq!(member_ids(&rig, "moments", "feed", &serde_json::Map::new()), ["m1"]);
}

fn next_page_cost(size: usize) -> usize {
    let rig = rig();
    let ids: Vec<String> = (0..size).map(|index| format!("m{index:06}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let mut first = landing(&rig, 100, &ids);
    first.page_info = Some(crate::store::PageInfo { has_next_page: true, end_cursor: Some("c1".to_string()), total: None });
    rig.sync.inner.land(first).unwrap();
    let mut next = landing(&rig, 200, &["next1", "next2"]);
    next.is_first_page = false;
    next.plans_as_first = false;
    next.coverage = crate::schema::Coverage::Page;
    next.requested_cursor = Some("c1".to_string());
    let before = crate::identity::touches::total();
    assert_eq!(rig.sync.inner.land(next).unwrap(), Landed::Applied { rows: 2, invalid: 0 });
    let cost = crate::identity::touches::total() - before;
    assert_eq!(member_ids(&rig, "moments", "feed", &serde_json::Map::new()).len(), size + 2);
    cost
}

#[test]
fn landing_a_next_page_reads_the_same_members_at_every_list_size() {
    assert_eq!(next_page_cost(100), next_page_cost(10_000), "member reads at 100 and 10000 rows");
}

use super::land::{Landed, Landing};
use super::load::DiscardReason;
use super::normalize::Normalized;
use super::support::*;
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
        normalized: Normalized { roots, related: Vec::new(), dropped: 0 },
        is_first_page: true,
        plans_as_first: true,
        requested_cursor: None,
        page_info: None,
        attempt: FetchAttempt::new(key, issued, rig.db.generation(), rig.db.revision_seq().unwrap()),
    }
}

#[test]
fn newer_attempt_wins_when_responses_arrive_out_of_order() {
    let rig = rig();
    assert_eq!(rig.sync.inner.land(landing(&rig, 200, &["m1"])).unwrap(), Landed::Applied { rows: 1 });
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
    assert_eq!(rig.sync.inner.land(landing(&rig, 200, &["m1", "m2"])).unwrap(), Landed::Applied { rows: 2 });
    assert_eq!(text_of(&rig, "moments", "m1"), Some("issued 200".to_string()));
}

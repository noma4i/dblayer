use super::*;

// Rows of `window.md` for the pages of a window: the page that goes to the wire, the end of the pages, a refresh over a page after the first, the first load of a new epoch.

use crate::owners::read::codec::load_variables;
use crate::schema::{CursorSpec, PageDirection, RemoteShape};

pub(super) fn connection(direction: PageDirection, cursor: Option<CursorSpec>) -> RemoteShape { RemoteShape::Connection { direction, coverage: crate::schema::Coverage::Complete, max_pages: None, cursor } }

pub(super) fn variables(shape: &RemoteShape, cursor: Option<&str>) -> serde_json::Value { load_variables(&serde_json::Map::new(), shape, 20, cursor, &Default::default()) }

/// The variables of a page request: backward asks the last items and before the cursor, a server cursor goes as a number or a text.
#[test]
fn win_page_variables_by_direction_and_cursor() {
    {
        let mut twin = Twin::new();
        assert_eq!(requests(&twin.call(1, Goal::First)), 1, "a backward window asks its first page like a forward one");
        assert_eq!(variables(&connection(PageDirection::Backward, None), None), serde_json::json!({"last": 20}), "the first page of a backward window asks the last items");
    }
    {
        let mut twin = fresh_with_cursor();
        assert_eq!(requests(&twin.call(2, Goal::Next)), 1);
        let numeric = connection(PageDirection::Forward, Some(CursorSpec { variable: "afterId".into(), from: "id".into(), numeric: true }));
        assert_eq!(variables(&numeric, None), serde_json::json!({}), "the first page of a server cursor window carries no pair");
        assert_eq!(variables(&numeric, Some("42")), serde_json::json!({"afterId": 42}), "the next page carries the cursor as a number and no first or after");
        let text = connection(PageDirection::Forward, Some(CursorSpec { variable: "afterId".into(), from: "id".into(), numeric: false }));
        assert_eq!(variables(&text, Some("42")), serde_json::json!({"afterId": "42"}));
    }
    {
        let mut twin = fresh_with_cursor();
        assert_eq!(requests(&twin.call(2, Goal::Next)), 1);
        assert_eq!(variables(&connection(PageDirection::Backward, None), Some("c2")), serde_json::json!({"last": 20, "before": "c2"}), "the next page of a backward window asks before the cursor");
    }
}




#[test]
fn win_wait_load_more_result() {
    let mut twin = fresh();
    take(&twin.log);
    twin.post_call(w::WindowsInput::Load { key: key(), id: 7, goal: w::Goal::Next, force: false, store_revision: 0 });
    let end = runs(take(&twin.log));
    assert!(matches!(end.as_slice(), [WindowsEffect::Window { effect: WindowEffect::Outcome { id: 7, outcome: WindowOutcome::Loaded { has_more: false, fetched: false, .. } }, .. }]), "no cursor: the call ends at once with no page fetched: {end:?}");
    let mut more = fresh_with_cursor();
    more.call(2, Goal::Next);
    more.respond(None);
    take(&more.log);
    let request = more.ref_landing();
    more.post(w::WindowsInput::Committed(Committed { batch: request, revision: 40, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    let fetched = runs(take(&more.log));
    assert!(fetched.iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { outcome: WindowOutcome::Loaded { has_more: false, fetched: true, .. }, .. }, .. })), "a page that landed answers Loaded with the page fetched: {fetched:?}");
}

#[test]
fn win_loading_refresh_over_more() {
    let mut twin = fresh_with_cursor();
    twin.call(2, Goal::Next);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    let refreshed = twin.step(Input::Load { id: 3, goal: Goal::First, force: true });
    assert_eq!(requests(&refreshed), 1, "a refresh over a page after the first cancels it and asks page 1: {refreshed:?}");
    assert!(refreshed.iter().any(|effect| matches!(effect, Effect::Cancel { .. })));
    twin.finish(Some(2));
    assert!(twin.state().waiters(&key()).contains(&2), "the waiter of page 2 waits for its page after page 1 landed");
}

#[test]
fn win_reload_on_connect_flag() {
    for build in [fresh_watched as fn() -> Twin, loading_watched, landing, waiting] {
        let mut twin = build();
        twin.step(Input::InterestPlus { id: STREAM });
        let stage = twin.stage();
        take(&twin.log);
        twin.post_call(w::WindowsInput::ReloadOnConnect { key: key(), id: id64(STREAM) });
        assert!(runs(take(&twin.log)).is_empty() && twin.stage() == stage, "the flag changes no state and starts nothing");
    }
    let mut twin = fresh_watched();
    twin.post_call(w::WindowsInput::ReloadOnConnect { key: key(), id: id64(STREAM) });
    twin.step(Input::InterestMinus { id: STREAM });
    twin.step(Input::InterestPlus { id: STREAM });
    twin.post(w::WindowsInput::Online { online: Online(false), online_seq: 0 });
    twin.post(w::WindowsInput::Online { online: Online(true), online_seq: 1 });
    assert_eq!(twin.stage(), Some(WindowStage::Idle), "the flag ended with the Interest- of its id");
}

#[test]
fn win_prop_first_load_after_reset() {
    let mut twin = fresh();
    twin.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH + 1, identity: Some(1), generation: 3 }));
    twin.post_call(w::WindowsInput::Online { online: Online(true), online_seq: 1 });
    take(&twin.log);
    twin.post_call(w::WindowsInput::Load { key: key(), id: 9, goal: w::Goal::First, force: false, store_revision: 0 });
    twin.post(w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(1, vec!["r1".into()], None) });
    take(&twin.log);
    twin.post(w::WindowsInput::Committed(Committed { batch: 2, revision: 30, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    let done = runs(take(&twin.log));
    assert!(done.iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { id: 9, outcome: WindowOutcome::Loaded { revision: 30, .. } }, .. })), "the call gets its own Loaded: {done:?}");
}

#[test]
fn win_prop_direction_key() {
    use crate::schema::PageDirection;
    let forward = crate::store::window_key("Moment:feed", PageDirection::Forward);
    let backward = crate::store::window_key("Moment:feed", PageDirection::Backward);
    assert_ne!(forward, backward, "the window of a stored key is told by its direction: the cursor and the membership of one never reach the other");
}

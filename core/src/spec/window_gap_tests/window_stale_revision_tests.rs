use super::*;

/// The effects of a `Stale(Change)` of the commit at `revision`, fed in the current stage.
fn change_at(twin: &mut Twin, revision: i64) -> Vec<Effect> {
    take(&twin.log);
    twin.post(w::WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::all(), cause: w::StaleCause::Change { revision: Some(revision) } });
    twin.split(take(&twin.log)).0
}

/// D-454, P-WIN-08: a `Stale(Change)` of the commit at r costs a second load only when the load that answers the window was pinned before r, in every stage the change can meet the load (before the pin, Loading, Landing, Idle).
#[test]
fn win_change_covered_by_a_load_pinned_after_its_commit() {
    const BASE: u64 = 5;
    let mut wrong = Vec::new();
    for revision in [5_i64, 6] {
        let second = usize::from(revision > 5);
        let mut before_pin = Twin::new();
        before_pin.hold.store(true, Ordering::SeqCst);
        before_pin.pin_revision.store(BASE, Ordering::SeqCst);
        before_pin.watch();
        let mut asked = requests(&change_at(&mut before_pin, revision));
        asked += requests(&before_pin.step(Input::Read(Read::None)));
        asked += requests(&before_pin.finish(None));
        let mut loading = Twin::new();
        loading.pin_revision.store(BASE, Ordering::SeqCst);
        loading.watch();
        let mut asked_loading = requests(&change_at(&mut loading, revision));
        asked_loading += requests(&loading.finish(None));
        let mut landing = Twin::new();
        landing.pin_revision.store(BASE, Ordering::SeqCst);
        landing.watch();
        landing.respond(None);
        let mut asked_landing = requests(&change_at(&mut landing, revision));
        asked_landing += requests(&landing.land());
        let mut idle = Twin::new();
        idle.pin_revision.store(BASE, Ordering::SeqCst);
        idle.watch();
        idle.finish(None);
        let asked_idle = requests(&change_at(&mut idle, revision));
        for (stage, got) in [("before the pin", asked - 1), ("Loading", asked_loading), ("Landing", asked_landing), ("Idle", asked_idle)] {
            if got != second { wrong.push(format!("change at {revision}, base {BASE}, {stage}: {got} more requests, the rule asks {second}")); }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// D-454: a load that does not read every row of the window (a page after the first, the marker check of a `media` window) covers no change: a committed change acts as a change outside the Store.
#[test]
fn win_change_not_covered_by_a_partial_load() {
    use super::window_media_tests::{answer_marker, media_checking, SAVED};
    let run = |revision: Option<i64>, media: bool| -> Vec<Effect> {
        let mut twin = if media { media_checking() } else { Twin::new() };
        if !media {
            twin.watch();
            twin.finish(Some(2));
            twin.call(1, Goal::Next);
        }
        take(&twin.log);
        twin.post(w::WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::all(), cause: w::StaleCause::Change { revision } });
        let mut seen = twin.split(take(&twin.log)).0;
        seen.extend(if media { answer_marker(&mut twin, SAVED) } else { twin.respond(None) });
        if seen.iter().any(|effect| matches!(effect, Effect::Commit { .. })) { seen.extend(twin.land()); }
        seen
    };
    for (name, media) in [("page 2", false), ("media marker check", true)] {
        assert_eq!(run(Some(0), media), run(None, media), "{name}: a change of a commit older than the pin acts as an uncommitted change");
    }
}

/// P-WIN-08 (one extra load per uncovered `Stale`, only after the landing of the load it met): a load left by its last reader takes its pending reread with it; the next load reads the window once.
#[test]
fn win_left_load_drops_its_reread() {
    let mut twin = Twin::new();
    twin.watch();
    twin.step(Input::Stale { in_keys: true });
    twin.step(Input::InterestMinus { id: STREAM });
    let asked = requests(&twin.watch());
    assert_eq!(asked, 1, "the window is not fresh: the next reader loads it");
    twin.respond(None);
    assert_eq!(requests(&twin.land()), 0, "the landing of the new load asks nothing more");
}

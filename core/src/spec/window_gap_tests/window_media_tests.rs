use super::*;

// Rows of `window.md` for a key with `media`: the marker, the tail and the batch that lands the rows of a chain with the marker that confirmed them.

pub(super) const SAVED: Marker3 = Marker3 { sequence: 5, revision: 1, count: 3 };

pub(super) fn marker(sequence: i64, revision: i64, count: i64) -> Marker3 { Marker3 { sequence, revision, count } }

pub(super) fn asked(effects: &[Effect]) -> usize { effects.iter().filter(|effect| matches!(effect, Effect::Request { .. } | Effect::Marker { .. } | Effect::Tail { .. })).count() }

pub(super) fn commits(effects: &[Effect]) -> usize { effects.iter().filter(|effect| matches!(effect, Effect::Commit { .. })).count() }

pub(super) fn answer_marker(twin: &mut Twin, marker: Marker3) -> Vec<Effect> {
    let (request, attempt) = twin.ref_request();
    twin.step(Input::MarkerAnswer { request, attempt, marker })
}

/// A saved media window that is not fresh, watched by a stream: the check of its marker is in flight.
pub(super) fn media_checking() -> Twin {
    let mut twin = Twin::media();
    twin.hold.store(true, Ordering::SeqCst);
    twin.watch();
    twin.step(Input::Read(Read::Media { marker: SAVED, members: 3 }));
    twin
}

/// The saved window has the newest rows of the server after sequence 5: the tail is in flight.
pub(super) fn media_tail() -> Twin {
    let mut twin = media_checking();
    answer_marker(&mut twin, marker(9, 1, 3));
    twin
}

/// A window without a saved marker: the whole chain is read, the newest sequence of the server is 9.
pub(super) fn media_full() -> Twin {
    let mut twin = Twin::media();
    twin.watch();
    answer_marker(&mut twin, marker(9, 1, 0));
    twin
}

#[test]
fn win_media_marker_check() {
    let mut saved = Twin::media();
    saved.hold.store(true, Ordering::SeqCst);
    saved.watch();
    let effects = saved.step(Input::Read(Read::Media { marker: SAVED, members: 3 }));
    assert!(effects.iter().any(|effect| matches!(effect, Effect::Marker { up_to: 5, .. })), "the marker is asked up to the saved sequence: {effects:?}");
    assert_eq!(saved.stage(), Some(WindowStage::Loading));
    let effects = Twin::media().watch();
    assert!(effects.iter().any(|effect| matches!(effect, Effect::Marker { up_to: 0, .. })), "without a saved marker the check starts at 0: {effects:?}");
    let effects = Twin::new().watch();
    assert!(effects.iter().any(|effect| matches!(effect, Effect::Request { page: 1, .. })) && asked(&effects) == 1, "a key without media asks its page: {effects:?}");
}

#[test]
fn win_media_marker_same() {
    let mut twin = media_checking();
    let effects = answer_marker(&mut twin, SAVED);
    assert_eq!(commits(&effects), 1);
    assert!(twin.commit.contains("WindowMarker") && !twin.commit.contains("Window {"), "the batch has the marker and no rows: {}", twin.commit);
    twin.land();
    assert_eq!((twin.stage(), twin.phase()), (Some(WindowStage::Idle), Phase::Loaded));
}

#[test]
fn win_media_marker_does_not_prove_a_window_that_opened_stale() {
    let mut twin = Twin::media();
    twin.hold.store(true, Ordering::SeqCst);
    twin.watch();
    twin.step(Input::Read(Read::MediaMarked { marker: SAVED, members: 3 }));
    let effects = answer_marker(&mut twin, SAVED);
    assert!(matches!(&effects[..], [Effect::Request { page: 1, .. }]), "the marker proved a window that opened with a refetch debt: {effects:?}");
}

#[test]
fn win_media_stale_mark_survives_failed_load() {
    let mut twin = Twin::media();
    twin.hold.store(true, Ordering::SeqCst);
    twin.watch();
    twin.step(Input::Read(Read::MediaMarked { marker: SAVED, members: 3 }));
    twin.step(Input::Online(false));
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    let effects = twin.step(Input::Online(true));
    assert!(effects.iter().any(|effect| matches!(effect, Effect::Marker { up_to: 5, .. })), "{effects:?}");
    let effects = answer_marker(&mut twin, SAVED);
    assert!(matches!(&effects[..], [Effect::Request { page: 1, .. }]), "the mark was spent by the failed load and the marker proved a stale window: {effects:?}");
}

#[test]
fn win_media_tail() {
    let mut twin = media_checking();
    let effects = answer_marker(&mut twin, marker(9, 1, 3));
    assert!(matches!(&effects[..], [Effect::Tail { after: 5, .. }]), "{effects:?}");
}

#[test]
fn win_media_full() {
    let mut unsaved = Twin::media();
    unsaved.watch();
    let effects = answer_marker(&mut unsaved, marker(9, 1, 0));
    assert!(matches!(&effects[..], [Effect::Request { page: 1, .. }]), "no saved marker: {effects:?}");
    for (case, answer) in [("the revision moved", marker(9, 2, 3)), ("the counts moved", marker(9, 1, 4))] {
        let mut twin = media_checking();
        let effects = answer_marker(&mut twin, answer);
        assert!(matches!(&effects[..], [Effect::Request { page: 1, .. }]), "{case}: {effects:?}");
    }
}

#[test]
fn win_media_next() {
    let effects = media_full().respond(Some(2));
    assert!(matches!(&effects[..], [Effect::Request { page: 1, .. }]), "the next page of the full read: {effects:?}");
    let effects = media_tail().respond(Some(2));
    assert!(matches!(&effects[..], [Effect::Tail { after: 5, .. }]), "the next page of the tail: {effects:?}");
}

#[test]
fn win_media_bad_cursor() {
    for (case, mut twin, rows) in [("full", media_full(), false), ("tail", media_tail(), true)] {
        let (request, attempt) = twin.ref_request();
        let effects = twin.step(Input::MoreWithoutCursor { request, attempt });
        assert_eq!(commits(&effects), 0, "{case}: nothing lands");
        assert_eq!((twin.stage(), twin.phase()), (Some(WindowStage::Idle), if rows { Phase::Loaded } else { Phase::Failed }), "{case}");
    }
    let mut repeated = media_full();
    repeated.respond(Some(2));
    let effects = repeated.respond(Some(2));
    assert_eq!(commits(&effects) + asked(&effects), 0, "a cursor that did not move ends the chain");
    assert_eq!((repeated.stage(), repeated.phase()), (Some(WindowStage::Idle), Phase::Failed));
}

#[test]
fn win_media_confirm() {
    let effects = media_full().respond(None);
    assert!(matches!(&effects[..], [Effect::Marker { up_to: 9, .. }]), "the last page asks the marker up to the newest sequence: {effects:?}");
}

#[test]
fn win_media_confirmed() {
    let mut full = media_full();
    full.respond(None);
    let effects = answer_marker(&mut full, marker(9, 1, 1));
    assert_eq!(commits(&effects), 1);
    assert!(full.commit.contains("WindowMarker") && full.commit.contains("Window {"), "the rows and the marker are one batch: {}", full.commit);
    full.land();
    assert_eq!((full.stage(), full.phase()), (Some(WindowStage::Idle), Phase::Loaded));
    let mut tail = media_tail();
    tail.respond(None);
    let effects = answer_marker(&mut tail, marker(9, 1, 4));
    assert_eq!(commits(&effects), 1, "the saved rows and the rows of the tail are the counts of the marker");
}

#[test]
fn win_media_confirm_retry() {
    let mut twin = media_full();
    twin.respond(None);
    let effects = answer_marker(&mut twin, marker(9, 1, 5));
    assert!(matches!(&effects[..], [Effect::ScheduleRetry { .. }]), "{effects:?}");
    assert_eq!(twin.phase(), Phase::Loading);
    let request = twin.ref_request().0;
    let effects = twin.step(Input::Retry { request });
    assert!(matches!(&effects[..], [Effect::Marker { up_to: 9, .. }]), "the same confirm is asked again: {effects:?}");
}

#[test]
fn win_media_confirm_failed() {
    let mut twin = media_full();
    twin.respond(None);
    answer_marker(&mut twin, marker(9, 1, 5));
    let request = twin.ref_request().0;
    twin.step(Input::Retry { request });
    let effects = answer_marker(&mut twin, marker(9, 1, 5));
    assert_eq!(commits(&effects), 0, "a chain that did not confirm lands nothing");
    assert_eq!((twin.stage(), twin.phase()), (Some(WindowStage::Idle), Phase::Failed));
}

#[test]
fn win_media_abort_keeps() {
    let mut twin = media_tail();
    twin.respond(Some(2));
    let effects = twin.step(Input::Online(false));
    assert!(commits(&effects) == 0 && effects.iter().any(|effect| matches!(effect, Effect::Cancel { .. })), "{effects:?}");
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    let effects = twin.step(Input::Online(true));
    assert!(effects.iter().any(|effect| matches!(effect, Effect::Marker { up_to: 5, .. })), "the saved marker is the same: the check starts again from it: {effects:?}");
}

/// P-WIN-22: the rows and the marker land in 1 batch after a confirm that matched; every abort leaves them as they were, a fresh window asks nothing.
#[test]
fn win_prop_media_atomic() {
    type Abort = fn(u64, u32) -> Input;
    let aborts: [(&str, Abort); 7] = [
        ("offline", |_, _| Input::Online(false)),
        ("epoch ended", |_, _| Input::EpochEnded),
        ("closing", |_, _| Input::Closing),
        ("the last reader leaves", |_, _| Input::InterestMinus { id: STREAM }),
        ("a permanent error", |request, attempt| Input::TransportError { request, attempt, failure: Failure::Permanent }),
        ("a dropped request", |request, attempt| Input::Dropped { request, attempt }),
        ("an invalid cursor twice", |request, attempt| Input::TransportError { request, attempt, failure: Failure::Permanent }),
    ];
    for (case, abort) in aborts {
        for mut twin in [media_tail(), media_full()] {
            twin.respond(Some(2));
            let (request, attempt) = twin.ref_request();
            let effects = twin.step(abort(request, attempt));
            assert_eq!(commits(&effects), 0, "{case}: no batch before a confirm");
        }
    }
    let mut twin = media_full();
    twin.respond(None);
    answer_marker(&mut twin, marker(9, 1, 1));
    twin.land();
    let effects = twin.step(Input::Load { id: 1, goal: Goal::First, force: false });
    assert_eq!(asked(&effects), 0, "a fresh window asks nothing");
}

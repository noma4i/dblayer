use super::window_fresh_tests::{opening_twin, saved, LIFE};
use super::*;

// Rows of `window.md` for the preload interest, power saving and a call refused offline.

pub(super) fn preload_twin(saving: bool) -> Twin {
    let mut twin = Twin::new();
    if saving {
        twin.step(Input::PowerSaving(true));
    }
    twin.step(Input::InterestPlus { id: PRELOAD });
    twin
}

/// A preload key asks the network only when its window is not fresh: a fresh live window, a saved fresh window, an old or missing saved window, the same key again.
#[test]
fn win_preload_asks_only_when_not_fresh() {
    {
        let mut twin = fresh_watched();
        assert_eq!(requests(&twin.step(Input::InterestPlus { id: PRELOAD })), 0, "the preload joins the interest of a fresh window and asks nothing");
        assert_eq!(twin.state().interest(&key()), [u64::from(STREAM), w::PRELOAD]);
        let mut stale = fresh_watched();
        stale.step(Input::InterestMinus { id: STREAM });
        stale.step(Input::Stale { in_keys: true });
        assert_eq!(requests(&stale.step(Input::InterestPlus { id: PRELOAD })), 1, "the preload on a window that is not fresh loads it");
    }
    {
        for (read, asked) in [(saved(NOW - 10_000, false), 0), (saved(NOW - LIFE, false), 1), (Read::None, 1)] {
            let mut twin = opening_twin();
            twin.step(Input::InterestPlus { id: PRELOAD });
            assert_eq!(requests(&twin.step(Input::Read(read))), asked, "a preload key with the saved window {read:?}");
        }
    }
    {
        let mut twin = fresh_watched();
        twin.step(Input::InterestPlus { id: PRELOAD });
        take(&twin.log);
        twin.post(w::WindowsInput::PreloadKeys { keys: vec![key()] });
        assert!(take(&twin.log).is_empty(), "a preload that already holds the key changes nothing");
    }
}




/// Power saving changes nothing for a window with a reader, a call, a request in flight, a landing or no interest.
#[test]
fn win_power_saving_defers_only_preload_windows() {
    {
        let mut twin = preload_twin(true);
        assert_eq!(requests(&twin.watch()), 1, "a reader does not wait for power saving");
        let mut called = preload_twin(true);
        assert_eq!(requests(&called.call(1, Goal::First)), 1, "a call does not wait for power saving");
    }
    {
        let mut twin = loading_watched();
        assert_eq!(twin.step(Input::PowerSaving(true)), none(), "a request that left goes on");
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
        let mut landing = landing();
        assert_eq!(landing.step(Input::PowerSaving(true)), none());
        assert_eq!(landing.stage(), Some(WindowStage::Landing));
    }
    {
        let mut twin = fresh_watched();
        for on in [true, false] {
            assert_eq!(twin.step(Input::PowerSaving(on)), none(), "a window with a reader ignores power saving");
        }
        let mut waiting = waiting();
        assert_eq!(waiting.step(Input::PowerSaving(true)), none());
        assert_eq!(waiting.stage(), Some(WindowStage::Waiting));
    }
    {
        let mut twin = fresh();
        for on in [true, false] {
            assert_eq!(twin.step(Input::PowerSaving(on)), none(), "a window without interest ignores power saving");
        }
    }
}





#[test]
fn win_busy_offline_refused() {
    for mut twin in [loading_watched(), landing()] {
        let stage = twin.stage();
        assert_eq!(twin.step(Input::OfflineRefused), none(), "a window that loads or lands is not changed");
        assert_eq!(twin.stage(), stage);
    }
}

#[test]
fn win_prop_preload_power_saving() {
    let mut twin = Twin::new();
    twin.step(Input::PowerSaving(true));
    let mut asked = requests(&twin.step(Input::InterestPlus { id: PRELOAD }));
    asked += requests(&twin.step(Input::Online(false))) + requests(&twin.step(Input::Online(true)));
    asked += requests(&twin.step(Input::Stale { in_keys: true }));
    assert_eq!(asked, 0, "a key only the preload watches asks nothing while the device saves power");
    assert_eq!(requests(&twin.step(Input::PowerSaving(false))), 1, "the end of power saving loads the key once");
    assert_eq!(requests(&twin.step(Input::PowerSaving(true))) + requests(&twin.step(Input::PowerSaving(false))), 0, "a request in flight is not asked again");
}

//! D-491 bounce rule in Windows: the send count of a load survives `Online(false)` + `Online(true)`.

use crate::ffi::DbError;
use crate::owners::contract::{ErrorClass, Online, RequestError};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::windows::{Goal, WindowEffect, WindowKey, Windows, WindowsEffect, WindowsInput, WindowsPeers, WindowsState};
use crate::owners::store::{StoreInput, StoreWriter};
use crate::runtime::{Address, Effect, StateOwner};

struct Run {
    state: WindowsState,
    pin: Option<u64>,
    request: Option<(u64, u32)>,
}

impl Run {
    fn step(&mut self, input: WindowsInput) {
        let effects = Windows::step(&mut self.state, input, crate::runtime::Time(0));
        for effect in effects {
            match effect {
                Effect::Run(WindowsEffect::Window { effect: WindowEffect::Request { request, attempt, .. }, .. }) => self.request = Some((request, attempt)),
                Effect::Send(outbound) if outbound.to == 0 => {
                    if let Ok(StoreInput::Pin { request_id, .. }) = outbound.payload.into_any().downcast::<StoreInput>().map(|input| *input) { self.pin = Some(request_id); }
                }
                _ => {}
            }
        }
    }

    fn pinned(&mut self) -> u64 {
        let loading = self.pin.take().expect("the load pins its request");
        self.step(WindowsInput::Pinned { request_id: loading, base_revision: 1 });
        loading
    }
}

/// The sends of one window load against a network that always fails; `bounce_after` = the failed send after which the path is replaced while the retry waits.
fn sends_of_a_failing_load(bounce_after: Option<u32>) -> u32 {
    let state = WindowsState::new(1, Some(true), WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) }).with_local_request_ids();
    let mut run = Run { state, pin: None, request: None };
    let key = WindowKey { model: "Item".into(), name: "items".into() };
    run.step(WindowsInput::Load { key: key.clone(), id: 1, goal: Goal::First, force: false, store_revision: 0 });
    run.step(WindowsInput::WindowRead { key, read_id: 1, result: Ok(None) });
    let mut loading = run.pinned();
    let (mut sends, mut online_seq) = (0, 0);
    while let Some((request, attempt)) = run.request.take() {
        sends += 1;
        assert!(sends < 10, "the load never gives up");
        run.step(WindowsInput::Failure { request, attempt, error: RequestError { class: ErrorClass::Network, failure: DbError::Dropped { detail: "down".into() } } });
        if bounce_after == Some(sends) {
            run.step(WindowsInput::Online { online: Online(false), online_seq });
            online_seq += 1;
            run.step(WindowsInput::Online { online: Online(true), online_seq });
            loading = run.pinned();
        } else {
            run.step(WindowsInput::RetryDue { request: loading });
        }
    }
    sends
}

#[test]
fn windows_path_bounce_keeps_attempts() {
    let plain = sends_of_a_failing_load(None);
    assert_eq!(plain, 3, "QueryNetwork allows 3 sends");
    for after in 1..plain {
        assert_eq!(sends_of_a_failing_load(Some(after)), plain, "a bounce after send {after} is the same logical request: the count goes on, it does not start again");
    }
}

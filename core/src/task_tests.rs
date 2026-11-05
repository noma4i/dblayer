use std::sync::Arc;

use tokio::runtime::Handle;

use super::*;
use crate::ffi::admission::{Leases, Origin};

fn held() -> Option<u64> { admitted().map(|admitted| admitted.value()) }

#[tokio::test(flavor = "multi_thread")]
async fn every_boundary_carries_the_epoch_it_is_given_and_a_detached_one_carries_none() {
    let handle = Handle::current();
    let admission = Arc::new(Leases::default()).admit(Origin::Explicit, "test call").unwrap();
    let carried = Some(admission.value());
    let carry = || Carry::from(admission.clone());
    let thread_held = |carry: Carry| {
        let (sent, got) = std::sync::mpsc::channel();
        spawn_thread(carry, "dblayer-carry-test".into(), move || { let _ = sent.send(held()); }).unwrap();
        crate::test_wait::recv_within(&got)
    };
    let seen = (
        spawn(carry(), async { held() }).await.unwrap(),
        spawn_on(carry(), &handle, async { held() }).await.unwrap(),
        spawn_blocking(carry(), held).await.unwrap(),
        spawn_blocking_on(carry(), &handle, held).await.unwrap(),
        spawn(Carry::Detached, async { held() }).await.unwrap(),
        spawn_on(Carry::Detached, &handle, async { held() }).await.unwrap(),
        spawn_blocking(Carry::Detached, held).await.unwrap(),
        thread_held(carry()),
        thread_held(Carry::Detached),
    );
    assert_eq!(seen, (carried, carried, carried, carried, None, None, None, carried, None));
}

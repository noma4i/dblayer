use std::sync::Arc;

use tokio::runtime::Handle;

use super::*;
use crate::lifecycle::Lifecycle;

fn held() -> Option<u64> { pinned_epoch().map(|(_, epoch)| epoch) }

#[tokio::test(flavor = "multi_thread")]
async fn every_inheriting_boundary_carries_the_epoch_and_every_unpinned_one_drops_it() {
    let handle = Handle::current();
    let seen = ORIGIN.scope((Arc::new(Lifecycle::default()), 7), async {
        let mut children = Children::new();
        children.spawn(async { held() });
        let callback = bind(|()| held());
        let foreign = std::thread::spawn(move || callback(()));
        (
            spawn(async { held() }).await.unwrap(),
            spawn_on(&handle, async { held() }).await.unwrap(),
            spawn_blocking(held).await.unwrap(),
            spawn_blocking_on(&handle, held).await.unwrap(),
            crate::test_wait::within(children.join_next()).await.unwrap().unwrap(),
            crate::test_wait::join_within(foreign),
            spawn_unpinned(async { held() }).await.unwrap(),
            spawn_on_unpinned(&handle, async { held() }).await.unwrap(),
            spawn_blocking_unpinned(held).await.unwrap(),
        )
    }).await;
    let carried = Some(7);
    assert_eq!(seen, (carried, carried, carried, carried, carried, carried, None, None, None));
}

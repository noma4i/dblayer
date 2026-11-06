use super::OpenSlot;

#[test]
fn the_slot_is_taken_until_the_lease_is_released_or_dropped() {
    let slot = OpenSlot::new();
    let first = slot.acquire().unwrap();
    assert!(slot.acquire().is_none());
    first.release();
    first.release();
    let second = slot.acquire().unwrap();
    assert!(slot.acquire().is_none());
    drop(second);
    assert!(slot.acquire().is_some());
}

#[cfg(dblayer_loom)]
mod model {
    use loom::thread;

    use crate::test_wait::join_within;

    use super::*;
    use crate::runtime::sync::{Arc, AtomicUsize, Ordering};

    #[test]
    fn loom_two_opens_never_hold_the_slot_together_and_the_slot_is_never_lost() {
        loom::model(|| {
            let slot = OpenSlot::new();
            let held = Arc::new(AtomicUsize::new(0));
            let opens: Vec<_> = (0..2)
                .map(|_| {
                    let (slot, held) = (slot.clone(), held.clone());
                    thread::spawn(move || {
                        let Some(lease) = slot.acquire() else { return false };
                        assert_eq!(held.fetch_add(1, Ordering::SeqCst), 0, "two leases at once");
                        held.fetch_sub(1, Ordering::SeqCst);
                        lease.release();
                        true
                    })
                })
                .collect();
            let opened = opens.into_iter().map(|open| join_within(open)).filter(|opened| *opened).count();
            assert!(opened >= 1, "a free slot was refused to everybody");
            assert!(slot.acquire().is_some(), "the slot came back after both opens");
        });
    }

    #[test]
    fn loom_release_against_drop_frees_the_slot_once() {
        loom::model(|| {
            let slot = OpenSlot::new();
            let lease = Arc::new(slot.acquire().unwrap());
            let again = lease.clone();
            let releasing = thread::spawn(move || again.release());
            drop(lease);
            join_within(releasing);
            let next = slot.acquire();
            assert!(next.is_some());
            assert!(slot.acquire().is_none());
        });
    }
}

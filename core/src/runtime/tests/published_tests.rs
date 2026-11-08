use crate::test_wait::join_within;
use super::sync::{Arc, AtomicBool, Ordering};
use super::PublishedCell;

#[test]
fn a_waiter_gets_the_change_made_after_it_started() {
    let cell = Arc::new(PublishedCell::<u32>::default());
    let flag = Arc::new(AtomicBool::new(false));
    assert_eq!(cell.get(), None);
    let (checked, first_check) = std::sync::mpsc::channel();
    let changer = {
        let (cell, flag) = (cell.clone(), flag.clone());
        std::thread::spawn(move || {
            crate::test_wait::recv_within(&first_check);
            flag.store(true, Ordering::SeqCst);
            cell.notify();
        })
    };
    assert_eq!(cell.wait_until(|| { let _ = checked.send(()); flag.load(Ordering::SeqCst).then_some(9) }), 9);
    join_within(changer);
    cell.publish(4);
    assert_eq!(cell.read(|value| value.copied()), Some(4));
}

#[cfg(dblayer_loom)]
mod model {
    use loom::thread;

    use super::*;

    #[test]
    fn loom_a_change_between_the_check_and_the_wait_is_never_missed() {
        loom::model(|| {
            let cell = Arc::new(PublishedCell::<u32>::default());
            let flag = Arc::new(AtomicBool::new(false));
            let changer = {
                let (cell, flag) = (cell.clone(), flag.clone());
                thread::spawn(move || {
                    flag.store(true, Ordering::SeqCst);
                    cell.notify();
                })
            };
            let found = cell.wait_until(|| flag.load(Ordering::SeqCst).then_some(1));
            join_within(changer);
            assert_eq!(found, 1);
        });
    }

    #[test]
    fn loom_publish_wakes_the_waiter_and_the_last_value_stays() {
        loom::model(|| {
            let cell = Arc::new(PublishedCell::<u32>::default());
            let flag = Arc::new(AtomicBool::new(false));
            let publisher = {
                let (cell, flag) = (cell.clone(), flag.clone());
                thread::spawn(move || {
                    cell.publish(1);
                    flag.store(true, Ordering::SeqCst);
                    cell.publish(2);
                })
            };
            let found = cell.wait_until(|| flag.load(Ordering::SeqCst).then_some(()));
            join_within(publisher);
            assert_eq!(found, ());
            assert_eq!(cell.get(), Some(2));
        });
    }
}

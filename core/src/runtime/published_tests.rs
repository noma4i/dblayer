use std::time::Duration;

use super::sync::{Arc, AtomicBool, Ordering};
use super::PublishedCell;

#[test]
fn a_waiter_gets_the_change_made_after_it_started_and_the_timeout_when_nothing_changes() {
    let cell = Arc::new(PublishedCell::<u32>::default());
    let flag = Arc::new(AtomicBool::new(false));
    assert_eq!(cell.get(), None);
    assert_eq!(cell.wait_for(Duration::from_millis(20), || None::<u32>), None);
    let changer = {
        let (cell, flag) = (cell.clone(), flag.clone());
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            flag.store(true, Ordering::SeqCst);
            cell.notify();
        })
    };
    assert_eq!(cell.wait_for(Duration::from_secs(5), || flag.load(Ordering::SeqCst).then_some(9)), Some(9));
    changer.join().unwrap();
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
            let found = cell.wait_for(Duration::from_secs(3600), || flag.load(Ordering::SeqCst).then_some(1));
            changer.join().unwrap();
            assert_eq!(found, Some(1));
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
            let found = cell.wait_for(Duration::from_secs(3600), || flag.load(Ordering::SeqCst).then_some(()));
            publisher.join().unwrap();
            assert_eq!(found, Some(()));
            assert_eq!(cell.get(), Some(2));
        });
    }
}

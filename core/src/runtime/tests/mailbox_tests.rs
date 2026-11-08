use std::time::Duration;

use super::mailbox::Mailbox;

#[test]
fn take_returns_at_once_when_the_schedule_is_due_and_keeps_the_order_of_pushes() {
    let mailbox = Mailbox::<u32>::new();
    assert!(mailbox.take(|| Some(Duration::ZERO)).is_empty());
    mailbox.push(1);
    mailbox.push(2);
    assert_eq!(mailbox.take(|| None), vec![1, 2]);
    assert_eq!(mailbox.wakeups(), 0, "a take that found commands did not sleep");
}

#[cfg(dblayer_loom)]
mod model {
    use loom::thread;

    use crate::test_wait::join_within;

    use super::*;
    use crate::runtime::sync::Arc;

    #[test]
    fn loom_every_push_wakes_the_taker() {
        loom::model(|| {
            let mailbox = Arc::new(Mailbox::<u32>::new());
            let producer = {
                let mailbox = mailbox.clone();
                thread::spawn(move || {
                    mailbox.push(1);
                    mailbox.push(2);
                })
            };
            let mut got = Vec::new();
            while got.len() < 2 {
                got.extend(mailbox.take(|| None));
            }
            join_within(producer);
            assert_eq!(got, vec![1, 2]);
        });
    }

    #[test]
    fn loom_two_producers_lose_nothing_and_keep_their_own_order() {
        loom::model(|| {
            let mailbox = Arc::new(Mailbox::<u32>::new());
            let producers: Vec<_> = [10, 20]
                .into_iter()
                .map(|base| {
                    let mailbox = mailbox.clone();
                    thread::spawn(move || {
                        mailbox.push(base);
                        mailbox.push(base + 1);
                    })
                })
                .collect();
            let mut got = Vec::new();
            while got.len() < 4 {
                got.extend(mailbox.take(|| None));
            }
            producers.into_iter().for_each(|producer| join_within(producer));
            for base in [10, 20] {
                let own: Vec<u32> = got.iter().copied().filter(|n| *n / 10 == base / 10).collect();
                assert_eq!(own, vec![base, base + 1]);
            }
            assert!(mailbox.take(|| Some(Duration::ZERO)).is_empty());
        });
    }
}

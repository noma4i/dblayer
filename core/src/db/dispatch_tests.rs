use super::dispatch::{Effects, OrderedEffects};
use super::fixtures::*;

#[test]
fn ordered_effects_run_in_push_order_and_a_push_from_an_effect_only_queues() {
    use std::sync::{Arc, Mutex};
    let ordered = Arc::new(OrderedEffects::default());
    let log = Arc::new(Mutex::new(Vec::new()));
    let note = |text: &'static str| {
        let log = log.clone();
        move || log.lock().unwrap().push(text)
    };
    let mut first = Effects::default();
    first.push({
        let (ordered, log, nested_note) = (ordered.clone(), log.clone(), note("nested"));
        move || {
            log.lock().unwrap().push("first starts");
            let mut nested = Effects::default();
            nested.push(nested_note);
            ordered.push(nested);
            ordered.run();
            log.lock().unwrap().push("first ends");
        }
    });
    let mut second = Effects::default();
    second.push(note("second"));
    ordered.push(first);
    ordered.push(second);
    ordered.run();
    assert_eq!(*log.lock().unwrap(), ["first starts", "first ends", "second", "nested"]);
    assert!(ordered.is_idle());
}

#[test]
fn a_panicking_effect_does_not_wedge_the_ordered_runner() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    let ordered = OrderedEffects::default();
    let mut failing = Effects::default();
    failing.push(|| panic!("effect failed"));
    ordered.push(failing);
    assert!(catch_unwind(AssertUnwindSafe(|| ordered.run())).is_err());
    assert!(ordered.is_idle(), "the runner is released after the unwind");
    let ran = Arc::new(AtomicBool::new(false));
    let mut next = Effects::default();
    next.push({
        let ran = ran.clone();
        move || ran.store(true, Ordering::SeqCst)
    });
    ordered.push(next);
    ordered.run();
    assert!(ran.load(Ordering::SeqCst), "the next batch runs");
}

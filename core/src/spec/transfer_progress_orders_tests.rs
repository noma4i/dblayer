//! Orders of part results, errors and restarts for `data.md` P-DATA-43. Requirement -> trace -> shown progress.

use crate::test_wait::permutations;
use super::transfer_harness::*;
use crate::owners::write::transfer::Shown;

const KEY: &str = "Moment:r1:upload";

#[test]
fn data_orders_upload_progress_around_error_and_restart() {
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Event { FirstPart, Error, Restart, SecondPart }
    use Event::*;
    let at = |order: &[Event], wanted: Event| order.iter().position(|e| *e == wanted);
    let orders: Vec<_> = permutations(&[FirstPart, Error, Restart]).into_iter().filter(|order| at(order, Error) < at(order, Restart) && !(at(order, Error) < at(order, FirstPart) && at(order, FirstPart) < at(order, Restart))).collect();
    assert_eq!(orders.len(), 2);
    let levels = |out: Out| -> Vec<u32> { out.shows().into_iter().filter_map(|show| if let Shown::Uploading { progress } = show { Some(progress) } else { None }).collect() };
    for order in orders {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 4);
        let mut committed = 0;
        for event in order.iter().copied().chain([SecondPart]) {
            let immediate = match event {
                FirstPart => levels(twin.step_ok(id, 1, None)),
                Error => levels(twin.step_error(id, true)),
                Restart => levels(twin.tick_retry(id)),
                SecondPart => levels(twin.step_ok(id, 2, None)),
            };
            match event {
                FirstPart | SecondPart => assert!(immediate.is_empty(), "{order:?}: a step result changes no value: {immediate:?}"),
                Error | Restart => assert!(immediate.iter().all(|level| *level == committed), "{order:?}: {event:?} shows only the committed value: {immediate:?}"),
            }
            while twin.has_pending() {
                let shown = levels(twin.commit());
                assert!(shown.iter().all(|level| *level >= committed), "{order:?}: a committed part never lowers the value: {shown:?} after {committed}");
                committed = shown.last().copied().unwrap_or(committed);
            }
        }
        assert_eq!(committed, 50, "{order:?}: two committed parts of four");
    }
}

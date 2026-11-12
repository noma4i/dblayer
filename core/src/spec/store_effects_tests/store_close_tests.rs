use super::*;

/// `Close` closes the reads first, the connections after the last read, `StoreClosed` once.
#[test]
fn store_close_waits_for_the_reads() {
    let mut twin = Twin::live();
    assert_eq!(twin.feed(Request::Close), vec![Action::CloseReads]);
    assert_eq!(twin.state().mode(), StoreMode::Open);
    twin.feed(Request::ReadsClosed(vec![4, 5]));
    twin.feed(Request::ReadFinished(4));
    assert_eq!(twin.state().mode(), StoreMode::Open, "a read is still open");
    let done = twin.feed(Request::ReadFinished(5));
    assert!(done.contains(&Action::CloseConnections) && done.contains(&Action::StoreClosed));
    assert_eq!(twin.state().mode(), StoreMode::Closed);
    twin.feed(Request::ReadFinished(5));
    twin.feed(Request::Close);
    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, StoreEffect::StoreClosed)).count(), 1);
}

#[test]
fn store_close_without_reads_closes_at_once() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    let done = twin.feed(Request::ReadsClosed(vec![]));
    assert!(done.contains(&Action::CloseConnections) && done.contains(&Action::StoreClosed));
}

/// A close that arrives while a batch is in SQLite waits for `Written`.
#[test]
fn store_close_waits_for_the_write() {
    let mut twin = Twin::live();
    twin.feed(Request::Commit { id: 1, batch: Twin::moment_batch() });
    assert!(twin.feed(Request::Close).is_empty(), "Close waits for the write");
    assert!(twin.feed(Request::Written { id: 1, result: Ok(1) }).contains(&Action::CloseReads));
    assert!(twin.feed(Request::ReadsClosed(vec![])).contains(&Action::StoreClosed));
}

#[test]
fn store_closing_refuses_new_work() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::Closed));
    assert_eq!(twin.feed(Request::ClearAccount { epoch_old: 1, cause: Cause::SessionReset, generation_new: 1, erase_id: 1 }), vec![Action::ClearFailed { erase_id: 1, closed: true }]);
    assert_eq!(twin.feed(Request::Published { publication_id: 5, epoch: 2, generation: 0, active: true }), vec![Action::SaveFailed { publication_id: 5, closed: true }]);
}

/// A closed Store refuses commits and ignores every other request.
#[test]
fn store_closed_ignores_requests() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![]));
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::Closed));
    for request in [Request::Published { publication_id: 4, epoch: 3, generation: 0, active: true }, Request::EpochEnded { epoch: 1 }, Request::Written { id: 5, result: Ok(1) }, Request::Erased { io_id: 5, result: Ok(1) }, Request::ReadIntents { epoch: 1 }, Request::ReadsClosed(vec![]), Request::ReadFinished(2)] {
        assert!(twin.feed(request.clone()).is_empty(), "{request:?}");
    }
}

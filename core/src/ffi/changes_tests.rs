use super::*;
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;
use crate::test_wait::{join_within, mutex_free, spin_until, within, ForeignWaker, ProbeWaker};

#[tokio::test]
async fn a_slow_reader_receives_a_bounded_replacement_with_every_current_row() {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    let database = Database::open(super::super::Config { path: None, data_version: None, cache_rows: 16 }, serde_json::to_string(&vec![model]).unwrap(), None, None, None, None, None, None).await.unwrap();
    let query = r#"{"model":"Note","order":[{"field":"text","direction":"asc"}]}"#;
    let queue = database.changes(query.into()).unwrap();
    let mut ids = Vec::new();
    for number in 0..(CHANGE_QUEUE_LIMIT * 3) {
        ids.push(database.create("Note".into(), serde_json::json!({"text": number.to_string()}).to_string(), "{}".into()).await.unwrap().id);
        database.db.await_revision(database.db.revision_seq().unwrap());
    }
    database.update("Note".into(), ids[0].clone(), r#"{"text":"edited"}"#.into(), "{}".into()).await.unwrap();
    database.destroy("Note".into(), ids[1].clone()).await.unwrap();
    database.db.await_revision(database.db.revision_seq().unwrap());
    for _ in 0..CHANGE_QUEUE_LIMIT {
        database.create("Note".into(), r#"{"text":"new"}"#.into(), "{}".into()).await.unwrap();
        database.db.await_revision(database.db.revision_seq().unwrap());
        let state = crate::db::locked(&queue.state);
        if state.pending.len() == 1 && state.pending[0].full { break; }
    }
    assert!(crate::db::locked(&queue.state).pending.len() <= CHANGE_QUEUE_LIMIT, "slow consumer queue exceeded the core limit");
    let replacement = within(queue.next()).await.unwrap();
    assert!(replacement.full);
    let current = database.peek_query(query.into()).await.unwrap().unwrap();
    assert_eq!(replacement.rows, current.rows);
    let mut expected = current.rows;
    let from = expected[0].id.clone();
    expected[0].id = "canonical".into();
    let mut renamed = LiveChange::whole(current.revision + 1, vec![expected[0].clone()]);
    renamed.full = false;
    renamed.size = expected.len() as u32;
    renamed.row_indexes = vec![0];
    renamed.inserted.clear();
    renamed.updated = vec![0];
    renamed.replaced = vec![super::super::live::Replacement { from, to: "canonical".into() }];
    renamed.renamed = renamed.replaced.clone();
    queue.push(renamed);
    for index in 0..CHANGE_QUEUE_LIMIT { queue.push(LiveChange { error: Some(DbError::Network { detail: "lost".into() }), ..LiveChange::empty(current.revision + 2 + index as i64) }); }
    let replacement = within(queue.next()).await.unwrap();
    assert!(replacement.full);
    assert_eq!(replacement.rows, expected);
    assert!(matches!(replacement.error, Some(DbError::Network { .. })));
    queue.cancel();
    assert!(within(queue.next()).await.is_none());
}

#[test]
fn cancelling_while_a_delivered_change_wakes_the_reader_finishes_and_ends_the_stream() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    join_within(std::thread::spawn(|| {
        let queue = LiveChanges::new();
        let foreign = Arc::new(Mutex::new(()));
        let waker = ForeignWaker::new(foreign.clone());
        let mut next = Box::pin(queue.next());
        assert!(next.as_mut().poll(&mut Context::from_waker(&Waker::from(waker.clone()))).is_pending());
        let task_lock = foreign.lock().unwrap();
        let delivering = {
            let listener = Listener(Arc::downgrade(&queue));
            std::thread::spawn(move || listener.changed(LiveChange::empty(1)))
        };
        spin_until("the delivered change never woke the reader", || waker.entered());
        queue.cancel();
        drop(task_lock);
        join_within(delivering);
        assert!(waker.held_classes().is_empty());
        assert!(matches!(next.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(None)));
    }));
}

#[test]
fn changes_cancel_during_foreign_wake() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    join_within(std::thread::spawn(|| {
        let queue = LiveChanges::new();
        let foreign = Arc::new(Mutex::new(()));
        let waker = ForeignWaker::new(foreign.clone());
        let mut next = Box::pin(queue.next());
        assert!(next.as_mut().poll(&mut Context::from_waker(&Waker::from(waker.clone()))).is_pending());
        let task_lock = foreign.lock().unwrap();
        let cancelling = {
            let queue = queue.clone();
            std::thread::spawn(move || queue.cancel())
        };
        spin_until("the cancel never woke the reader", || waker.entered());
        Listener(Arc::downgrade(&queue)).changed(LiveChange::empty(1));
        drop(task_lock);
        join_within(cancelling);
        assert!(waker.held_classes().is_empty());
        assert!(matches!(next.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(None)));
        let state = crate::db::locked(&queue.state);
        assert!(state.closed && state.pending.is_empty() && state.rows.is_empty());
    }));
}

#[test]
fn every_stream_wake_runs_unlocked() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    enum Trigger { Value, Error, Cancel, Close }
    for trigger in [Trigger::Value, Trigger::Error, Trigger::Cancel, Trigger::Close] {
        let queue = LiveChanges::new();
        let seen = queue.clone();
        let waker = ProbeWaker::new(move || mutex_free(&seen.state) && mutex_free(&seen.subscription));
        let mut next = Box::pin(queue.next());
        assert!(next.as_mut().poll(&mut Context::from_waker(&Waker::from(waker.clone()))).is_pending());
        let listener = Listener(Arc::downgrade(&queue));
        match trigger {
            Trigger::Value => listener.changed(LiveChange::empty(1)),
            Trigger::Error => listener.changed(LiveChange { error: Some(DbError::Network { detail: "lost".into() }), ..LiveChange::empty(2) }),
            Trigger::Cancel => queue.cancel(),
            Trigger::Close => super::super::open_streams::Closable::close(&*queue),
        }
        assert_eq!((waker.wakes() >= 1, waker.violations()), (true, 0));
        let ready = next.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        match trigger {
            Trigger::Value => assert!(matches!(ready, Poll::Ready(Some(change)) if change.revision == 1)),
            Trigger::Error => assert!(matches!(ready, Poll::Ready(Some(change)) if matches!(change.error, Some(DbError::Network { .. })))),
            Trigger::Cancel | Trigger::Close => assert!(matches!(ready, Poll::Ready(None))),
        }
    }
}

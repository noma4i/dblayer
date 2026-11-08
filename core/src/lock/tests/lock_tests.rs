use super::debug::{lock, lock_as};
use std::sync::Mutex;

#[tokio::test]
async fn direct_effect_entries_reject_a_held_class() {
    let task = tokio::spawn(std::future::pending::<()>());
    let abort = task.abort_handle();
    let notify = tokio::sync::Notify::new();
    let (watch, _watch) = tokio::sync::watch::channel(false);
    let (channel, _channel) = tokio::sync::mpsc::unbounded_channel();
    let (broadcast, _broadcast) = tokio::sync::broadcast::channel(1);
    let (standard, _standard) = std::sync::mpsc::channel();
    for kind in ["notify_one", "notify_waiters", "watch_send", "watch_replace", "watch_modify", "watch_if_modified", "mpsc", "oneshot", "broadcast", "std_mpsc", "join_abort", "handle_abort"] {
        let mutex = Mutex::new(());
        let guard = lock_as(&mutex, "direct effect held class");
        let called = std::cell::Cell::new(false);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| super::effect(|| {
            called.set(true);
            match kind {
                "notify_one" => notify.notify_one(),
                "notify_waiters" => notify.notify_waiters(),
                "watch_send" => { let _ = watch.send(true); }
                "watch_replace" => { watch.send_replace(true); }
                "watch_modify" => watch.send_modify(|value| *value = true),
                "watch_if_modified" => { watch.send_if_modified(|value| { *value = true; true }); }
                "mpsc" => { let _ = channel.send(()); }
                "oneshot" => { let (sender, _receiver) = tokio::sync::oneshot::channel(); let _ = sender.send(()); }
                "broadcast" => { let _ = broadcast.send(()); }
                "std_mpsc" => { let _ = standard.send(()); }
                "join_abort" => task.abort(),
                "handle_abort" => abort.abort(),
                _ => unreachable!(),
            }
        })));
        drop(guard);
        let diagnostic = message(result.expect_err("the effect must reject a held guard"));
        assert!(diagnostic.contains("direct effect held class"), "{kind}: {diagnostic}");
        assert!(!called.get(), "{kind} ran before rejecting the guard");
    }
    task.abort();
    assert!(crate::test_wait::within(task).await.unwrap_err().is_cancelled());
}

fn message(error: Box<dyn std::any::Any + Send>) -> String {
    error.downcast_ref::<String>().cloned().or_else(|| error.downcast_ref::<&str>().map(|text| (*text).to_string())).unwrap_or_default()
}

#[test]
fn debug_lock_graph_rejects_cycle() {
    let first = Mutex::new(());
    let second = Mutex::new(());
    { let _a = lock_as(&first, "test graph first"); let _b = lock_as(&second, "test graph second"); }
    let _b = lock_as(&second, "test graph second");
    let result = std::panic::catch_unwind(|| { let _a = lock_as(&first, "test graph first"); });
    let diagnostic = message(result.expect_err("the inverse acquisition must fail before blocking"));
    assert!(diagnostic.contains("test graph first") && diagnostic.contains("test graph second"));
}

#[test]
fn same_instance_reacquire_is_diagnosed() {
    let mutex = Mutex::new(());
    let _guard = lock_as(&mutex, "test reacquire");
    let result = std::panic::catch_unwind(|| { let _again = lock_as(&mutex, "test reacquire"); });
    let diagnostic = message(result.expect_err("reacquiring the same mutex must fail before blocking"));
    assert!(diagnostic.contains("test reacquire"));
}

#[test]
fn effect_entry_rejects_guard() {
    struct EffectLock;
    let mutex = Mutex::new(EffectLock);
    let _guard = lock(&mutex);
    let result = std::panic::catch_unwind(|| super::assert_no_library_lock("Effects::run"));
    let diagnostic = message(result.expect_err("effects must reject an active library guard"));
    assert!(diagnostic.contains("EffectLock") && diagnostic.contains("Effects::run"));
}

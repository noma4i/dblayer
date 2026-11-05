#[test]
fn inv_wait_03_the_send_sources_hold_no_timer() {
    let dir = crate::test_wait::package_dir().join("src/sync/send");
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with("_tests.rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        for word in ["sleep(", "interval(", "sleep_until("] {
            assert!(!source.contains(word), "{name} holds a timer: {word}");
        }
    }
}

#[test]
fn inv_wait_04_no_test_of_the_wrappers_waits_without_a_bound() {
    let scripts = crate::test_wait::package_dir().join("../scripts");
    for script in ["check_waits_test.sh", "check_waits.sh"] {
        let output = std::process::Command::new("bash").arg(scripts.join(script)).output().unwrap();
        assert!(output.status.success(), "{script}: {}", String::from_utf8_lossy(&output.stderr));
    }
}

struct Polled<F> { work: std::pin::Pin<Box<F>>, polls: std::sync::Arc<std::sync::atomic::AtomicUsize> }

impl<F: std::future::Future> std::future::Future for Polled<F> {
    type Output = F::Output;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<F::Output> {
        self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.work.as_mut().poll(cx)
    }
}

#[test]
fn inv_wait_01_inv_wait_02_an_outcome_wait_wakes_only_when_its_operation_ends() { crate::test_wait::paused(async {
    use crate::sync::rig::*;
    use crate::db::fixtures::rid;
    use serde_json::json;
    let rig = rig();
    let (_, waited) = create_claimed(&rig.sync, "moments", params(json!({"text": "held"})));
    rig.transport.enqueue("CreateMoment", crate::transport::Reply::data(json!({"createMoment": {"id": "m77", "text": "held", "userId": null}})).after(std::time::Duration::from_secs(20)));
    let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ticket = waited.clone();
    let waiting = tokio::spawn(Polled { work: Box::pin(async move { ticket.result().await.result }), polls: polls.clone() });
    let sync = rig.sync.clone();
    let flushing = tokio::spawn(async move { let admitted = sync.inner.db.admit().unwrap(); crate::sync::rig::app(&sync, sync.flush(&admitted)).await.map(drop) });
    put(&rig, "moments", moment("m2", "t"));
    rig.transport.enqueue("UpdateMoment", crate::transport::Reply::data(json!({"updateMoment": {"id": "m2", "text": "three", "userId": null}})));
    for text in ["one", "two", "three"] {
        rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m2"), params(json!({"text": text}))).unwrap();
    }
    crate::test_wait::pass(std::time::Duration::from_secs(10)).await;
    assert_eq!((polls.load(std::sync::atomic::Ordering::SeqCst), rig.transport.call_count("CreateMoment")), (1, 1));
    crate::test_wait::within(flushing).await.unwrap().unwrap();
    assert_eq!(crate::test_wait::within(waiting).await.unwrap(), crate::sync::SendResult::Committed);
    assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 2);
}) }

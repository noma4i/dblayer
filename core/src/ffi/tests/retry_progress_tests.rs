use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;

use super::{DbError, EncryptedReply, FakeHttp};
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::RetrySnapshot;

struct Refusing;

#[async_trait]
impl FakeHttp for Refusing {
    async fn post(&self, _: String, _: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        Ok(EncryptedReply { status: 503, body: String::new() })
    }
}

#[derive(Clone, Copy, Debug)]
enum Input {
    Change,
    Cancel,
    PollHeld,
}

type Held = Pin<Box<dyn Future<Output = Option<RetrySnapshot>> + Send>>;

fn poll_once(call: &mut Held) -> Poll<Option<RetrySnapshot>> { call.as_mut().poll(&mut Context::from_waker(Waker::noop())) }

/// Requirement: a `next` call of a retry-progress reader that passed the cancel check and waits never returns a snapshot after `cancel` returned, and a call that starts later returns nothing.
/// Observable: the result of each `next` call. Assert: for every order of change, cancel and poll of the waiting call, a poll after the cancel gives `None`, a poll before it gives the pending call or the newest snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn inv_observe_08_a_retry_progress_call_that_passed_the_cancel_check_does_not_start_after_the_cancel_returns() {
    use Input::{Cancel, Change, PollHeld};
    let orders = [[Change, Cancel, PollHeld], [Change, PollHeld, Cancel], [Cancel, Change, PollHeld], [Cancel, PollHeld, Change], [PollHeld, Change, Cancel], [PollHeld, Cancel, Change]];
    for order in orders {
        for round in 0..16 {
            let config = crate::transport::EncryptedConfig { origin: origin().into(), pin: pin().into(), server_key: public_key_der_b64().into(), ..crate::transport::encrypted::tests::protocol_tests::config() };
            let retries = super::RetryProgress::new();
            let clock = super::TestClock::new(1_800_000_000_000);
            let call = tokio::spawn({
                let (retries, clock) = (retries.clone(), clock.manual());
                async move { super::encrypted::fetch_server_config_with(config, crate::transport::AppPlatform::Ios, "1.2".into(), None, Arc::new(Refusing), Some(retries), super::encrypted::Waits { connectivity: crate::transport::Connectivity::new(true), clock }).await }
            });
            crate::test_wait::within_named(8, "the first retry timer is armed", clock.await_armed(0)).await;
            let reader = retries.subscribe();
            assert_eq!(crate::test_wait::within_named(8, "the first read of the snapshot", reader.next()).await.map(|snapshot| (snapshot.waiting.len(), snapshot.waiting[0].owner)), Some((1, crate::transport::RetryOwner::Bootstrap)), "the first call returns the current snapshot");
            let mut held: Held = Box::pin({
                let reader = reader.clone();
                async move { crate::test_wait::within_named(8, "the held read", reader.next()).await }
            });
            assert!(poll_once(&mut held).is_pending(), "{order:?}: the waiting call passed the cancel check and waits for a change");

            let (mut cancelled, mut changed, mut done) = (false, None, false);
            for input in order {
                match input {
                    Change => {
                        let armed = clock.armed();
                        clock.advance(1_000);
                        crate::test_wait::within_named(8, "the retry timer is armed again after the clock moved", clock.await_armed(armed)).await;
                        changed = Some(retries.current());
                    }
                    Cancel => {
                        reader.cancel();
                        cancelled = true;
                    }
                    PollHeld if done => {}
                    PollHeld => match (poll_once(&mut held), cancelled, &changed) {
                        (Poll::Ready(None), true, _) => done = true,
                        (Poll::Ready(value), true, _) => panic!("{order:?} round {round}: the call gave {value:?} after the cancel returned"),
                        (Poll::Ready(value), false, Some(newest)) => {
                            assert_eq!(value.as_ref(), Some(newest), "{order:?} round {round}");
                            done = true;
                        }
                        (Poll::Pending, false, None) => {}
                        (result, ..) => panic!("{order:?} round {round}: the poll gave {result:?} with cancelled {cancelled} and changed {changed:?}"),
                    },
                }
            }
            if !done {
                let late = crate::test_wait::within_named(8, "the held read ends after the cancel", held).await;
                assert_eq!(late, None, "{order:?} round {round}: the call that waited through the cancel returned {late:?}");
            }
            assert_eq!(crate::test_wait::within_named(8, "a read after the cancel", reader.next()).await, None, "{order:?} round {round}: a call that starts after the cancel returned a value");
            call.abort();
            let gone = retries.subscribe();
            crate::test_wait::within_named(8, "the aborted call leaves the registry", async { while !retries.current().waiting.is_empty() { gone.next().await; } }).await;
        }
    }
}

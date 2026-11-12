//! Owner-level tests of the owner `Transfer`: the abort pair Session restores (`held_orphan`, D-280). The pair flies with Session; Transfer sends no second abort and creates no record for a pair without one.

use crate::test_wait::permutations;
use super::transfer_harness::*;
use crate::owners::contract::ErrorClass;
use crate::owners::session::{AuthorOutcome, SessionError};
use crate::owners::write::transfer::TransferInput;
use crate::owners::write::wire::abort_request_id;
use crate::transport::AnswerKind;

const KEY: &str = "Moment:r1:upload";
const UPLOAD: &str = "u9";

fn restored(twin: &mut Twin, orphans: Vec<(String, String)>, held_orphan: Option<&str>) -> Out {
    twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans, batches: Default::default(), held_orphan: held_orphan.map(str::to_string) })
}

fn register(twin: &mut Twin) -> Out { twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())], complete: false }) }

fn ready(twin: &mut Twin) -> Out { twin.post(TransferInput::RestoreReady { epoch: EPOCH }) }

fn answered(twin: &mut Twin, outcome: AuthorOutcome) -> Out { twin.post(TransferInput::StepAnswer { request_id: abort_request_id(UPLOAD), outcome }) }

fn confirmed() -> AuthorOutcome { AuthorOutcome::Response { body: "{\"data\":{\"abortMultipart\":{\"ok\":true}}}".into(), kind: AnswerKind::Answered { sequence: None }, pending_retained: false } }

fn debt() -> Vec<(String, String)> { vec![(UPLOAD.into(), KEY.into())] }

/// transfer.md xfer_restored_held_orphan_flight: the restored pair is in flight from `Restored`; no order of Register, RestoreReady and Online(true) sends a second abort, the answer of the pair removes the debt.
#[test]
fn xfer_restored_held_orphan_flight() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Event { Register, Ready, Online }
    for order in permutations(&[Event::Register, Event::Ready, Event::Online]) {
        let mut twin = Twin::bare(1);
        restored(&mut twin, debt(), Some(UPLOAD));
        for event in order.iter().copied() {
            let out = match event { Event::Register => register(&mut twin), Event::Ready => ready(&mut twin), Event::Online => twin.online(true) };
            assert!(out.orphan_aborts().is_empty(), "{order:?} at {event:?}: the restored pair flies with Session, Transfer sends no abort: {:?}", out.orphan_aborts());
        }
        let out = answered(&mut twin, confirmed());
        assert_eq!(out.orphans_removed(), vec![UPLOAD.to_string()], "{order:?}: the answer of the restored pair removes the debt: {out:?}");
        twin.commit();
        twin.online(false);
        assert!(twin.online(true).orphan_aborts().is_empty(), "{order:?}: the debt is gone");
    }
}

/// transfer.md xfer_restored_held_orphan_without_record: a restored pair without a debt record creates nothing and sends nothing; its answer is a foreign input (R4).
#[test]
fn xfer_restored_held_orphan_without_record() {
    let mut twin = Twin::bare(1);
    let quiet = [restored(&mut twin, Vec::new(), Some(UPLOAD)), register(&mut twin), twin.online(true), ready(&mut twin)];
    assert!(quiet.iter().all(|out| out.orphan_aborts().is_empty() && out.writes() == 0 && out.releases().is_empty()), "a pair without a record sends and writes nothing on the way: {quiet:?}");
    let out = answered(&mut twin, confirmed());
    assert!(out.orphan_aborts().is_empty() && out.orphans_removed().is_empty() && out.releases().is_empty() && out.writes() == 0, "nothing is created or sent for a pair without a record: {out:?}");
    twin.online(false);
    assert!(twin.online(true).orphan_aborts().is_empty(), "still no debt");
}

/// transfer.md xfer_orphan_abort_owed, xfer_orphan_abort_owed_resent: Online(true) and RestoreReady during the restored flight owe the abort; a temporary answer of the pair makes Transfer send it once.
#[test]
fn xfer_held_orphan_answer_temporary() {
    for outcome in [AuthorOutcome::Error { class: ErrorClass::Network, error: SessionError::Invalid, pending_retained: false }, AuthorOutcome::Dropped] {
        let mut twin = Twin::bare(1);
        let quiet = [restored(&mut twin, debt(), Some(UPLOAD)), register(&mut twin), twin.online(true), ready(&mut twin)];
        assert!(quiet.iter().all(|out| out.orphan_aborts().is_empty()), "{outcome:?}: the restored pair flies with Session until its answer: {quiet:?}");
        let out = answered(&mut twin, outcome.clone());
        assert_eq!(out.orphan_aborts(), vec![UPLOAD.to_string()], "{outcome:?}: the owed abort is sent once by Transfer after the temporary answer: {out:?}");
        assert!(out.orphans_removed().is_empty(), "{outcome:?}: the debt stays");
        assert!(twin.online(false).orphan_aborts().is_empty() && twin.online(true).orphan_aborts().is_empty(), "{outcome:?}: the abort in flight is not sent twice");
    }
}

/// transfer.md xfer_orphan_abort_done (404, 410 remove the debt) through the restored pair: the answer of Session with a gone upload removes the debt like a confirmed abort.
#[test]
fn xfer_held_orphan_answer_gone() {
    for status in [404u16, 410] {
        let mut twin = Twin::bare(1);
        restored(&mut twin, debt(), Some(UPLOAD));
        register(&mut twin);
        twin.online(true);
        let out = answered(&mut twin, AuthorOutcome::Error { class: ErrorClass::Permanent, error: SessionError::Http { status }, pending_retained: false });
        assert_eq!(out.orphans_removed(), vec![UPLOAD.to_string()], "{status}: a gone upload ends the debt: {out:?}");
    }
}

/// transfer.md «идентичность шага», R4: a second answer of the same pair before Committed and a late one after it change nothing.
#[test]
fn xfer_held_orphan_answer_repeated() {
    let mut twin = Twin::bare(1);
    restored(&mut twin, debt(), Some(UPLOAD));
    register(&mut twin);
    twin.online(true);
    assert_eq!(answered(&mut twin, confirmed()).orphans_removed(), vec![UPLOAD.to_string()]);
    let again = answered(&mut twin, confirmed());
    assert!(again.orphans_removed().is_empty() && again.orphan_aborts().is_empty() && again.writes() == 0, "the second answer before Committed writes nothing: {again:?}");
    twin.commit();
    let late = answered(&mut twin, confirmed());
    assert!(Twin::quiet(&late), "a late answer after Committed is a foreign input: {late:?}");
}

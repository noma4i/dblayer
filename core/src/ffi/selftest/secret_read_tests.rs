use super::super::tests::sealed_errors_tests::signed_in;
use super::super::tests::session_guards_tests::Rig;
use super::super::{Database, DbError, SessionCode};
use crate::store::session_state;
use crate::transport::CredentialSlot;

fn stored(database: &Database) -> Option<(String, u64)> {
    let inner = database.db.lock();
    session_state::session(&inner.connection).unwrap().map(|session| (session.credential_digest, u64::try_from(session.sequence).unwrap()))
}

/// sess_read_secret_missing (P-SESS-22, D-197): the secret vanishes after a login. A query and a mutation end with `StateLost`; no secret is created, the digest and the sequence stay and no request leaves.
#[tokio::test(flavor = "multi_thread")]
async fn a_vanished_secret_ends_every_call_with_state_lost_and_creates_nothing() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    let epoch = database.session_epoch();
    let (kept, sets, sent) = (stored(&database), rig.touched().0, rig.server.executed().len());
    CredentialSlot::clear(&rig.credentials.inner).unwrap();

    let read = database.run(epoch, "Feed".into(), "{}".into()).await;
    let written = database.run(epoch, "MakeMoment".into(), "{}".into()).await;

    for answer in [read, written] {
        assert!(matches!(answer, Err(DbError::Session { code: SessionCode::StateLost, .. })), "{answer:?}");
    }
    assert_eq!((stored(&database), rig.touched().0, rig.server.executed().len(), rig.credentials.inner.value()), (kept, sets, sent, None), "the stored session stays, no secret was created, no request left");
}

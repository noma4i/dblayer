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
async fn sess_read_secret_missing() {
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

/// sess_read_secret_missing (P-SESS-22, D-197) on the cable connect: the secret vanishes after a login, then a stream asks for the cable. No subscription reaches the socket, no secret is created, the digest and the sequence stay.
#[tokio::test(flavor = "multi_thread")]
async fn a_vanished_secret_stops_the_cable_connect_and_creates_nothing() {
    use super::encrypted_cable_tests::Peer;
    use super::rig_tests::bound;
    use crate::ffi::tests::encrypted_tests::ThroughServer;
    use crate::ffi::tests::session_guards_tests::SOCIAL_BODY;
    use crate::ffi::{Config, OpenPorts, ReadRequest};
    use std::sync::Arc;

    let rig = Rig::new();
    let config = crate::transport::encrypted::tests::protocol_tests::config();
    let (subscribed, mut subscriptions) = tokio::sync::mpsc::unbounded_channel();
    let (closed, _closes) = tokio::sync::mpsc::unbounded_channel();
    let peer = Arc::new(Peer::new(config.clone(), subscribed, closed));
    let database = Database::open(Config { path: None, data_version: None }, crate::ffi::specs(super::rig_models::plain_models()), OpenPorts { partial_operations: true, transport: Some(peer.clone()), encrypted: Some(config), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"}]"#.into()).await.unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let (kept, sets) = (stored(&database), rig.touched().0);
    CredentialSlot::clear(&rig.credentials.inner).unwrap();

    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Notice".into() });
    let life = database.life_handle();
    crate::test_wait::within(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();

    assert_eq!(peer.cable.connects(), 0, "no socket was opened");
    assert!(subscriptions.try_recv().is_err(), "no subscription reached the socket");
    assert_eq!((stored(&database), rig.touched().0, rig.credentials.inner.value()), (kept, sets, None), "the stored session stays and no secret was created");
    drop(stream);
    bound(database.close_database()).await.unwrap();
}

use super::encrypted_tests::rig;
use crate::transport::encrypted::vectors_tests::{pin, public_key_der_b64};

#[tokio::test]
async fn one_offline_signal_reaches_the_database_and_encrypted_client() {
    let (database, _server, _credentials) = rig(public_key_der_b64(), pin()).await;
    let sync = database.db.connectivity();
    let session = database.session.as_ref().unwrap().online.clone();
    let flags = [("sync", &sync), ("encrypted client", &session)];
    for (name, flag) in flags { assert!(flag.is_online(), "{name} starts online"); }
    sync.set_online(false);
    for (name, flag) in flags { assert!(!flag.is_online(), "{name} must see offline after the flag is set once"); }
    sync.set_online(true);
    for (name, flag) in flags { assert!(flag.is_online(), "{name} must see online after the flag is set once"); }
}

use super::encrypted_tests::rig;
use crate::cloud::fixtures::FakeCloud;
use crate::transport::encrypted::vectors_tests::{pin, public_key_der_b64};

#[tokio::test]
async fn one_offline_signal_reaches_sync_encrypted_client_and_cloud() {
    let (database, _server, _credentials) = rig(public_key_der_b64(), pin()).await;
    let link = database.connect_cloud(FakeCloud::new()).unwrap();
    let sync = database.sync.as_ref().unwrap().inner.connectivity.clone();
    let session = database.session.as_ref().unwrap().online.clone();
    let cloud = link.cloud.connectivity();
    let flags = [("sync", &sync), ("encrypted client", &session), ("cloud", &cloud)];
    for (name, flag) in flags { assert!(flag.is_online(), "{name} starts online"); }
    sync.set_online(false);
    for (name, flag) in flags { assert!(!flag.is_online(), "{name} must see offline after the flag is set once"); }
    sync.set_online(true);
    for (name, flag) in flags { assert!(flag.is_online(), "{name} must see online after the flag is set once"); }
}

use rusqlite::Connection;

use crate::store::cloud::{self, PendingRecord};

#[test]
fn delayed_cloud_acknowledgement_keeps_the_newer_change_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cloud.sqlite");
    let mut connection = Connection::open(&path).unwrap();
    let original = PendingRecord { model: "notes".into(), row_id: "n1".into(), version: 1, deleted: false };
    let deletion = PendingRecord { version: 2, deleted: true, ..original.clone() };

    let transaction = connection.transaction().unwrap();
    cloud::enable(&transaction).unwrap();
    cloud::stage(&transaction, "account-a", &original).unwrap();
    cloud::stage(&transaction, "account-b", &original).unwrap();
    cloud::write_progress(&transaction, "account-b", b"other-cursor").unwrap();
    transaction.commit().unwrap();
    assert_eq!(cloud::pending(&connection, "account-a", 1).unwrap(), vec![original.clone()]);

    let transaction = connection.transaction().unwrap();
    cloud::stage(&transaction, "account-a", &deletion).unwrap();
    cloud::acknowledge(&transaction, "account-a", &original).unwrap();
    cloud::write_progress(&transaction, "account-a", b"committed-cursor").unwrap();
    transaction.commit().unwrap();

    let transaction = connection.transaction().unwrap();
    cloud::write_progress(&transaction, "account-a", b"uncommitted-cursor").unwrap();
    cloud::acknowledge(&transaction, "account-a", &deletion).unwrap();
    transaction.rollback().unwrap();
    drop(connection);

    let mut reopened = Connection::open(path).unwrap();
    assert_eq!(cloud::pending(&reopened, "account-a", 1).unwrap(), vec![deletion.clone()]);
    assert_eq!(cloud::progress(&reopened, "account-a").unwrap().as_deref(), Some(b"committed-cursor".as_slice()));

    let transaction = reopened.transaction().unwrap();
    cloud::acknowledge(&transaction, "account-a", &deletion).unwrap();
    cloud::stage(&transaction, "account-a", &deletion).unwrap();
    transaction.commit().unwrap();
    assert!(cloud::pending(&reopened, "account-a", 1).unwrap().is_empty());

    let transaction = reopened.transaction().unwrap();
    cloud::reset(&transaction, "account-a").unwrap();
    transaction.commit().unwrap();
    assert_eq!(cloud::progress(&reopened, "account-a").unwrap(), None);
    assert_eq!(cloud::pending(&reopened, "account-b", 1).unwrap(), vec![original]);
    assert_eq!(cloud::progress(&reopened, "account-b").unwrap().as_deref(), Some(b"other-cursor".as_slice()));
}

use super::*;

#[test]
fn stored_payload_reads_back() {
    let store = Store::open_in_memory().unwrap();
    store.put("moments".into(), "m1".into(), "{\"a\":1}".into()).unwrap();
    assert_eq!(store.get("moments".into(), "m1".into()).unwrap(), Some("{\"a\":1}".to_string()));
    assert_eq!(store.get("moments".into(), "m2".into()).unwrap(), None);
}

#[test]
fn version_is_package_version() {
    assert_eq!(version(), env!("CARGO_PKG_VERSION"));
}

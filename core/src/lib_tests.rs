use super::*;

#[test]
fn version_is_package_version() {
    assert_eq!(version(), env!("CARGO_PKG_VERSION"));
}

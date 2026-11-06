use super::*;
use crate::schema::FieldSpec;
use serde_json::json;

fn descriptor() -> FileDescriptor {
    FileDescriptor { id: "f1".into(), size: 3, name: Some("a.png".into()), mime: Some("image/png".into()), sha256: Some("abc".into()) }
}

#[test]
fn value_round_trips_and_passes_the_file_field_check() {
    let value = descriptor().to_value();
    assert_eq!(FileDescriptor::from_value(&value), Some(descriptor()));
    assert!(FieldSpec::file("media").decode(&value).is_some());
}

#[test]
fn value_without_id_or_size_is_not_a_descriptor() {
    assert_eq!(FileDescriptor::from_value(&json!({"size": 3})), None);
    assert_eq!(FileDescriptor::from_value(&json!({"id": "f1"})), None);
    assert_eq!(FileDescriptor::from_value(&json!("f1")), None);
}

#[test]
fn server_descriptor_may_omit_name_mime_and_digest() {
    let parsed = FileDescriptor::from_value(&json!({"id": "s1", "size": 9})).unwrap();
    assert_eq!((parsed.name, parsed.mime, parsed.sha256), (None, None, None));
}

#[test]
fn digest_is_lowercase_hex_sha256() {
    assert_eq!(sha256_hex(b"abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

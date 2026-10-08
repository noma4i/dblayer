use super::*;
use crate::identity::RowId;
use crate::pacing::Counters;
use std::collections::BTreeMap;

fn path(name: &str) -> Path {
    Path::new(name)
}

#[test]
fn server_fields_are_admitted_only_when_row_is_not_newer() {
    let row = RowId::new("row-1").unwrap();
    let fields = [path("server"), path("local"), path("continuity"), path("default")];
    let policies: BTreeMap<Path, WritePolicy> =
        [(path("server"), WritePolicy::Server), (path("local"), WritePolicy::Local), (path("continuity"), WritePolicy::Continuity)].into();
    let target = AdmissionTarget { model: "message".into(), row: row.clone() };
    let newer = admission_verdict(&target, &fields, &policies, 3, 2);
    let older = admission_verdict(&target, &fields, &policies, 1, 2);
    assert!(admits_server_fields(1, 2));
    assert!(admits_server_fields(2, 2));
    assert!(!admits_server_fields(3, 2));
    assert_eq!(newer.accepted_fields, [path("local"), path("continuity")]);
    assert_eq!(newer.dropped_fields, [path("server"), path("default")]);
    assert_eq!(newer.drop, Some(AdmissionDrop { model: "message".into(), row, fields: vec![path("server"), path("default")] }));
    assert_eq!(older.accepted_fields, fields);
    assert!(older.dropped_fields.is_empty());
    assert_eq!(older.drop, None);
}

#[test]
fn buffer_keeps_last_hundred() {
    let row = RowId::new("row-1").unwrap();
    let mut buffer = Vec::new();
    for index in 0..105 {
        append_drop(&mut buffer, AdmissionDrop { model: format!("model-{index}"), row: row.clone(), fields: vec![path(&format!("field-{index}"))] });
    }
    assert_eq!(buffer.len(), DROP_EVENT_CAPACITY);
    assert_eq!(buffer[0].model, "model-5");
}

#[test]
fn drop_counted_once_per_row() {
    let row = RowId::new("row-1").unwrap();
    let fields = vec![path("title"), path("body")];
    let dropped = AdmissionVerdict {
        accepted_fields: vec![],
        dropped_fields: fields.clone(),
        drop: Some(AdmissionDrop { model: "message".into(), row, fields }),
    };
    let accepted = AdmissionVerdict { accepted_fields: vec![path("title")], dropped_fields: vec![], drop: None };
    let mut counters = Counters::default();
    count_drops(&mut counters, &dropped);
    assert_eq!(counters.causal_admission_drops, 1);
    count_drops(&mut counters, &accepted);
    assert_eq!(counters.causal_admission_drops, 1);
}

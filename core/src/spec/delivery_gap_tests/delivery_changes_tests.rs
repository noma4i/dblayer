use super::*;

fn pair(table: &str, id: &str) -> (String, String) { (table.to_string(), id.to_string()) }

fn changed(revision: i64, rows: &[(&str, &str)], reset: bool) -> Input { Input::Changed { revision, rows: rows.iter().map(|(table, id)| pair(table, id)).collect(), reset } }

#[test]
fn dlv_changes_opened() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::Open { kind: Kind::Changes, epoch_current: true }), [Effect::Available]);
    assert_eq!(twin.read_all(), [Val::Ready]);
}

#[test]
fn dlv_changes_each_commit() {
    let mut twin = open_as(Kind::Changes);
    twin.read_all();
    twin.step(changed(1, &[("row_Chat", "c1"), ("row_Message", "m1")], false));
    twin.step(changed(2, &[("row_Chat", "c1")], false));
    twin.step(changed(3, &[], true));
    assert_eq!(
        twin.read_all(),
        [
            Val::Changes { revision: 1, rows: vec![pair("row_Chat", "c1"), pair("row_Message", "m1")] },
            Val::Changes { revision: 2, rows: vec![pair("row_Chat", "c1")] },
            Val::Changes { revision: 3, rows: Vec::new() },
        ]
    );
}

#[test]
fn dlv_changes_quiet() {
    let mut twin = open_as(Kind::Changes);
    twin.read_all();
    assert!(twin.step(changed(1, &[], false)).is_empty());
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_changes_ended() {
    let mut twin = open_as(Kind::Changes);
    twin.read_all();
    twin.step(Input::EpochEnded { own: true });
    assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
    let mut closing = open_as(Kind::Changes);
    closing.read_all();
    closing.step(Input::Closing);
    assert_eq!(closing.read_all(), [Val::Ended(Cause::Closed)]);
}

#[test]
fn dlv_changes_not_in_census() {
    let mut twin = open_rows();
    let rows_only = census_of(&mut twin);
    twin.call(d::DeliveryInput::Open { stream: STREAM + 1, view: view(Kind::Changes), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    assert_eq!(census_of(&mut twin), rows_only, "an open Changes stream leaves the census as it was");
    assert_eq!(twin.state().streams(), 2);
    let mut alone = open_as(Kind::Changes);
    alone.step(Input::EpochEnded { own: true });
    assert_eq!(census_of(&mut alone), (0, 0), "an ended Changes stream is not retained");
}

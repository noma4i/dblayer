use super::*;

fn debt_list() -> Twin {
    let mut twin = Twin::new();
    let mut shown = view(Kind::List);
    shown.node = Some("node".into());
    twin.post(d::DeliveryInput::Open { stream: STREAM, view: shown, epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    twin.step(Input::WindowState(Ws::loaded_at(1)));
    twin
}

fn read(twin: &mut Twin, revision: i64, owing: &[&str]) -> Vec<String> {
    take(&twin.log);
    let rows = ["r1", "r2"].iter().map(|id| d::StreamRow::new(id, 1)).collect();
    let debts = owing.iter().map(|id| ("Moment".to_string(), id.to_string(), "node".to_string())).collect();
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: twin.read_id(), revision, position: 0, relevant: Some((revision, 0)), touching: Vec::new(), data: d::ReadData::Owed(Box::new(d::ReadData::Rows { rows, replaced: Vec::new(), total: None }), debts) });
    take(&twin.log).into_iter().filter_map(|entry| match entry { Emitted::Send { to: WINDOWS, input } => Some(input), _ => None }).collect()
}

/// dlv_owed_row_interest: a shown row in debt holds the interest of its `node` window, a row that pays nothing holds none; a stream with `fetch: never` holds it as well when the window is not its own.
#[test]
fn dlv_owed_row_interest() {
    let mut twin = debt_list();
    let sends = read(&mut twin, 1, &["r1"]);
    assert_eq!(sends.iter().filter(|send| send.starts_with("InterestPlus") && send.contains("node|") && send.contains("r1")).count(), 1, "{sends:?}");
    assert!(!sends.iter().any(|send| send.contains("r2")), "{sends:?}");
    let mut never = Twin::new();
    let mut shown = view(Kind::Entity(Fetch::Never));
    shown.node = Some("node".into());
    never.post(d::DeliveryInput::Open { stream: STREAM, view: shown, epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    never.step(Input::WindowState(Ws::loaded_at(1)));
    let sends = read(&mut never, 1, &["r1"]);
    assert_eq!(sends.iter().filter(|send| send.starts_with("InterestPlus") && send.contains("node|") && send.contains("r1")).count(), 1, "fetch never: {sends:?}");
}

/// dlv_owed_row_paid_releases: the interest ends when the row pays or the stream ends, and a second debt of the row holds it again.
#[test]
fn dlv_owed_row_paid_releases() {
    let mut twin = debt_list();
    read(&mut twin, 1, &["r1"]);
    let paid = read(&mut twin, 2, &[]);
    assert_eq!(paid.iter().filter(|send| send.starts_with("InterestMinus") && send.contains("r1")).count(), 1, "{paid:?}");
    let again = read(&mut twin, 3, &["r1"]);
    assert_eq!(again.iter().filter(|send| send.starts_with("InterestPlus") && send.contains("r1")).count(), 1, "{again:?}");
    take(&twin.log);
    twin.call(d::DeliveryInput::Release { stream: STREAM, reader: None });
    let ended: Vec<String> = take(&twin.log).into_iter().filter_map(|entry| match entry { Emitted::Send { to: WINDOWS, input } => Some(input), _ => None }).collect();
    assert_eq!(ended.iter().filter(|send| send.starts_with("InterestMinus") && send.contains("r1")).count(), 1, "{ended:?}");
}

/// dlv_owes_only_no_frame: a read that differs from the value only by the debt of a row gives the reader no value.
#[test]
fn dlv_owes_only_no_frame() {
    let mut twin = debt_list();
    read(&mut twin, 1, &[]);
    twin.read_all();
    read(&mut twin, 2, &["r1"]);
    assert_eq!(twin.state().journal_len(STREAM), 0, "the debt is no part of the value");
    read(&mut twin, 3, &[]);
    assert_eq!(twin.state().journal_len(STREAM), 0, "the payment is no part of the value");
}

/// dlv_owed_row_interest: a count reads the debt of the members of its window, a read of a local table the debt of every row of the model.
#[test]
fn dlv_owed_row_interest_count_and_local_table() {
    let ids = |names: &[&str]| names.iter().map(|id| ("Moment".to_string(), id.to_string(), "node".to_string())).collect::<std::collections::BTreeSet<_>>();
    let mut count = Twin::new();
    let mut counted = view(Kind::Count);
    counted.node = Some("node".into());
    count.post(d::DeliveryInput::Open { stream: STREAM, view: counted, epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    count.step(Input::WindowState(Ws::loaded_at(1)));
    take(&count.log);
    count.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: count.read_id(), revision: 1, position: 0, relevant: Some((1, 0)), touching: Vec::new(), data: d::ReadData::Owed(Box::new(d::ReadData::Count(2)), ids(&["r1"])) });
    let sends: Vec<String> = take(&count.log).into_iter().filter_map(|entry| match entry { Emitted::Send { to: WINDOWS, input } => Some(input), _ => None }).collect();
    assert_eq!(sends.iter().filter(|send| send.starts_with("InterestPlus") && send.contains("r1")).count(), 1, "{sends:?}");
    let mut local = Twin::new();
    let mut table = view(Kind::List);
    table.node = Some("node".into());
    table.remote = false;
    local.post(d::DeliveryInput::Open { stream: STREAM, view: table, epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    take(&local.log);
    local.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: local.read_id(), revision: 1, position: 0, relevant: Some((1, 0)), touching: Vec::new(), data: d::ReadData::Owed(Box::new(d::ReadData::Rows { rows: vec![d::StreamRow::new("r1", 1)], replaced: Vec::new(), total: None }), ids(&["r9"])) });
    let sends: Vec<String> = take(&local.log).into_iter().filter_map(|entry| match entry { Emitted::Send { to: WINDOWS, input } => Some(input), _ => None }).collect();
    assert_eq!(sends.iter().filter(|send| send.starts_with("InterestPlus") && send.contains("r9")).count(), 1, "{sends:?}");
}

/// dlv_owed_row_of_the_own_window_keeps_the_interest: the stream of the `node` window of a row in debt holds no second interest for the debt, so the payment leaves its own interest.
#[test]
fn dlv_owed_row_of_the_own_window_keeps_the_interest() {
    let mut twin = Twin::new();
    let mut shown = view(Kind::Entity(Fetch::Never));
    shown.node = Some("node".into());
    shown.window = WindowKey::relation("Moment", "node", &[(crate::schema::NODE_PARAM.to_string(), serde_json::json!("r1"))].into_iter().collect());
    twin.post(d::DeliveryInput::Open { stream: STREAM, view: shown, epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    twin.step(Input::WindowState(Ws::loaded_at(1)));
    let owing = read(&mut twin, 1, &["r1"]);
    assert!(!owing.iter().any(|send| send.starts_with("InterestPlus")), "the debt adds no interest to the own window: {owing:?}");
    let paid = read(&mut twin, 2, &[]);
    assert!(!paid.iter().any(|send| send.starts_with("InterestMinus")), "the payment keeps the own interest: {paid:?}");
}

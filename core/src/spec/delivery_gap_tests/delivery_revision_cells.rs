use super::*;

#[test]
fn dlv_resync_read_done() {
    let mut twin = live_rows();
    twin.step(Input::Resync);
    let (_, values) = twin.step_read(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    assert_eq!(values, [snap(1, &["r1"], Ws::loaded_at(1))]);
}

use super::load_machine::*;

fn state(phase: LoadPhase, error: Option<LoadError>) -> RelationState {
    RelationState { phase, initial: false, refreshing: false, fetching_more: false, has_more: false, total: None, error, loaded_at: None, revision: 0 }
}

fn offline() -> Option<LoadError> {
    Some(LoadError::new("network", "offline"))
}

#[test]
fn inv_loadm_13_the_phase_of_a_snapshot_follows_the_rows_of_the_same_snapshot() {
    use LoadPhase::*;
    let table = [
        (Loaded, None, 0, Empty),
        (Empty, None, 3, Loaded),
        (Loaded, None, 3, Loaded),
        (Empty, None, 0, Empty),
        (Loaded, offline(), 0, Failed),
        (Loaded, offline(), 2, Loaded),
        (Failed, offline(), 2, Loaded),
        (Failed, offline(), 0, Failed),
        (NotLoaded, None, 0, NotLoaded),
        (NotLoaded, None, 4, NotLoaded),
        (Loading, None, 0, Loading),
        (Loading, offline(), 5, Loading),
        (Closed, None, 0, Closed),
        (Closed, offline(), 1, Closed),
    ];
    for (phase, error, size, expected) in table {
        let shown = snapshot_phase(&state(phase, error.clone()), size);
        assert_eq!(shown, expected, "state {phase:?} error {error:?} with {size} rows");
    }
}

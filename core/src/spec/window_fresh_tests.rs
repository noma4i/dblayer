// Rows of `window.md` for `Opening` and freshness by the `now` of the input: the owner `Windows` fed by hand, the read pool answers when the test says so.

const LIFE: i64 = 300_000;
const EMPTY_LIFE: i64 = 60_000;

struct Bench {
    det: DetScheduler,
    addr: Address<Spy<Windows>>,
    log: Spied,
}

fn saved(landed_at: i64, stale: bool) -> w::SavedWindow {
    w::SavedWindow { landed_at, fresh_until: landed_at + LIFE, stale, cursor: None, pages: 1, has_rows: true, generation: 0 }
}

impl Bench {
    fn new() -> Self {
        let mut det = DetScheduler::new(3);
        let log = new_log();
        let state = w::WindowsState::new(EPOCH, Some(true), peers()).with_lifetimes(Arc::new(|_| (LIFE, EMPTY_LIFE)));
        let addr = add_windows(&mut det, &log, state, Discard, Arc::new(AtomicU64::new(0)));
        det.start_epoch(EPOCH);
        Self { det, addr, log }
    }

    fn post(&mut self, input: w::WindowsInput) -> Vec<Emitted<WindowsEffect>> {
        take(&self.log);
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.det.run_until_quiet(10_000).expect("the owner is quiet");
        take(&self.log)
    }

    fn stage(&self) -> Option<WindowStage> { inner(&self.det, self.addr).stage(&key()) }

    fn reading(&self) -> u64 { inner(&self.det, self.addr).reading(&key()).expect("the key waits for its read") }

    fn watch(&mut self, id: u64, now: i64) -> Vec<Emitted<WindowsEffect>> { self.post(w::WindowsInput::InterestPlus { key: key(), id, now }) }

    fn load(&mut self, id: u64, now: i64, force: bool) -> Vec<Emitted<WindowsEffect>> { self.post(w::WindowsInput::Load { key: key(), id, goal: w::Goal::First, force, store_revision: 0, now }) }

    fn read(&mut self, result: Result<Option<w::SavedWindow>, String>) -> Vec<Emitted<WindowsEffect>> {
        let read_id = self.reading();
        self.post(w::WindowsInput::WindowRead { key: key(), read_id, result })
    }

    /// The window of a key that never landed opens, loads the first page and lands it: the answer carries `answered`.
    fn landed(rows: bool, answered: i64) -> Self {
        let mut bench = Self::new();
        bench.watch(STREAM_ID, answered);
        bench.read(Ok(None));
        let page = AnswerPage::new(1, if rows { vec!["r1".into()] } else { Vec::new() }, None);
        bench.post(w::WindowsInput::Response { request: 1, attempt: 1, page, now: answered });
        bench.post(w::WindowsInput::Committed(Committed { batch: 1, revision: 20, tables: Vec::new(), models: Default::default(), stale_parents: Vec::new(), position: 0 }));
        assert_eq!(bench.stage(), Some(WindowStage::Idle));
        bench
    }
}

const STREAM_ID: u64 = 100;

fn asks(emitted: &[Emitted<WindowsEffect>]) -> usize { emitted.iter().filter(|entry| matches!(entry, Emitted::Send { to: STORE, input } if input.starts_with("Pin {"))).count() }

fn reads(emitted: &[Emitted<WindowsEffect>]) -> usize { emitted.iter().filter(|entry| matches!(entry, Emitted::Run(WindowsEffect::ReadWindow { .. }))).count() }

fn outcomes_of(emitted: &[Emitted<WindowsEffect>]) -> Vec<(u64, WindowOutcome)> {
    emitted.iter().filter_map(|entry| if let Emitted::Run(WindowsEffect::Window { effect: WindowEffect::Outcome { id, outcome }, .. }) = entry { Some((*id, outcome.clone())) } else { None }).collect()
}

#[test]
fn win_opening_read() {
    let mut bench = Bench::new();
    let opened = bench.load(1, NOW, false);
    assert_eq!((bench.stage(), reads(&opened), asks(&opened), outcomes_of(&opened).len()), (Some(WindowStage::Opening), 1, 0, 0), "the saved window is read in the pool, nothing is asked, the call waits");
    let read = bench.read(Ok(None));
    assert_eq!((bench.stage(), asks(&read)), (Some(WindowStage::Loading), 1), "the held load starts after the read");
}

#[test]
fn win_created_from_saved() {
    let mut bench = Bench::new();
    bench.watch(STREAM_ID, NOW);
    let opened = bench.read(Ok(Some(saved(NOW - 1_000, false))));
    assert_eq!((bench.stage(), asks(&opened)), (Some(WindowStage::Idle), 0), "a saved window inside its lifetime asks nothing");
    let fresh = bench.load(1, NOW, false);
    assert!(matches!(outcomes_of(&fresh).as_slice(), [(1, WindowOutcome::Loaded { .. })]), "a call on a fresh saved window is answered at once");
}

#[test]
fn win_created_other_generation() {
    let mut bench = Bench::new();
    bench.watch(STREAM_ID, NOW);
    let mut other = saved(NOW - 1_000, false);
    other.generation = 7;
    let opened = bench.read(Ok(Some(other)));
    assert_eq!((bench.stage(), asks(&opened)), (Some(WindowStage::Loading), 1), "a window of another generation is none: the first load asks the network");
}

#[test]
fn win_opening_read_failed() {
    let mut bench = Bench::new();
    bench.watch(STREAM_ID, NOW);
    let opened = bench.read(Err("disk".into()));
    assert_eq!((bench.stage(), asks(&opened)), (Some(WindowStage::Loading), 1), "the window loads from the network, the held input runs");
}

#[test]
fn win_opening_read_stray() {
    let mut bench = Bench::new();
    bench.watch(STREAM_ID, NOW);
    let read_id = bench.reading();
    bench.post(w::WindowsInput::WindowRead { key: key(), read_id: read_id + 5, result: Ok(None) });
    assert_eq!(bench.stage(), Some(WindowStage::Opening), "a read of another id changes nothing");
}

#[test]
fn win_opening_held() {
    let mut bench = Bench::new();
    bench.load(1, NOW, false);
    bench.watch(STREAM_ID, NOW);
    bench.post(w::WindowsInput::Online { online: Online(false), now: NOW });
    bench.post(w::WindowsInput::InterestMinus { key: key(), id: 1 });
    assert_eq!(bench.stage(), Some(WindowStage::Opening));
    assert_eq!((inner(&bench.det, bench.addr).interest(&key()), inner(&bench.det, bench.addr).waiters(&key())), (vec![STREAM_ID], Vec::<u64>::new()), "Interest+ and Interest- change the interest at once");
    let opened = bench.read(Ok(None));
    assert_eq!((bench.stage(), asks(&opened)), (Some(WindowStage::Waiting), 1), "the held inputs run in order: the first one began the load, the loss of the network moved it to Waiting");
}

#[test]
fn win_prop_opening_held_order() {
    let mut bench = Bench::new();
    bench.load(1, NOW, false);
    bench.load(2, NOW, false);
    bench.post(w::WindowsInput::InterestMinus { key: key(), id: 1 });
    let opened = bench.read(Ok(None));
    assert_eq!(asks(&opened), 1, "the inputs of the key run once, in the order they came");
    assert_eq!(inner(&bench.det, bench.addr).waiters(&key()), vec![2], "the waiter that left is gone, the other stays");
}

#[test]
fn win_opening_end() {
    let mut bench = Bench::new();
    bench.load(1, NOW, false);
    bench.det.end_epoch(EPOCH, Ended::SessionReset);
    bench.det.run_until_quiet(10_000).expect("quiet");
    let ended = outcomes_of(&take(&bench.log));
    assert_eq!((bench.stage(), ended), (Some(WindowStage::Ended), vec![(1, WindowOutcome::Interrupted)]), "the call that waited is interrupted");
    let late = bench.post(w::WindowsInput::WindowRead { key: key(), read_id: 1, result: Ok(None) });
    assert_eq!((bench.stage(), asks(&late)), (Some(WindowStage::Ended), 0), "a late read is stray");
}

#[test]
fn win_idle_interest_expired() {
    let mut bench = Bench::landed(true, NOW);
    let inside = bench.watch(STREAM_ID + 1, NOW + LIFE - 1);
    assert_eq!(asks(&inside), 0, "inside the lifetime a stream asks nothing");
    bench.post(w::WindowsInput::InterestMinus { key: key(), id: STREAM_ID + 1 });
    bench.post(w::WindowsInput::InterestMinus { key: key(), id: STREAM_ID });
    let expired = bench.watch(STREAM_ID + 2, NOW + LIFE);
    assert_eq!((bench.stage(), asks(&expired)), (Some(WindowStage::Loading), 1), "at fresh_until the stream starts a load");
}

#[test]
fn win_idle_load_expired() {
    let mut bench = Bench::landed(true, NOW);
    assert!(matches!(outcomes_of(&bench.load(1, NOW + LIFE - 1, false)).as_slice(), [(1, WindowOutcome::Loaded { .. })]));
    let expired = bench.load(2, NOW + LIFE, false);
    assert_eq!((bench.stage(), asks(&expired)), (Some(WindowStage::Loading), 1), "a call at fresh_until starts a load");
}

#[test]
fn win_idle_online_expired() {
    let mut bench = Bench::landed(true, NOW);
    bench.post(w::WindowsInput::Online { online: Online(false), now: NOW });
    let expired = bench.post(w::WindowsInput::Online { online: Online(true), now: NOW + LIFE });
    assert_eq!((bench.stage(), asks(&expired)), (Some(WindowStage::Loading), 1), "the network returns after fresh_until: the watched window loads");
}

#[test]
fn win_idle_online_fresh() {
    let mut bench = Bench::landed(true, NOW);
    bench.post(w::WindowsInput::Online { online: Online(false), now: NOW });
    let inside = bench.post(w::WindowsInput::Online { online: Online(true), now: NOW + LIFE - 1 });
    assert_eq!((bench.stage(), asks(&inside)), (Some(WindowStage::Idle), 0), "the network returns inside the lifetime: no request");
}

#[test]
fn win_idle_expiry_silent() {
    let mut bench = Bench::landed(true, NOW);
    take(&bench.log);
    bench.det.tick(Time(u64::MAX / 2));
    bench.det.run_until_quiet(100).expect("quiet");
    assert!(take(&bench.log).is_empty(), "the time passed fresh_until and no input came: nothing happens");
}

#[test]
fn win_landing_fresh_until() {
    let rows = Bench::landed(true, NOW);
    let mut empty = Bench::landed(false, NOW);
    let mut rows = rows;
    assert_eq!(asks(&rows.load(1, NOW + LIFE - 1, false)), 0, "a page with rows is fresh for expires_in");
    assert_eq!(asks(&empty.load(1, NOW + EMPTY_LIFE - 1, false)), 0);
    assert_eq!(asks(&empty.load(2, NOW + EMPTY_LIFE, false)), 1, "an empty page is fresh for empty_in");
}

#[test]
fn win_landing_next_page_keeps_fresh_until() {
    let mut bench = Bench::new();
    bench.watch(STREAM_ID, NOW);
    bench.read(Ok(None));
    bench.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["r1".into()], Some("c1".into())), now: NOW });
    bench.post(w::WindowsInput::Committed(Committed { batch: 1, revision: 20, tables: Vec::new(), models: Default::default(), stale_parents: Vec::new(), position: 0 }));
    bench.post(w::WindowsInput::Load { key: key(), id: 1, goal: w::Goal::Next, force: false, store_revision: 0, now: NOW + 100_000 });
    bench.post(w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(2, vec!["r2".into()], None), now: NOW + 200_000 });
    bench.post(w::WindowsInput::Committed(Committed { batch: 2, revision: 21, tables: Vec::new(), models: Default::default(), stale_parents: Vec::new(), position: 0 }));
    assert_eq!(asks(&bench.load(2, NOW + LIFE, false)), 1, "page 2 did not move landed_at: the window expires from page 1");
}

#[test]
fn win_prop_freshness_by_input_time() {
    let mut bench = Bench::landed(true, NOW);
    let silent = bench.det.trace().len();
    bench.det.tick(Time(u64::MAX / 2));
    bench.det.run_until_quiet(100).expect("quiet");
    assert_eq!(bench.det.trace().len(), silent, "without inputs there are no requests and no schedules");
    for (now, expect) in [(NOW + LIFE - 1, 0), (NOW + LIFE, 1)] {
        let mut probe = Bench::landed(true, NOW);
        assert_eq!(asks(&probe.watch(STREAM_ID + 1, now)), expect, "now = {now}");
    }
}

#[test]
fn win_prop_clock_back() {
    let mut bench = Bench::landed(true, NOW);
    assert_eq!(asks(&bench.watch(STREAM_ID + 1, NOW - 1)), 1, "a clock before landed_at does not make the window fresh");
}

#[test]
fn win_prop_freshness_survives_open() {
    for (stale, landed_at, now, asked) in [(false, NOW - 10_000, NOW, 0), (false, NOW - LIFE, NOW, 1), (true, NOW - 10_000, NOW, 1)] {
        let mut bench = Bench::new();
        bench.watch(STREAM_ID, now);
        let opened = bench.read(Ok(Some(saved(landed_at, stale))));
        assert_eq!(asks(&opened), asked, "landed_at = {landed_at}, stale = {stale}");
    }
}

use std::sync::Arc;
use std::time::Duration;

use super::rig_tests::{bound, Gate, Held, Rig};

/// The generator of the stress rig, which a feature gates: the same splitmix steps, so a seed picks the same actions in every build.
struct Dice(u64);

impl Dice {
    fn new(seed: u64) -> Self { Self(seed) }
    fn below(&mut self, limit: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (mixed ^ (mixed >> 31)) % limit.max(1)
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> { (!items.is_empty()).then(|| &items[self.below(items.len() as u64) as usize]) }
}

/// The smallest step count that still fails, by halving within the budget.
fn shrink(reached: u64, mut fails: impl FnMut(u64) -> bool, budget: Duration) -> Option<u64> {
    let started = std::time::Instant::now();
    let (mut low, mut high) = (1, reached.max(1));
    if !fails(high) { return None; }
    while low < high && started.elapsed() < budget {
        let middle = low + (high - low) / 2;
        if fails(middle) { high = middle } else { low = middle + 1 }
    }
    Some(high)
}

const SEEDS: u64 = 16;
const STEPS: u64 = 32;

#[derive(Clone, Copy, Debug)]
enum Act {
    Login,
    Exit { revoke: bool },
    SessionReset,
    Refresh,
    OpenHeld,
    CancelHeld,
    Tick,
    Offline,
    Overlap(Transition),
}

#[derive(Clone, Copy, Debug)]
enum Transition {
    SessionReset,
    Exit,
}

struct Tracked {
    held: Held,
    exits: Vec<(String, (usize, usize, usize, usize))>,
}

/// The independent model of the app: what the client holds and what every kept view must show.
struct Machine {
    rig: Rig,
    dice: Dice,
    credential: Option<String>,
    ended: bool,
    data_empty: bool,
    views: Vec<Tracked>,
    accounts: usize,
    late: Vec<String>,
    tape: Vec<String>,
}

async fn eventually(what: &str, mut ready: impl FnMut() -> bool) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(10), async { while !ready() { tokio::task::yield_now().await; } }).await.map_err(|_| format!("timed out waiting for {what}"))
}

impl Machine {
    async fn new(seed: u64) -> Machine {
        Machine { rig: Rig::encrypted().await, dice: Dice::new(seed), credential: None, ended: true, data_empty: true, views: Vec::new(), accounts: 0, late: Vec::new(), tape: Vec::new() }
    }

    fn live(&self) -> bool { self.credential.is_some() && !self.ended }

    fn allowed(&self) -> Vec<Act> {
        let mut acts = vec![Act::Tick];
        if self.views.len() < 3 && (self.data_empty || self.live()) { acts.push(Act::OpenHeld); }
        if !self.views.is_empty() { acts.push(Act::CancelHeld); }
        if self.credential.is_none() {
            acts.extend([Act::Login, Act::Login, Act::Login]);
        } else {
            acts.extend([Act::Exit { revoke: true }, Act::Exit { revoke: false }, Act::SessionReset, Act::Refresh]);
        }
        if self.live() {
            acts.extend([Act::Offline, Act::Overlap(Transition::SessionReset), Act::Overlap(Transition::Exit)]);
        }
        acts
    }

    async fn run(&mut self, acts: Act) -> Result<(), String> {
        self.tape.push(format!("{acts:?}"));
        match acts {
            Act::Login => {
                self.accounts += 1;
                let account = format!("acc{}", self.accounts);
                self.rig.login_ios(&account).await;
                self.credential = Some(account);
                self.ended = false;
                self.data_empty = false;
            }
            Act::Exit { revoke } => self.exit(revoke).await?,
            Act::SessionReset => {
                self.rig.database.session_reset().await.unwrap();
                self.credential = None;
                self.ended = true;
                self.quiet().await?;
            }
            Act::Refresh => {
                bound(self.rig.database.refresh(self.rig.database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.map_err(|error| format!("refresh with a stored credential failed: {error:?}"))?;
                self.ended = false;
                self.data_empty = false;
            }
            Act::OpenHeld => self.views.push(Tracked { held: Held::open(&self.rig.database), exits: Vec::new() }),
            Act::CancelHeld => {
                let index = self.dice.below(self.views.len() as u64) as usize;
                self.views.remove(index).held.cancel_all();
            }
            Act::Tick => self.rig.clock.advance(60_000),
            Act::Offline => {
                self.rig.database.set_online(false).unwrap();
                bound(crate::ffi::settled(&self.rig.database)).await.map_err(|error| format!("settle offline: {error:?}"))?;
                self.rig.database.set_online(true).unwrap();
            }
            Act::Overlap(transition) => self.overlap(transition).await?,
        }
        self.settle_and_check().await
    }

    async fn exit(&mut self, revoke: bool) -> Result<(), String> {
        let database = self.rig.database.clone();
        if revoke {
            bound(database.run(database.session_epoch(), "UnregisterDevice".into(), "{}".into())).await.map_err(|error| format!("unregister: {error:?}"))?;
            bound(database.session_call("logout".into(), "{}".into())).await.map_err(|error| format!("logout call: {error:?}"))?;
        }
        database.session_reset().await.unwrap();
        let account = self.credential.take();
        self.ended = true;
        self.data_empty = true;
        if let Some(account) = account {
            for view in &mut self.views { view.exits.push((account.clone(), view.held.deliveries())); }
        }
        self.quiet().await
    }

    /// After a transition that closes the session, nothing reaches the server while the session stays closed.
    async fn quiet(&self) -> Result<(), String> {
        self.rig.world.forbid(true);
        let settled = bound(crate::ffi::settled(&self.rig.database)).await;
        self.rig.world.forbid(false);
        settled.map_err(|error| format!("settle after the session closed: {error:?}"))?;
        let violations = self.rig.world.violations();
        if violations.is_empty() { Ok(()) } else { Err(format!("a request reached the server while the session was closed: {violations:?}")) }
    }

    async fn overlap(&mut self, transition: Transition) -> Result<(), String> {
        bound(crate::ffi::settled(&self.rig.database)).await.map_err(|error| format!("settle before the overlap: {error:?}"))?;
        let label = format!("late{}", self.late.len());
        let gate: Arc<Gate> = self.rig.world.hold_next("Users").as_account(&label);
        let database = self.rig.database.clone();
        let epoch = database.session_epoch();
        let running = tokio::spawn(async move { database.refresh(epoch, "User".into(), "all".into(), "{}".into()).await });
        tokio::time::timeout(Duration::from_secs(10), gate.entered_for("the overlapping call")).await.map_err(|_| "the overlapping call never reached the server".to_string())?;
        match transition {
            Transition::SessionReset => {
                self.rig.database.session_reset().await.unwrap();
                self.credential = None;
                self.ended = true;
            }
            Transition::Exit => {
                self.rig.database.session_reset().await.unwrap();
                let account = self.credential.take();
                self.ended = true;
                self.data_empty = true;
                if let Some(account) = account {
                    for view in &mut self.views { view.exits.push((account.clone(), view.held.deliveries())); }
                }
            }
        }
        gate.release();
        drop(bound(running).await.unwrap());
        self.late.push(label);
        Ok(())
    }

    async fn settle_and_check(&mut self) -> Result<(), String> {
        bound(crate::ffi::settled(&self.rig.database)).await.map_err(|error| format!("settle: {error:?}"))?;
        let hits = self.rig.world.hits();
        bound(crate::ffi::settled(&self.rig.database)).await.map_err(|error| format!("second settle: {error:?}"))?;
        if self.rig.world.hits() != hits {
            return Err(format!("an idle database sent {:?}", self.rig.world.log().iter().skip(hits).collect::<Vec<_>>()));
        }
        if let (true, Some(account)) = (self.live(), self.credential.clone()) {
            for (index, view) in self.views.iter().enumerate() {
                view.held.follow(&self.rig.database);
                eventually(&format!("view {index} to show {account}"), || view.held.displays(&account)).await?;
            }
        }
        if self.live() && !self.views.is_empty() { self.data_empty = false; }
        if self.ended && self.data_empty {
            for (index, view) in self.views.iter().enumerate() {
                if !view.held.is_clear() { return Err(format!("view {index} shows rows although the data was cleared and the session is closed")); }
            }
        }
        for (index, view) in self.views.iter().enumerate() {
            for (account, mark) in &view.exits {
                if !view.held.quiet_since(*mark, account) { return Err(format!("view {index} showed {account} after its exit")); }
            }
            for label in &self.late {
                if !view.held.never_showed(label) { return Err(format!("view {index} showed {label}, the answer of a call that a transition ended")); }
            }
        }
        Ok(())
    }
}

async fn run_seed(seed: u64, steps: u64) -> Result<(), String> {
    let mut machine = Machine::new(seed).await;
    let outcome = async {
        for step in 0..steps {
            let acts = machine.allowed();
            let act = *machine.dice.pick(&acts).expect("an allowed action");
            machine.run(act).await.map_err(|failure| format!("step {step} {act:?}: {failure}"))?;
        }
        Ok::<(), String>(())
    }
    .await;
    let tape = machine.tape.clone();
    for view in &machine.views { view.held.cancel_all(); }
    machine.rig.database.close_now();
    outcome.map_err(|failure| format!("{failure}\ntape {tape:?}"))
}

/// G09: 16 seeds of 32 steps over login, exit, session reset, data logout, reset, refresh, held views, ticks, offline and overlapping calls. After every step the model decides what each kept view must show and what may reach the server. A failure prints the seed and the shortest failing prefix.
#[tokio::test(flavor = "multi_thread")]
async fn seeded_lifecycle_steps_keep_every_view_and_every_request_consistent() {
    for seed in 0..SEEDS {
        let Err(failure) = run_seed(seed, STEPS).await else { continue };
        let handle = tokio::runtime::Handle::current();
        let shortest = tokio::task::block_in_place(|| shrink(STEPS, |steps| handle.block_on(run_seed(seed, steps)).is_err(), Duration::from_secs(20)));
        panic!("seed {seed} fails; shortest failing prefix {shortest:?} steps; replay with seed {seed}\n{failure}");
    }
}

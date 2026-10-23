use std::time::Duration;

use serde_json::Map;
use tokio::task::JoinHandle;

use super::load::{LoadOptions, LoadOutcome};
use super::load_machine::LoadPhase;
use super::load_machine_oracle_tests::{expected, replay, Random};
use super::rig::*;
use super::SyncError;
use crate::transport::Reply;

const SEEDS: u64 = 300;
const COMMANDS: usize = 25;

type Flight = JoinHandle<Result<LoadOutcome, SyncError>>;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

fn enqueue(rig: &Rig, random: &mut Random) {
    let delay = Duration::from_secs([0, 1, 5, 30][random.below(4)]);
    let reply = if random.chance(5) { Reply::graphql_error("nope", "FORBIDDEN") } else { feed_reply(&["a", "b"], random.chance(2), "c1") };
    rig.transport.enqueue("MomentsFeed", reply.after(delay));
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_17_the_state_of_a_real_sync_equals_the_oracle_over_random_commands() {
    for seed in 1..=SEEDS {
        let mut random = Random(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let rig = rig();
        let state = rig.sync.relation_state("moments", "feed", &none()).unwrap();
        let window = rig.sync.window_key("moments", "feed", &none()).unwrap();
        let owner = &rig.sync.inner.owner;
        let mut flights: Vec<Flight> = Vec::new();
        let mut commands = Vec::new();
        for step in 0..COMMANDS {
            let command = random.below(9);
            commands.push(command);
            match command {
                0 | 1 => {
                    enqueue(&rig, &mut random);
                    let (sync, force) = (rig.sync.clone(), random.chance(2));
                    flights.push(tokio::spawn(async move { sync.load("moments", "feed", &none(), LoadOptions { force }).await }));
                }
                2 => {
                    enqueue(&rig, &mut random);
                    let sync = rig.sync.clone();
                    flights.push(tokio::spawn(async move { sync.load_more("moments", "feed", &none()).await }));
                }
                3 if !flights.is_empty() => flights.remove(random.below(flights.len())).abort(),
                4 => rig.sync.set_online(false),
                5 => rig.sync.set_online(true),
                6 => rig.db.reset(&["moments"]).unwrap(),
                7 => rig.clock.advance(Duration::from_secs(400)),
                _ => {}
            }
            tokio::time::advance(Duration::from_millis(1)).await;
            let run = replay(&owner.history());
            assert_eq!(Some(state.borrow().clone()), expected(&run, &window), "seed {seed} step {step} commands {commands:?}");
            assert_eq!(owner.orphan_ends(), 0, "seed {seed} step {step}: the wiring ended a flight that never started");
        }
        for flight in flights.drain(..) {
            flight.abort();
        }
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert_eq!(owner.flights(&window), 0, "seed {seed}: a flight outlived its caller, commands {commands:?}");
        assert_ne!(state.borrow().phase, LoadPhase::Loading, "seed {seed}: commands {commands:?}");
        assert_eq!(owner.orphan_ends(), 0, "seed {seed}");
    }
}

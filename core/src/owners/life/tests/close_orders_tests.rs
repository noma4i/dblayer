use std::sync::{Arc, Mutex, PoisonError};

use crate::owners::contract::Drained;
use crate::owners::lifecycle::{Lifecycle, LifecycleEffect, LifecycleError, LifecycleInput, LifecyclePhase, LifecycleState, ALL_OWNERS};
use crate::owners::store::{Done, StoreEffect, StoreInput, StoreMode, StoreState, StoreWriter};
use crate::runtime::{Address, Back, DetScheduler, EffectRunner, EpochStamp};

type Bag<E> = Arc<Mutex<Vec<E>>>;

struct Keep<E>(Bag<E>);

impl EffectRunner<Lifecycle> for Keep<LifecycleEffect> {
    fn run(&mut self, _stamp: EpochStamp, effect: LifecycleEffect, _back: &Back<Lifecycle>) { self.0.lock().unwrap_or_else(PoisonError::into_inner).push(effect); }
}

impl EffectRunner<StoreWriter> for Keep<StoreEffect> {
    fn run(&mut self, _stamp: EpochStamp, effect: StoreEffect, _back: &Back<StoreWriter>) { self.0.lock().unwrap_or_else(PoisonError::into_inner).push(effect); }
}

const TO_STORE: u32 = 100;
const TO_LIFECYCLE: u32 = 101;
const PLATFORM: u32 = 102;
const READS: u32 = 103;
const SECOND_CLOSE: u32 = 104;
const SAVES: u32 = 105;
const DRAINS: u32 = 10;

struct World {
    det: DetScheduler,
    life: Address<Lifecycle>,
    store: Address<StoreWriter>,
    life_fx: Bag<LifecycleEffect>,
    store_fx: Bag<StoreEffect>,
    replies: Vec<(u64, Result<(), LifecycleError>)>,
}

impl World {
    fn new(seed: u64) -> Self {
        let (life_fx, store_fx): (Bag<LifecycleEffect>, Bag<StoreEffect>) = (Arc::default(), Arc::default());
        let mut det = DetScheduler::new(seed);
        let life = det.add::<Lifecycle>("lifecycle", LifecycleState::new(), Keep(life_fx.clone()));
        let store = det.add::<StoreWriter>("store", StoreState::new(), Keep(store_fx.clone()));
        Self { det, life, store, life_fx, store_fx, replies: Vec::new() }
    }

    fn pump(&mut self) {
        let life_fx = std::mem::take(&mut *self.life_fx.lock().unwrap_or_else(PoisonError::into_inner));
        for effect in life_fx {
            match effect {
                LifecycleEffect::Closing => {
                    for (index, owner) in ALL_OWNERS.iter().enumerate() {
                        self.det.inject(DRAINS + index as u32, self.life, EpochStamp::Control, LifecycleInput::Drained(Drained { owner: owner.name() }));
                    }
                }
                LifecycleEffect::StopObserving => self.det.inject(PLATFORM, self.life, EpochStamp::Control, LifecycleInput::ObserversStopped),
                LifecycleEffect::StoreClose => self.det.inject(TO_STORE, self.store, EpochStamp::Control, StoreInput::Close),
                LifecycleEffect::Publish { publication_id, published } => self.det.inject(TO_STORE, self.store, EpochStamp::Control, StoreInput::Published { publication_id, published }),
                LifecycleEffect::Reply { id, result } => self.replies.push((id, result)),
                _ => {}
            }
        }
        let store_fx = std::mem::take(&mut *self.store_fx.lock().unwrap_or_else(PoisonError::into_inner));
        for effect in store_fx {
            match effect {
                StoreEffect::SavePublished { io_id, .. } => self.det.inject(SAVES, self.store, EpochStamp::Control, StoreInput::Erased { io_id, result: Ok(Done { revision: 1 }) }),
                StoreEffect::CloseReads => {
                    self.det.inject(READS, self.store, EpochStamp::Control, StoreInput::ReadsClosed { ids: vec![7, 8] });
                    for read_id in [7, 8] { self.det.inject(READS, self.store, EpochStamp::Control, StoreInput::ReadFinished { read_id }); }
                }
                StoreEffect::Saved { publication_id } => self.det.inject(TO_LIFECYCLE, self.life, EpochStamp::Control, LifecycleInput::Saved { publication_id }),
                StoreEffect::StoreClosed => self.det.inject(TO_LIFECYCLE, self.life, EpochStamp::Control, LifecycleInput::StoreClosed),
                _ => {}
            }
        }
    }

    fn run_to_quiet(&mut self) {
        for _ in 0..10_000 {
            self.pump();
            if self.det.choices().is_empty() { return; }
            self.det.step();
        }
        panic!("the close did not come to rest in 10000 steps");
    }
}

/// P-LIFE-22 on the real owners: Lifecycle and Store with their runners replaced by recorders, every delivery lane (each owner `Drained`, `ObserversStopped`, the Store requests, the Store answers, the reads, the save of the closing publication, a joining second `close`) in a seeded random order. Each `close` gets one `Ok` and both owners end closed.
#[test]
fn life_prop_close_handshake_every_lane_order() {
    for seed in 0..3000u64 {
        let mut world = World::new(seed);
        world.det.post(world.life, EpochStamp::Control, LifecycleInput::Close { reply: 1 });
        let joined = seed % 2 == 1;
        if joined { world.det.inject(SECOND_CLOSE, world.life, EpochStamp::Control, LifecycleInput::Close { reply: 2 }); }
        world.run_to_quiet();
        assert!(matches!(world.det.state(world.life).map(LifecycleState::phase), Some(LifecyclePhase::Closed)), "seed {seed}: Lifecycle is not Closed");
        assert_eq!(world.det.state(world.store).map(StoreState::mode), Some(StoreMode::Closed), "seed {seed}: Store is not Closed");
        let mut replies = world.replies.clone();
        replies.sort_by_key(|reply| reply.0);
        assert_eq!(replies, if joined { vec![(1, Ok(())), (2, Ok(()))] } else { vec![(1, Ok(()))] }, "seed {seed}: one Ok per close");
    }
}

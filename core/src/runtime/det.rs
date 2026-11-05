use std::any::Any;

use super::cell::{Cell, EffectRunner, Ran};
use super::owner::{Address, Cause, Epoch, Message, Owner, Stamp, Time};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEntry {
    pub owner: &'static str,
    pub stamp: Stamp,
    pub input: String,
    pub effects: String,
    pub dropped: bool,
}

trait ErasedSlot {
    fn name(&self) -> &'static str;
    fn ready(&self) -> bool;
    fn run_one(&mut self) -> Option<Ran>;
    fn deliver(&mut self, stamp: Stamp, payload: Box<dyn Message>);
    fn start_epoch(&mut self, epoch: Epoch);
    fn end_epoch(&mut self, epoch: Epoch, cause: Cause);
    fn close(&mut self);
    fn tick(&mut self, now: Time);
    fn as_any(&self) -> &dyn Any;
}

struct Slot<O: Owner> {
    name: &'static str,
    cell: Cell<O>,
    runner: Box<dyn EffectRunner<O>>,
}

impl<O: Owner> ErasedSlot for Slot<O> {
    fn name(&self) -> &'static str { self.name }

    fn ready(&self) -> bool { self.cell.ready() }

    fn run_one(&mut self) -> Option<Ran> { self.cell.run_one(self.runner.as_mut(), true) }

    fn deliver(&mut self, stamp: Stamp, payload: Box<dyn Message>) { self.cell.push_message(stamp, payload); }

    fn start_epoch(&mut self, epoch: Epoch) { self.cell.start_epoch(epoch); }

    fn end_epoch(&mut self, epoch: Epoch, cause: Cause) { self.cell.end_epoch(epoch, cause); }

    fn close(&mut self) { self.cell.close(); }

    fn tick(&mut self, now: Time) { self.cell.tick(now); }

    fn as_any(&self) -> &dyn Any { self }
}

pub struct DetScheduler {
    slots: Vec<Box<dyn ErasedSlot>>,
    random: u64,
    trace: Vec<TraceEntry>,
}

impl DetScheduler {
    pub fn new(seed: u64) -> Self { Self { slots: Vec::new(), random: seed, trace: Vec::new() } }

    pub fn add<O: Owner>(&mut self, name: &'static str, state: O::State, runner: impl EffectRunner<O>) -> Address<O> {
        self.slots.push(Box::new(Slot::<O> { name, cell: Cell::new(state), runner: Box::new(runner) }));
        Address::new(self.slots.len() - 1)
    }

    pub fn send<O: Owner>(&mut self, to: Address<O>, stamp: Stamp, input: O::Input) { self.slots[to.index].deliver(stamp, Box::new(input)); }

    pub fn start_epoch(&mut self, epoch: Epoch) { self.slots.iter_mut().for_each(|slot| slot.start_epoch(epoch)); }

    pub fn end_epoch(&mut self, epoch: Epoch, cause: Cause) { self.slots.iter_mut().for_each(|slot| slot.end_epoch(epoch, cause)); }

    pub fn close(&mut self) { self.slots.iter_mut().for_each(|slot| slot.close()); }

    pub fn tick(&mut self, now: Time) { self.slots.iter_mut().for_each(|slot| slot.tick(now)); }

    pub fn ready(&self) -> Vec<usize> { (0..self.slots.len()).filter(|index| self.slots[*index].ready()).collect() }

    pub fn step_choice(&mut self, choice: usize) -> bool {
        let ready = self.ready();
        let Some(&index) = ready.get(choice) else { return false };
        let Some(ran) = self.slots[index].run_one() else { return false };
        self.trace.push(TraceEntry { owner: self.slots[index].name(), stamp: ran.stamp, input: ran.input, effects: ran.effects, dropped: ran.dropped });
        for outbound in ran.sends {
            self.slots[outbound.to].deliver(outbound.stamp, outbound.payload);
        }
        true
    }

    pub fn step(&mut self) -> bool {
        let ready = self.ready().len();
        if ready == 0 {
            return false;
        }
        let choice = (self.next_random() % ready as u64) as usize;
        self.step_choice(choice)
    }

    pub fn run_until_quiet(&mut self, limit: usize) -> Result<usize, usize> {
        let mut steps = 0;
        while !self.ready().is_empty() {
            if steps == limit {
                return Err(steps);
            }
            self.step();
            steps += 1;
        }
        Ok(steps)
    }

    pub fn trace(&self) -> &[TraceEntry] { &self.trace }

    pub fn state<O: Owner>(&self, address: Address<O>) -> &O::State {
        &self.slots[address.index].as_any().downcast_ref::<Slot<O>>().expect("address type matches owner").cell.state
    }

    pub fn dropped<O: Owner>(&self, address: Address<O>) -> u64 {
        self.slots[address.index].as_any().downcast_ref::<Slot<O>>().expect("address type matches owner").cell.dropped()
    }

    fn next_random(&mut self) -> u64 {
        self.random = self.random.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.random;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }
}

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::json;

use super::bridge_support_tests::message;
use super::flow_fixtures::{add_chat, cell};
use super::rig_events::events;
use super::rig_tests::{bound, Fault, Gate, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::wire::{Cell, WireRow};
use crate::ffi::{ReadFetch, ReadRequest, WireOperationReceipt};
use crate::test_wait::settled_showing;

const SEEDS: u64 = 100;
const STEPS: usize = 24;

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }
    fn below(&mut self, bound: usize) -> usize { (self.next() % bound as u64) as usize }
    fn pick<T: Clone>(&mut self, items: &[T]) -> T { items[self.below(items.len())].clone() }
}

#[derive(Clone, Debug)]
struct Msg { id: String, seq: i64, at: i64 }

#[derive(Clone, Debug, PartialEq)]
struct Own { temp: String, at: i64 }

#[derive(Clone, Debug, PartialEq, Default)]
struct Shown { id: Option<String>, at: Option<i64>, seq: Option<i64> }

enum Held {
    Send { gate: Arc<Gate>, receipt: WireOperationReceipt, own: Own, accepted: Option<Msg> },
    Delete { gate: Arc<Gate>, receipt: WireOperationReceipt, id: String, confirmed: bool },
}

struct Reference {
    server: BTreeMap<String, (Msg, bool)>,
    received: BTreeMap<String, Msg>,
    own: Vec<Own>,
    kept: Vec<String>,
    event_deleted: BTreeSet<String>,
    hidden: Option<String>,
    pending: Option<String>,
    next: usize,
    next_seq: i64,
    clock: i64,
    used: BTreeSet<i64>,
}

impl Reference {
    fn new() -> Self { Reference { server: BTreeMap::new(), received: BTreeMap::new(), own: Vec::new(), kept: Vec::new(), event_deleted: BTreeSet::new(), hidden: None, pending: None, next: 1, next_seq: 1, clock: 100, used: BTreeSet::new() } }

    fn server_message(&mut self, random: &mut Random) -> Msg {
        self.clock += 1 + random.below(3) as i64;
        self.used.insert(self.clock);
        let message = Msg { id: format!("m{}", self.next), seq: self.next_seq, at: self.clock };
        self.next += 1;
        self.next_seq += 1;
        self.server.insert(message.id.clone(), (message.clone(), false));
        message
    }

    fn device_time(&mut self, random: &mut Random) -> i64 {
        loop {
            let at = self.clock - 40 + random.below(81) as i64;
            if at > 0 && self.used.insert(at) { return at; }
        }
    }

    fn visible(&self) -> Vec<&Msg> { self.received.values().filter(|message| self.hidden.as_deref() != Some(message.id.as_str())).collect() }

    fn newest_confirmed(&self) -> Option<&Msg> { self.visible().into_iter().max_by_key(|message| (message.seq, message.at)) }

    fn expected(&self) -> Shown {
        let confirmed = self.newest_confirmed();
        match (self.own.iter().max_by_key(|own| own.at), confirmed) {
            (None, None) => Shown::default(),
            (None, Some(message)) => Shown { id: Some(message.id.clone()), at: Some(message.at), seq: Some(message.seq) },
            (Some(own), confirmed) => Shown { id: Some(own.temp.clone()), at: Some(confirmed.map_or(own.at, |message| message.at.max(own.at))), seq: confirmed.map(|message| message.seq) },
        }
    }

    fn visible_ids(&self) -> BTreeSet<String> { self.visible().iter().map(|message| message.id.clone()).chain(self.own.iter().map(|own| own.temp.clone())).collect() }
}

fn shown(rig: &Rig, row: &WireRow) -> Shown {
    let int = |field: &str| match cell(rig, "Chat", row, field) { Cell::Int { value } => Some(value), _ => None };
    Shown { id: match cell(rig, "Chat", row, "lastMessageId") { Cell::Text { value } => Some(value), _ => None }, at: int("lastMessageAt"), seq: int("lastSequenceNumber") }
}

fn last_shown(rig: &Rig, rows: &Rows) -> Option<Shown> { rows.0.lock().unwrap().iter().find(|row| row.id == "c1").map(|row| shown(rig, row)) }

fn frames(rig: &Rig, rows: &Rows) -> Vec<Shown> {
    rows.1.lock().unwrap().iter().filter_map(|seen| seen.rows.iter().find(|row| row.id == "c1")).map(|row| shown(rig, row)).collect()
}

fn message_row(message: &Msg) -> serde_json::Value { json!({"id": message.id, "chatId": "c1", "text": format!("text {}", message.id), "at": message.at, "seq": message.seq}) }

async fn land(rig: &Rig, model: &str, row: serde_json::Value) { rig.land_unsettled(model, row).await; }

struct Run {
    rig: Rig,
    rows: Arc<Rows>,
    keep: bool,
    reference: Reference,
    held: Option<Held>,
    program: Vec<String>,
}

impl Run {
    async fn open(keep: bool) -> Run {
        let rig = Rig::signed_as(World::new("server", "A", true), false, events(move |models| {
            add_chat(models);
            if keep {
                let message = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Message").unwrap();
                let create = message["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "create").unwrap();
                create["on_failure"] = json!("keep");
            }
        })).await;
        rig.land("Chat", json!({"id": "c1", "name": "c1", "unreadCount": 0, "pinned": false, "muted": false})).await;
        let rows = Arc::new(Rows::default());
        let stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, rows.clone());
        bound(stream.wait_for_initial_load()).await.unwrap();
        crate::test_wait::settled(&rig, "the chat list shows c1", || last_shown(&rig, &rows).is_some()).await;
        Run { rig, rows, keep, reference: Reference::new(), held: None, program: Vec::new() }
    }

    fn describe(&self) -> String {
        let reference = &self.reference;
        format!("; program {:?}; reference: received {:?}, own {:?}, hidden {:?}, held {}", self.program, reference.received.keys().collect::<Vec<_>>(), reference.own, reference.hidden, self.held.is_some())
    }

    async fn barrier(&self, seed: u64, step: &str) {
        let expected = self.reference.expected();
        let what = format!("seed {seed}, after step `{step}`: the chat row shows the fields of the newest child");
        settled_showing(&self.rig, &what, || format!("{}; expected {expected:?}, shown {:?}", self.describe(), last_shown(&self.rig, &self.rows)), || last_shown(&self.rig, &self.rows).as_ref() == Some(&expected)).await;
        let pointer = last_shown(&self.rig, &self.rows).and_then(|row| row.id);
        let visible = self.reference.visible_ids();
        assert!(pointer.as_ref().is_none_or(|id| visible.contains(id)), "seed {seed}, after step `{step}`: the pointer {pointer:?} names a row that is not a visible child; {}", self.describe());
        let Some(id) = pointer else { return };
        let named = Arc::new(Rows::default());
        let stream = self.rig.database.watch(ReadRequest::Entity { model: "Message".into(), id: id.clone(), fetch: ReadFetch::Never }, named.clone());
        let what = format!("seed {seed}, after step `{step}`: an entity stream opened on the pointer {id} shows that row");
        let shown = || named.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>();
        settled_showing(&self.rig, &what, || format!("{}; shown {:?}", self.describe(), shown()), || shown() == [id.clone()]).await;
        stream.cancel();
    }

    async fn step(&mut self, random: &mut Random) -> (String, bool) {
        let epoch = self.rig.database.session_epoch();
        let held_message = match &self.held { Some(Held::Send { accepted: Some(message), .. }) => Some(message.clone()), _ => None };
        let reference = &mut self.reference;
        let mut choices: Vec<&str> = vec!["foreign", "missed", "page", "parent_with_child", "parent_values", "edit", "server_delete"];
        if self.held.is_none() {
            choices.extend(["own_send", "own_send", "own_delete", "resubscribe"]);
            if !reference.kept.is_empty() { choices.push("discard"); }
        } else {
            choices.extend(["resolve", "resolve", "resolve"]);
            if held_message.is_some() { choices.extend(["held_event", "held_page"]); }
        }
        let choice = random.pick(&choices);
        let free: Vec<Msg> = reference.server.values().filter(|(message, deleted)| !deleted && reference.pending.as_deref() != Some(message.id.as_str()) && reference.hidden.as_deref() != Some(message.id.as_str())).map(|(message, _)| message.clone()).collect();
        match choice {
            "foreign" => {
                let message = reference.server_message(random);
                reference.received.insert(message.id.clone(), message.clone());
                land(&self.rig, "Message", message_row(&message)).await;
                (format!("foreign event {}", message.id), false)
            }
            "missed" => {
                let message = reference.server_message(random);
                (format!("server holds {} that no event delivered", message.id), false)
            }
            "page" => {
                let mut page = Vec::new();
                for _ in 0..1 + random.below(3) {
                    if free.is_empty() { break; }
                    let message = random.pick(&free);
                    if !page.iter().any(|held: &Msg| held.id == message.id) { page.push(message); }
                }
                for message in &page {
                    reference.received.insert(message.id.clone(), message.clone());
                    land(&self.rig, "Message", message_row(message)).await;
                }
                (format!("page {:?}", page.iter().map(|message| (message.id.as_str(), message.seq)).collect::<Vec<_>>()), false)
            }
            "held_event" => {
                let message = held_message.unwrap();
                reference.received.insert(message.id.clone(), message.clone());
                land(&self.rig, "Message", message_row(&message)).await;
                (format!("event of the held own send as {} at seq {}", message.id, message.seq), false)
            }
            "held_page" => {
                let message = held_message.unwrap();
                let mut page = vec![message];
                for other in free.iter().take(1 + random.below(2)) { page.push(other.clone()); }
                for message in &page {
                    reference.received.insert(message.id.clone(), message.clone());
                    land(&self.rig, "Message", message_row(message)).await;
                }
                (format!("page with the held own send {:?}", page.iter().map(|message| (message.id.as_str(), message.seq)).collect::<Vec<_>>()), false)
            }
            "parent_with_child" => {
                let pool: Vec<Msg> = reference.server.values().filter(|(message, deleted)| reference.pending.as_deref() != Some(message.id.as_str()) && reference.hidden.as_deref() != Some(message.id.as_str()) && (!deleted || reference.event_deleted.contains(&message.id))).map(|(message, _)| message.clone()).collect();
                if pool.is_empty() { return ("parent answer without a message".into(), false); }
                let message = random.pick(&pool);
                if !reference.server[&message.id].1 { reference.received.insert(message.id.clone(), message.clone()); }
                let mut row = json!({"id": "c1", "name": "c1", "lastMessageId": message.id, "lastMessageAt": message.at, "lastSequenceNumber": message.seq});
                row["lastMessage"] = message_row(&message);
                land(&self.rig, "Chat", row).await;
                (format!("parent answer of an earlier state with {}", message.id), false)
            }
            "parent_values" => {
                let all: Vec<Msg> = reference.server.values().map(|(message, _)| message.clone()).collect();
                if all.is_empty() { return ("parent row without a message".into(), false); }
                let message = random.pick(&all);
                land(&self.rig, "Chat", json!({"id": "c1", "name": "c1", "lastMessageId": message.id, "lastMessageAt": message.at, "lastSequenceNumber": message.seq})).await;
                (format!("parent row that names {}", message.id), false)
            }
            "edit" => {
                let visible: Vec<Msg> = reference.visible().into_iter().cloned().collect();
                if visible.is_empty() { return ("edit without a message".into(), false); }
                let message = random.pick(&visible);
                let mut row = message_row(&message);
                row["text"] = json!(format!("edited {}", message.id));
                land(&self.rig, "Message", row).await;
                (format!("edit event {}", message.id), false)
            }
            "server_delete" => {
                if free.is_empty() { return ("delete without a message".into(), false); }
                let message = random.pick(&free);
                let dropped = reference.received.remove(&message.id).is_some();
                reference.server.get_mut(&message.id).unwrap().1 = true;
                reference.event_deleted.insert(message.id.clone());
                land(&self.rig, "Message", json!({"id": message.id, "deleted": true})).await;
                (format!("server deletes {}", message.id), dropped)
            }
            "own_send" => {
                let device = reference.device_time(random);
                let refuse = random.below(3) == 0;
                let accepted = if refuse { None } else { Some(reference.server_message(random)) };
                let text = format!("own {device}");
                let hold = match &accepted {
                    Some(message) => self.rig.world.hold_then("SendMessage", Fault::Answer(json!({"data": {"sendMessage": {"__typename": "Message", "id": message.id, "chatId": "c1", "text": text, "at": message.at, "seq": message.seq}}}))),
                    None => self.rig.world.hold_then("SendMessage", Fault::Http(422)),
                };
                reference.pending = accepted.as_ref().map(|message| message.id.clone());
                let receipt = bound(self.rig.database.send(message(epoch, &text, device))).await.unwrap();
                hold.entered().await;
                let own = Own { temp: receipt.optimistic_value.clone().unwrap().id, at: device };
                reference.own.push(own.clone());
                let label = format!("own send at {device}, {}", if refuse { "will be refused" } else { "will be answered" });
                self.held = Some(Held::Send { gate: hold, receipt, own, accepted });
                (label, false)
            }
            "own_delete" => {
                let visible: Vec<Msg> = reference.visible().into_iter().cloned().collect();
                if visible.is_empty() { return ("own delete without a message".into(), false); }
                let message = random.pick(&visible);
                let confirmed = random.below(3) != 0;
                let hold = if confirmed { self.rig.world.hold_next("MessageDelete") } else { self.rig.world.hold_then("MessageDelete", Fault::Http(422)) };
                let receipt = bound(self.rig.database.destroy(epoch, "Message".into(), message.id.clone())).await.unwrap();
                hold.entered().await;
                reference.hidden = Some(message.id.clone());
                self.held = Some(Held::Delete { gate: hold, receipt, id: message.id.clone(), confirmed });
                (format!("own delete of {} will be {}", message.id, if confirmed { "confirmed" } else { "refused" }), true)
            }
            "resolve" => match self.held.take().unwrap() {
                Held::Send { gate, receipt, own, accepted } => {
                    gate.release();
                    let outcome = bound(self.rig.database.confirmed(receipt, false)).await;
                    assert_eq!(outcome.is_ok(), accepted.is_some(), "the outcome of the send; {}", self.describe());
                    let reference = &mut self.reference;
                    reference.pending = None;
                    match accepted {
                        Some(message) => {
                            reference.own.retain(|held| *held != own);
                            reference.received.insert(message.id.clone(), message);
                        }
                        None if self.keep => reference.kept.push(own.temp.clone()),
                        None => reference.own.retain(|held| *held != own),
                    }
                    ("send resolved".into(), false)
                }
                Held::Delete { gate, receipt, id, confirmed } => {
                    gate.release();
                    let outcome = bound(self.rig.database.confirmed(receipt, false)).await;
                    assert_eq!(outcome.is_ok(), confirmed, "the outcome of the delete; {}", self.describe());
                    let reference = &mut self.reference;
                    reference.hidden = None;
                    if confirmed {
                        reference.received.remove(&id);
                        reference.server.get_mut(&id).unwrap().1 = true;
                    }
                    ("delete resolved".into(), false)
                }
            },
            "discard" => {
                let temp = reference.kept.remove(random.below(reference.kept.len()));
                reference.own.retain(|own| own.temp != temp);
                bound(self.rig.database.discard(epoch, "Message".into(), temp.clone(), None)).await.unwrap();
                (format!("discard of the kept row {temp}"), false)
            }
            "resubscribe" => {
                self.rig.database.set_online(false).await.unwrap();
                self.rig.settle().await;
                self.rig.database.set_online(true).await.unwrap();
                self.rig.await_subscribed().await;
                ("the connection is lost and comes back".into(), false)
            }
            other => unreachable!("{other}"),
        }
    }
}

async fn program(seed: u64) -> Result<(), String> {
    let mut random = Random(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
    let mut run = Run::open(seed % 2 == 0).await;
    let mut floor: Option<Shown> = None;
    let mut seen = 0;
    for _ in 0..STEPS {
        let (label, dropped) = run.step(&mut random).await;
        run.program.push(label.clone());
        run.barrier(seed, &label).await;
        let all = frames(&run.rig, &run.rows);
        if dropped { floor = None; }
        for frame in &all[seen..] {
            if let Some(held) = &floor {
                if frame.seq < held.seq { return Err(format!("seed {seed}, step `{label}`: the sequence number went back from {:?} to {:?}; {}", held.seq, frame.seq, run.describe())); }
            }
            floor = Some(frame.clone());
        }
        seen = all.len();
    }
    if let Some(Held::Send { gate, .. } | Held::Delete { gate, .. }) = &run.held { gate.release(); }
    run.rig.database.close_now();
    Ok(())
}

/// P-DATA-103, P-DATA-56..60: whatever the order of the sends, answers, refusals, events, pages, stale parent answers, edits and deletes, the chat row shows the fields of the newest child, its sequence number does not go back unless a confirmed child left, and its pointer names a visible child.
#[tokio::test(flavor = "multi_thread")]
async fn sets_fields_follow_the_newest_child_in_every_order() {
    let runs: Vec<_> = (1..=SEEDS).map(|seed| tokio::spawn(program(seed))).collect();
    for run in runs {
        match run.await {
            Err(error) => std::panic::resume_unwind(error.into_panic()),
            Ok(Err(failure)) => panic!("{failure}"),
            Ok(Ok(())) => {}
        }
    }
}

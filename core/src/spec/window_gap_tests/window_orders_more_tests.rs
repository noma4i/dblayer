use super::window_fresh_tests::{opening_twin, saved, LIFE};
use super::window_media_tests::{answer_marker, commits, marker};
use super::window_orders_tests::{at, orders, outcomes_of, run, settle, Ev, Seen};
use super::*;
use crate::spec::support::committed_at;

// Orders of the inputs of a window (P-WIN-*), second part: freshness, opening, power saving, media, epoch, keys, reconnect.

fn requests_of(emitted: Vec<Emitted<WindowsEffect>>) -> Vec<(WindowKey, u64, u32, u32, Option<String>)> {
    runs(emitted).into_iter().filter_map(|effect| if let WindowsEffect::Window { key, effect: WindowEffect::Request { request, attempt, page, cursor, .. } } = effect { Some((key, request, attempt, page, cursor)) } else { None }).collect()
}

#[test]
fn win_orders_dictionary_fields() {
    let all = orders(&[Ev::Offline, Ev::Call1], &[]);
    assert_eq!(all.len(), 2);
    for order in all {
        let mut twin = Twin::new();
        twin.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(1), generation: 5 }));
        let mut seen = Seen::default();
        for event in order.iter().copied() {
            run(&mut twin, &mut seen, event);
        }
        assert_eq!((twin.stage(), seen.flying()), (Some(WindowStage::Waiting), 0), "{order:?}: a window created or running when the field says offline waits");
        run(&mut twin, &mut seen, Ev::Online);
        settle(&mut twin, &mut seen);
        assert_eq!(outcomes_of(&twin, 1), [Result3::Loaded], "{order:?}");
    }
}

/// The first load after the open follows the freshness and the debt saved before the close and lands on the saved generation with Loaded, whatever the order of the stream, the call and the read.
#[test]
fn win_orders_open_load() {
    {
        for (read, asked) in [(saved(NOW - 10_000, false), 0), (saved(NOW - LIFE, false), 1), (saved(NOW - 10_000, true), 1)] {
            let all: Vec<_> = orders(&[Ev::Watch, Ev::Call1, Ev::Respond], &[]).into_iter().filter(|order| order[0] != Ev::Respond).collect();
            assert_eq!(all.len(), 4);
            for order in all {
                let mut twin = opening_twin();
                let mut seen = Seen::default();
                for event in order.iter().copied() {
                    let effects = match event {
                        Ev::Watch => twin.watch(),
                        Ev::Call1 => twin.call(1, Goal::First),
                        _ => twin.step(Input::Read(read)),
                    };
                    seen.add(&effects);
                }
                assert_eq!(seen.requests, asked, "{order:?} {read:?}");
                assert_eq!(outcomes_of(&twin, 1), if asked == 0 { vec![Result3::Loaded] } else { Vec::new() }, "{order:?} {read:?}");
            }
        }
    }
    {
        let all: Vec<_> = orders(&[Ev::Watch, Ev::Call1, Ev::Respond], &[]).into_iter().filter(|order| order[0] != Ev::Respond).collect();
        assert_eq!(all.len(), 4);
        for order in all {
            let mut twin = opening_twin();
            let mut seen = Seen::default();
            for event in order.iter().copied() {
                let effects = match event {
                    Ev::Watch => twin.watch(),
                    Ev::Call1 => twin.call(1, Goal::First),
                    _ => twin.step(Input::Read(saved(NOW - LIFE, false))),
                };
                seen.add(&effects);
            }
            settle(&mut twin, &mut seen);
            assert_eq!(seen.requests, 1, "{order:?}");
            assert_eq!(outcomes_of(&twin, 1), [Result3::Loaded], "{order:?}: a landing, not a refusal");
            assert_eq!(twin.stage(), Some(WindowStage::Idle));
        }
    }
}


/// P-WIN-25: the inputs that arrive while the saved window is read apply once, in the order of arrival, and the answer of another read changes nothing.
#[test]
fn win_orders_opening_held() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Held { Watch, Call, Leave, Stray, Answer }
    use Held::*;
    let all: Vec<_> = orders(&[Watch, Call, Leave, Stray, Answer], &[(Call, Leave), (Stray, Answer), (Answer, Leave)]).into_iter().filter(|order| order[0] != Stray).collect();
    assert_eq!(all.len(), 7);
    for order in all {
        let mut twin = opening_twin();
        let mut seen = Seen::default();
        for held in order.iter().copied() {
            let effects = match held {
                Watch => twin.watch(),
                Call => twin.call(1, Goal::First),
                Leave => twin.step(Input::InterestMinus { id: 1 }),
                Stray => twin.step(Input::Read(Read::Stray)),
                Answer => twin.step(Input::Read(Read::None)),
            };
            seen.add(&effects);
        }
        assert_eq!((twin.state().interest(&key()), twin.state().waiters(&key())), (vec![u64::from(STREAM)], Vec::new()), "{order:?}");
        assert_eq!((seen.flying(), twin.stage(), outcomes_of(&twin, 1)), (1, Some(WindowStage::Loading), Vec::new()), "{order:?}: the window of the reader loads once");
    }
}


/// P-WIN-21: a key only the preload watches asks nothing while the device saves power and asks once at the end of it; a reader is not deferred.
#[test]
fn win_orders_power_saving() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Power { Save, Preload, Stale, Spend, Reader }
    use Power::*;
    let all = orders(&[Save, Preload, Stale, Spend], &[(Save, Spend)]);
    assert_eq!(all.len(), 12);
    for order in all {
        let mut twin = Twin::new();
        let (mut seen, mut saving) = (Seen::default(), false);
        for event in order.iter().copied() {
            let before = seen.requests;
            let effects = match event {
                Save => twin.step(Input::PowerSaving(true)),
                Spend => twin.step(Input::PowerSaving(false)),
                Preload => twin.step(Input::InterestPlus { id: PRELOAD }),
                Stale => twin.step(Input::Stale { in_keys: true }),
                Reader => unreachable!(),
            };
            saving = match event {
                Save => true,
                Spend => false,
                _ => saving,
            };
            seen.add(&effects);
            assert!(!saving || seen.requests == before, "{order:?}: {event:?} asked while saving");
        }
        assert_eq!(seen.requests, 1, "{order:?}: one load of the preload key");
    }
    let all = orders(&[Save, Reader], &[]);
    assert_eq!(all.len(), 2);
    for order in all {
        let mut twin = Twin::new();
        let mut seen = Seen::default();
        for event in order.iter().copied() {
            seen.add(&if event == Save { twin.step(Input::PowerSaving(true)) } else { twin.watch() });
        }
        assert_eq!(seen.requests, 1, "{order:?}: a reader is not deferred by power saving");
    }
}

/// P-WIN-22: a media chain lands its rows and marker only in the batch after the matching confirm: an abort anywhere before it leaves no batch, one after it leaves the batch sent.
#[test]
fn win_orders_media_abort() {
    type Abort = fn(u64, u32) -> Input;
    let aborts: [(&str, Abort); 4] = [
        ("offline", |_, _| Input::Online(false)),
        ("the last reader leaves", |_, _| Input::InterestMinus { id: STREAM }),
        ("a permanent error", |request, attempt| Input::TransportError { request, attempt, failure: Failure::Permanent }),
        ("a dropped request", |request, attempt| Input::Dropped { request, attempt }),
    ];
    let all = orders(&[0u8, 1, 2, 3, 4], &[(1, 2), (2, 3), (3, 4)]);
    assert_eq!(all.len(), 5);
    for (case, abort) in aborts {
        for order in &all {
            let mut twin = media_full_chain();
            let mut sent = 0;
            for tag in order.iter().copied() {
                let loading = twin.stage() == Some(WindowStage::Loading);
                let effects = match tag {
                    0 if loading => {
                        let (request, attempt) = twin.ref_request();
                        twin.step(abort(request, attempt))
                    }
                    0 => twin.step(abort(99, 99)),
                    1 if loading => twin.respond(Some(2)),
                    2 if loading => twin.respond(None),
                    3 if loading => answer_marker(&mut twin, marker(9, 1, 2)),
                    1 | 2 => twin.step(Input::Response { request: 99, attempt: 99, cursor: None, rows: true }),
                    3 => twin.step(Input::MarkerAnswer { request: 99, attempt: 99, marker: marker(9, 1, 2) }),
                    _ if twin.stage() == Some(WindowStage::Landing) => twin.land(),
                    _ => twin.step(Input::Committed { request: 99, revision: 1 }),
                };
                sent += commits(&effects);
            }
            assert_eq!(sent, usize::from(at(order, 0) > at(order, 3)), "{case} {order:?}");
        }
    }
}

fn media_full_chain() -> Twin {
    let mut twin = Twin::media();
    twin.watch();
    answer_marker(&mut twin, marker(9, 1, 0));
    twin
}

/// P-WIN-23: the first load of a key in a new epoch lands on the generation of that epoch with its own `Loaded`, whatever the order of the start, the network, the call and the late answer of the old epoch.
#[test]
fn win_prop_generation() {
    let all: Vec<_> = orders(&[Ev::Reset, Ev::Online, Ev::Call1, Ev::Respond], &[(Ev::Reset, Ev::Online), (Ev::Reset, Ev::Call1)]);
    assert_eq!(all.len(), 8);
    for (order, start) in all.into_iter().flat_map(|order| [loading as fn() -> Twin, fresh as fn() -> Twin].map(|start| (order.clone(), start))) {
        let mut twin = start();
        take(&twin.log);
        twin.window_batches();
        let (mut batches, mut fresh_request) = (Vec::new(), None);
        for event in order.iter().copied() {
            match event {
                Ev::Reset => {
                    twin.det.end_epoch(EPOCH, Ended::SessionReset);
                    twin.run();
                    twin.det.start_epoch(EPOCH + 1);
                    twin.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH + 1, identity: Some(1), generation: 3 }));
                }
                Ev::Online => twin.post_call(w::WindowsInput::Online { online: Online(true), online_seq: 1 }),
                Ev::Call1 => twin.post_call(w::WindowsInput::Load { key: key(), id: 9, goal: w::Goal::First, force: false, store_revision: 0 }),
                _ => twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["old".into()], None) }),
            }
            let emitted = take(&twin.log);
            batches.extend(twin.window_batches().into_iter().filter(|batch| batch.stamp == EpochStamp::Epoch(EPOCH + 1)));
            if let Some((_, request, attempt, ..)) = requests_of(emitted).into_iter().find(|found| found.1 != 1) {
                fresh_request = Some((request, attempt));
            }
        }
        let (request, attempt) = fresh_request.expect("the new epoch asked for its first page");
        twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH + 1), w::WindowsInput::Response { request, attempt, page: AnswerPage::new(1, vec!["new".into()], None) });
        twin.run();
        batches.extend(twin.window_batches().into_iter().filter(|batch| batch.stamp == EpochStamp::Epoch(EPOCH + 1)));
        twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH + 1), w::WindowsInput::Committed(committed_at(request, 30, &[])));
        twin.run();
        let done = runs(take(&twin.log));
        let loaded = done.iter().filter(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { id: 9, outcome: WindowOutcome::Loaded { revision: 30, .. } }, .. })).count();
        assert_eq!((batches.len(), loaded), (1, 1), "{order:?}: one batch on the new generation and the own Loaded of the call");
        assert!(batches[0].ops.iter().any(|op| matches!(op, StoreOp::Window { key, .. } if key == "feed")), "{order:?}: the batch of the new generation is the page of the window");
    }
}

/// P-WIN-24: the keys named by the source are the keys that hold the `preload` interest, whatever the order of the sets the source names.
#[test]
fn win_orders_preload_sets() {
    let other = WindowKey { model: "Moment".into(), name: "other".into() };
    let third = WindowKey { model: "Moment".into(), name: "third".into() };
    let sets = [vec![key(), other.clone()], vec![key()], vec![other.clone(), third.clone()]];
    let all = orders(&[0usize, 1, 2], &[]);
    assert_eq!(all.len(), 6);
    for order in all {
        let mut twin = Twin::new();
        let mut named = Vec::new();
        for index in order.iter().copied() {
            twin.post(w::WindowsInput::PreloadKeys { keys: sets[index].clone() });
            named.extend(sets[index].iter().cloned());
        }
        let last = &sets[*order.last().expect("an order")];
        for each in [key(), other.clone(), third.clone()] {
            let holds = twin.state().interest(&each).contains(&w::PRELOAD);
            assert_eq!(holds, last.contains(&each), "{order:?}: {each:?}");
        }
        named.sort_by(|a, b| a.name.cmp(&b.name));
        named.dedup();
        assert_eq!(twin.state().windows(), named.len(), "{order:?}: a window for every key named");
    }
}

/// P-WIN-20: keys that differ only in the direction keep their cursors apart: the next page of a direction asks with the cursor of its own answer, in every interleaving of the two chains.
#[test]
fn win_orders_direction_keys() {
    use crate::schema::PageDirection;
    const STEPS: [u8; 4] = [0, 1, 2, 3];
    let keys = [PageDirection::Forward, PageDirection::Backward].map(|direction| WindowKey { model: "Moment".into(), name: crate::store::window_key("feed", direction) });
    let items: Vec<(usize, u8)> = (0..2).flat_map(|side| STEPS.iter().map(move |step| (side, *step))).collect();
    let chain: Vec<((usize, u8), (usize, u8))> = (0..2).flat_map(|side| STEPS.windows(2).map(move |pair| ((side, pair[0]), (side, pair[1])))).collect();
    let all = orders(&items, &chain);
    assert_eq!(all.len(), 70);
    for order in all {
        let mut twin = Twin::new();
        let mut asked: [Option<(u64, u32)>; 2] = [None, None];
        for (side, step) in order.iter().copied() {
            let key = keys[side].clone();
            let cursor = format!("cursor{side}");
            match step {
                0 => twin.post_call(w::WindowsInput::Load { key: key.clone(), id: side as u64 + 1, goal: w::Goal::First, force: false, store_revision: 0 }),
                1 => {
                    let (request, attempt) = asked[side].expect("the load asked");
                    twin.post(w::WindowsInput::Response { request, attempt, page: AnswerPage::new(1, vec![format!("r{side}")], Some(cursor.clone())) });
                }
                2 => {
                    let (request, _) = asked[side].expect("the load asked");
                    twin.post(w::WindowsInput::Committed(committed_at(request, 20 + side as i64, &[])));
                }
                _ => twin.post_call(w::WindowsInput::Load { key: key.clone(), id: side as u64 + 10, goal: w::Goal::Next, force: false, store_revision: 0 }),
            }
            for (found, request, attempt, page, cursor_sent) in requests_of(take(&twin.log)) {
                let owner = keys.iter().position(|each| *each == found).expect("a request of one of the two keys");
                if page == 1 {
                    asked[owner] = Some((request, attempt));
                } else {
                    assert_eq!(cursor_sent, Some(format!("cursor{owner}")), "{order:?}: the next page of side {owner} asked with another cursor");
                }
            }
        }
    }
}

/// P-WIN-26: whatever the order of the answers of one `nodes` key, the result is the ids of the last answer in the order of the call, and a cached row is replaced only by an answer that returned it.
#[test]
fn win_orders_nodes_answers() {
    use crate::identity::RowId;
    use crate::owners::contract::BatchAuthor;
    use crate::owners::store::{RowValue, StoreBatch, StoreOp};
    use crate::store::fixtures::{all_specs, put};
    use crate::store::Store;
    let asked = ["m3", "m1", "m2"];
    let key = WindowKey::nodes("moments", &asked);
    let answers: [&[&str]; 3] = [&[], &["m1", "m3"], &["m2"]];
    let all = orders(&[0usize, 1, 2], &[]);
    assert_eq!(all.len(), 6);
    for order in all {
        let db = Store::open_models(None, all_specs(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        for id in asked {
            put(&db, "moments", serde_json::json!({"id": id, "userId": "u1", "text": format!("cached {id}")}));
        }
        for index in order.iter().copied() {
            let roots = answers[index].iter().map(|id| RowValue { row_id: (*id).into(), values: serde_json::json!({"id": id, "userId": "u1", "text": format!("served {id}")}).to_string() }).collect();
            let page = StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name.clone(), model: key.model.clone(), page: 1, replace_all: false, cursor: None, roots, related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
            let batch = StoreBatch { author: BatchAuthor::Window, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), rows: Vec::new(), intents: 0, credential: None, ops: vec![page] };
            db.write_batch(&batch, db.files(), None).unwrap();
        }
        let ids: Vec<RowId> = asked.iter().map(|name| RowId::new(name).unwrap()).collect();
        let got: Vec<String> = db.nodes_answered(&key.model, &key.name, &ids).unwrap().iter().map(|id| id.as_str().to_string()).collect();
        let last = answers[*order.last().expect("an order")];
        let expected: Vec<&str> = asked.iter().copied().filter(|id| last.contains(id)).collect();
        assert_eq!(got, expected, "{order:?}");
        for id in asked {
            let row = db.read(&["moments"], |reader| reader.point("moments", id)).unwrap().expect("the cached row stays");
            let served = order.iter().any(|index| answers[*index].contains(&id));
            let want = if served { format!("served {id}") } else { format!("cached {id}") };
            assert_eq!(row.get("text"), Some(&serde_json::json!(want)), "{order:?}: {id}");
        }
    }
}

/// P-WIN-28: one return of the network gives one read of a watched window in either order of `Online(true)` and `Stale{Gap}`, with and without `ReloadOnConnect`.
#[test]
fn win_orders_reconnect_gap() {
    use super::{failed_watched, fresh_watched};
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Network { Off, On, Gap }
    use Network::*;
    let all = orders(&[Off, On, Gap], &[(Off, On), (Off, Gap)]);
    assert_eq!(all.len(), 2);
    for (name, make) in [("fresh", fresh_watched as fn() -> Twin), ("failed", failed_watched as fn() -> Twin)] {
        for reload in [false, true] {
            for order in &all {
                let mut twin = make();
                if reload {
                    twin.post_call(w::WindowsInput::ReloadOnConnect { key: key(), id: u64::from(STREAM) });
                }
                take(&twin.log);
                let mut reads = 0;
                for event in order.iter().copied() {
                    twin.post(match event {
                        Off => w::WindowsInput::Online { online: Online(false), online_seq: 0 },
                        On => w::WindowsInput::Online { online: Online(true), online_seq: 1 },
                        Gap => w::WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::all(), cause: w::StaleCause::Gap { online_seq: 1 } },
                    });
                    reads += requests(&twin.split(take(&twin.log)).0);
                }
                assert_eq!(reads, 1, "{name} reload={reload} {order:?}");
            }
        }
    }
}

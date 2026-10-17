use serde_json::json;

use super::super::rig::*;
use super::rig::*;
use crate::db::fixtures::changes;
use crate::transport::Reply;

#[tokio::test(start_paused = true)]
async fn confirming_a_created_row_keeps_it_after_the_rows_shown_before() {
    let rig = chat_rig(true);
    load_page(&rig, vec![message("m1", Some(1), 1), message("m2", Some(2), 2), message("m5", Some(5), 5)], false, "c").await;
    let (log, _window) = watch_thread(&rig, 50);
    let local = rig.db.create("messages", params(json!({"chatId": CHAT, "text": "mine", "createdAt": "2026-01-01T00:00:09.000Z"}))).unwrap();
    rig.db.settle();
    assert_eq!(shown(&log), ["m1", "m2", "m5", local.id.as_str()]);

    let seen = changes(&log).len();
    rig.transport.enqueue("CreateMessage", Reply::data(json!({"createMessage": message("m9", Some(3), 9)})));
    rig.sync.flush().await.unwrap();
    rig.db.settle();

    assert_eq!(shown(&log), ["m1", "m2", "m5", "m9"]);
    assert_eq!(moves_since(&log, seen), Vec::<String>::new());
    assert_eq!(changes(&log).iter().skip(seen).map(|change| change.diff.replaced.len()).sum::<usize>(), 1, "the swap is a replace, not an insert and a remove");
    let whole: Vec<String> = rig.db.query(&thread_query(&rig)).unwrap().iter().map(|row| row.id.to_string()).collect();
    assert_eq!(whole, ["m1", "m2", "m5", "m9"], "a list read of the same relation keeps the same order");
}

#[tokio::test(start_paused = true)]
async fn a_row_delivered_by_an_event_before_its_create_is_confirmed_keeps_the_place_of_the_created_row() {
    let rig = chat_rig(true);
    load_page(&rig, vec![message("m1", Some(1), 1), message("m2", Some(2), 2), message("m5", Some(5), 5)], false, "c").await;
    let _subscription = subscribe_to_events(&rig).await;
    let (log, _window) = watch_thread(&rig, 50);
    rig.db.create("messages", params(json!({"chatId": CHAT, "text": "mine", "createdAt": "2026-01-01T00:00:09.000Z"}))).unwrap();
    event(&rig, message("m9", Some(3), 9)).await;
    assert_eq!(shown(&log).len(), 5, "the event row and the created row are both listed until the answer swaps them");

    rig.transport.enqueue("CreateMessage", Reply::data(json!({"createMessage": message("m9", Some(3), 9)})));
    rig.sync.flush().await.unwrap();
    rig.db.settle();
    assert_eq!(shown(&log), ["m1", "m2", "m5", "m9"]);

    event(&rig, message("m10", Some(4), 10)).await;
    assert_eq!(shown(&log), ["m1", "m2", "m5", "m9", "m10"], "the confirmed row is a neighbour by its own sequence number");
}

#[tokio::test(start_paused = true)]
async fn many_inserts_at_one_place_rebalance_keys_rarely_and_only_nearby() {
    let rig = chat_rig(true);
    load_page(&rig, vec![message("a", Some(1), 1), message("z", Some(1_000_000), 2)], false, "c").await;
    let _subscription = subscribe_to_events(&rig).await;
    let (log, _window) = watch_thread(&rig, 500);
    let (mut rounds, mut widest, mut before) = (0, 0, member_keys(&rig));
    for sequence in (999_700..1_000_000).rev() {
        event(&rig, message(&format!("n{sequence}"), Some(sequence), 3)).await;
        let after = member_keys(&rig);
        let changed = before.iter().filter(|(id, key)| after.iter().any(|(other, kept)| other == id && kept != key)).count();
        rounds += usize::from(changed > 0);
        (widest, before) = (widest.max(changed), after);
    }
    let longest = before.iter().map(|(_, key)| key.len()).max().unwrap();
    assert!((1..=30).contains(&rounds), "{rounds} rounds of rebalancing for 300 inserts");
    assert!(widest <= 16, "a rebalance rewrote {widest} keys");
    assert!(longest <= crate::value::ORDER_KEY_LIMIT + 8, "the longest key has {longest} characters");
    let expected: Vec<String> = ["a".to_string()].into_iter().chain((999_700..1_000_000).map(|sequence| format!("n{sequence}"))).chain(["z".to_string()]).collect();
    assert_eq!(shown(&log), expected);
}

#[tokio::test(start_paused = true)]
async fn a_shown_row_keeps_its_place_when_an_event_or_a_page_changes_its_sequence_number() {
    let rig = chat_rig(true);
    enqueue_page(&rig, vec![message("m1", Some(1), 1), message("m2", Some(2), 2), message("m3", Some(3), 3)], false, "c");
    enqueue_page(&rig, vec![message("m1", Some(1), 1), message("x", Some(2), 8), message("m3", Some(3), 3), message("m2", Some(9), 2)], false, "c");
    load_first_page(&rig).await;
    let _subscription = subscribe_to_events(&rig).await;
    event(&rig, message("x", None, 8)).await;
    let (log, _window) = watch_thread(&rig, 50);
    assert_eq!(shown(&log), ["m1", "m2", "m3", "x"]);
    let seen = changes(&log).len();

    event(&rig, message("x", Some(2), 8)).await;
    assert_eq!(shown(&log), ["m1", "m2", "m3", "x"], "null -> number by an event");

    load_first_page(&rig).await;
    assert_eq!(shown(&log), ["m1", "m2", "m3", "x"], "the shown order wins over the order of a reloaded page");
    assert_eq!(moves_since(&log, seen), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn rows_of_a_later_page_and_of_events_take_their_place_by_sequence_number() {
    let rig = chat_rig(true);
    enqueue_page(&rig, vec![message("m10", Some(10), 10), message("m20", Some(20), 20), message("m30", Some(30), 30)], true, "c1");
    enqueue_page(&rig, vec![message("m05", Some(5), 5), message("m15", Some(15), 15), message("m25", Some(25), 25)], false, "c2");
    load_first_page(&rig).await;
    let _subscription = subscribe_to_events(&rig).await;
    let (log, _window) = watch_thread(&rig, 50);

    load_next_page(&rig).await;
    assert_eq!(shown(&log), ["m05", "m10", "m15", "m20", "m25", "m30"]);

    event(&rig, message("m40", Some(40), 40)).await;
    event(&rig, message("m22", Some(22), 22)).await;
    assert_eq!(shown(&log), ["m05", "m10", "m15", "m20", "m22", "m25", "m30", "m40"]);
}

#[tokio::test(start_paused = true)]
async fn a_later_row_is_placed_after_the_member_that_has_the_nearest_lower_current_sequence_number() {
    let rig = chat_rig(true);
    load_page(&rig, vec![message("a", Some(1), 1), message("b", Some(5), 2)], false, "c").await;
    let _subscription = subscribe_to_events(&rig).await;
    let (log, _window) = watch_thread(&rig, 50);

    event(&rig, message("c", Some(3), 3)).await;
    event(&rig, message("c", Some(9), 3)).await;
    assert_eq!(shown(&log), ["a", "c", "b"], "a changed number does not move the row");
    event(&rig, message("d", Some(10), 4)).await;
    assert_eq!(shown(&log), ["a", "c", "d", "b"], "the neighbour of a new row is found by the current number of c, not by the number c had");
}

#[tokio::test(start_paused = true)]
async fn a_created_row_goes_after_the_last_row_even_when_its_clock_is_behind() {
    let rig = chat_ordered_by_time();
    load_page(&rig, vec![message("m1", Some(1), 1), message("m2", Some(2), 5)], false, "c").await;
    let (log, _window) = watch_thread(&rig, 50);
    let local = rig.db.create("messages", params(json!({"chatId": CHAT, "text": "mine", "createdAt": "2026-01-01T00:00:03.000Z"}))).unwrap();
    rig.db.settle();
    assert_eq!(shown(&log), ["m1", "m2", local.id.as_str()]);
}

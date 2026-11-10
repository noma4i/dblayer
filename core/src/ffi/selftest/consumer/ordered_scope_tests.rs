use std::sync::Arc;

use serde_json::{json, Value};

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tape::Rows;
use crate::ffi::{QuerySpec, ReadRequest};
use crate::test_wait::settled;

#[derive(Clone, Copy)]
struct Case {
    model: &'static str,
    scope: &'static str,
    operation: &'static str,
    params: &'static str,
    media: &'static str,
    ascending: bool,
}

fn row(case: Case, number: i32) -> Value {
    let timestamp = format!("2026-10-27T00:00:{number:02}Z");
    if case.model == "Message" {
        let gift = case.media == "GIFT";
        json!({"id": format!("m{number}"), "chatId": "c1", "body": format!("message {number}"), "kind": if gift { "GIFT" } else { "TEXT" }, "status": "SENT", "sequenceNumber": 1 + (number - 1) / 3, "media": {"kind": if gift { "IMAGE" } else { case.media }}, "createdAt": timestamp, "updatedAt": timestamp})
    } else {
        json!({"id": format!("m{number}"), "uuid": format!("u{number}"), "user": {"id": "u1"}, "unreadSimilarMomentsCount": (number - 1) / 3, "createdAt": timestamp, "updatedAt": timestamp})
    }
}

fn answer(case: Case, numbers: &[i32], more: bool, cursor: &str) -> Value {
    let nodes: Vec<_> = numbers.iter().map(|number| row(case, *number)).collect();
    let connection = json!({"nodes": nodes, "pageInfo": {"hasNextPage": more, "endCursor": cursor}});
    match case.operation {
        "chatMedia" => json!({"node": {"messages": connection}}),
        "userMoments" => json!({"user": {"moments": connection}}),
        "myMoments" => json!({"viewer": {"moments": connection}}),
        "meCompass" => json!({"viewer": {"compassMoments": connection}}),
        _ => unreachable!(),
    }
}

fn ids(rows: &Rows) -> Vec<String> { rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect() }

async fn ordered_scope(case: Case) {
    let stand = Stand::prepare().await;
    let first: Vec<_> = if case.ascending { (1..=3).collect() } else { (7..=9).rev().collect() };
    let second: Vec<_> = if case.ascending { (4..=6).collect() } else { (4..=6).rev().collect() };
    let third: Vec<_> = if case.ascending { (7..=9).collect() } else { (1..=3).rev().collect() };
    stand.answer(case.operation, answer(case, &first, true, "c1"));
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    let query = QuerySpec::json(json!({"model":case.model, "relation":case.scope, "params":serde_json::from_str::<Value>(case.params).unwrap()}).to_string());
    let rows = Arc::new(Rows::default());
    let _stream = stand.database.watch(ReadRequest::List { query }, rows.clone());
    settled(&stand, "the first page", || rows.len() == 3).await;
    let after_first = stand.requests(case.operation);
    for (numbers, more, cursor, expected) in [(&second, true, "c2", 6), (&third, false, "c3", 9)] {
        stand.answer(case.operation, answer(case, numbers, more, cursor));
        bound(stand.database.load_more(epoch, case.model.into(), Some(case.scope.into()), case.params.into())).await.unwrap();
        settled(&stand, "the next page", || rows.len() == expected).await;
    }
    let expected: Vec<_> = first.iter().chain(&second).chain(&third).map(|n| format!("m{n}")).collect();
    assert_eq!(ids(&rows), expected, "{}.{} after three pages", case.model, case.scope);
    assert_eq!(stand.requests(case.operation), after_first + 2);
    stand.answer(case.operation, answer(case, &first, true, "c1"));
    bound(stand.database.refresh(epoch, case.model.into(), Some(case.scope.into()), case.params.into())).await.unwrap();
    stand.settle().await;
    let visible = ids(&rows);
    assert_eq!(visible, expected, "{}.{} after refresh: {} rows", case.model, case.scope, visible.len());
    let kept = [first[0], first[2]];
    stand.answer(case.operation, answer(case, &kept, true, "c1"));
    bound(stand.database.refresh(epoch, case.model.into(), Some(case.scope.into()), case.params.into())).await.unwrap();
    stand.settle().await;
    let without: Vec<_> = expected.iter().filter(|id| **id != format!("m{}", first[1])).cloned().collect();
    assert_eq!(ids(&rows), without, "{}.{} after deletion", case.model, case.scope);
    assert_eq!(stand.requests(case.operation), after_first + 4);
    stand.database.close_now();
}

macro_rules! ordered_case {
    ($name:ident, $model:literal, $scope:literal, $op:literal, $params:literal, $media:literal, $asc:literal) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            ordered_scope(Case { model: $model, scope: $scope, operation: $op, params: $params, media: $media, ascending: $asc }).await;
        }
    };
}

ordered_case!(consumer_ordered_visual_media, "Message", "visual_media", "chatMedia", r#"{"chatId":"c1"}"#, "IMAGE", false);
ordered_case!(consumer_ordered_audio_media, "Message", "audio_media", "chatMedia", r#"{"chatId":"c1"}"#, "AUDIO", false);
ordered_case!(consumer_ordered_gift_media, "Message", "gift_media", "chatMedia", r#"{"chatId":"c1"}"#, "GIFT", false);
ordered_case!(consumer_ordered_visual_since, "Message", "visual_since", "chatMedia", r#"{"chatId":"c1","afterSequence":0}"#, "IMAGE", true);
ordered_case!(consumer_ordered_audio_since, "Message", "audio_since", "chatMedia", r#"{"chatId":"c1","afterSequence":0}"#, "AUDIO", true);
ordered_case!(consumer_ordered_gifts_since, "Message", "gifts_since", "chatMedia", r#"{"chatId":"c1","afterSequence":0}"#, "GIFT", true);

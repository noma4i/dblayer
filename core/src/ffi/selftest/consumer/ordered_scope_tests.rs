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
        json!({"id": format!("m{number}"), "chatId": "c1", "body": format!("message {number}"), "kind": if gift { "GIFT" } else { "TEXT" }, "status": "SENT", "oneTime": false, "expired": false, "isSystem": false, "isForwarded": false, "isReply": false, "unread": false, "sequenceNumber": 1 + (number - 1) / 3, "media": {"kind": if gift { "IMAGE" } else { case.media }}, "createdAt": timestamp, "updatedAt": timestamp})
    } else {
        let mut moment = super::stand::moment(&format!("m{number}"), 0);
        moment["uuid"] = json!(format!("u{number}"));
        moment["user"] = json!({"id": "u1", "fullName": "Ann", "username": "ann"});
        moment["unreadSimilarMomentsCount"] = json!((number - 1) / 3);
        moment["createdAt"] = json!(timestamp);
        moment["updatedAt"] = json!(timestamp);
        moment
    }
}

fn answer(case: Case, numbers: &[i32], more: bool, cursor: &str) -> Value {
    let nodes: Vec<_> = numbers.iter().map(|number| row(case, *number)).collect();
    let edges: Vec<_> = nodes.into_iter().enumerate().map(|(index, node)| if matches!(case.operation, "userMoments" | "myMoments") { json!({"cursor":format!("c{index}"), "node":node}) } else { json!({"node":node}) }).collect();
    let mut connection = json!({"edges": edges, "pageInfo": {"hasNextPage": more, "endCursor": cursor}});
    if case.operation == "chatMedia" { connection["totalCount"] = json!(numbers.len()); }
    match case.operation {
        "chatMedia" => json!({"node": {"messages": connection}}),
        "userMoments" => json!({"user": {"moments": connection}}),
        "myMoments" => json!({"viewer": {"moments": connection}}),
        "meCompass" => json!({"viewer": {"compassMoments": connection}}),
        _ => unreachable!(),
    }
}

fn ids(rows: &Rows) -> Vec<String> { rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect() }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_chat_media_load_all_keeps_every_page() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    let params = r#"{"chatId":"c1"}"#;
    let cases = [
        Case { model: "Message", scope: "visual_media", operation: "chatMedia", params, media: "IMAGE", ascending: false },
        Case { model: "Message", scope: "audio_media", operation: "chatMedia", params, media: "AUDIO", ascending: false },
        Case { model: "Message", scope: "gift_media", operation: "chatMedia", params, media: "GIFT", ascending: false },
    ];
    for pass in 0..2 {
        for (index, case) in cases.into_iter().enumerate() {
            let messages: Vec<_> = (0..2).map(|page| {
                let mut message = row(case, (index * 2 + page + 1) as i32);
                if case.media == "GIFT" { message["media"] = Value::Null; }
                message["createdAt"] = json!("2026-10-20T00:00:00Z");
                message["updatedAt"] = json!("2026-10-20T00:00:00Z");
                json!({"node":message})
            }).collect();
            stand.answer("chatMediaState", media_marker(&(pass + 1).to_string(), case.scope, 2, 2));
            if pass == 0 {
                stand.answer_next("chatMedia", json!({"data":{"node":{"messages":{"totalCount":2,"edges":[messages[0]],"pageInfo":{"hasNextPage":true,"endCursor":"mid"}}}}}));
                stand.answer_next("chatMedia", json!({"data":{"node":{"messages":{"totalCount":2,"edges":[messages[1]],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}));
            } else {
                stand.answer_next("chatMedia", json!({"data":{"node":{"messages":{"totalCount":2,"edges":messages,"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}));
            }
            bound(stand.database.load_all(epoch, "Message".into(), Some(case.scope.into()), params.into(), true)).await.unwrap();
            let query = QuerySpec::json(json!({"model":"Message","relation":case.scope,"params":{"chatId":"c1"}}).to_string());
            let mut found: Vec<_> = bound(stand.database.read_query(epoch, query)).await.unwrap().into_iter().map(|row| row.id).collect();
            found.sort();
            assert_eq!(found, vec![format!("m{}", index * 2 + 1), format!("m{}", index * 2 + 2)], "{} pass {pass}", case.scope);
        }
        let requests: Vec<_> = stand.asked().into_iter().filter(|(name, _)| name == "chatMedia").collect();
        assert_eq!(requests.len(), 6 + pass * 3);
        assert_eq!(requests.iter().filter(|(_, variables)| variables.get("after").is_some_and(|value| !value.is_null())).count(), 3);
    }
    stand.database.close_now();
}

fn media_marker(revision: &str, scope: &str, count: i64, latest: i64) -> Value {
    let counts = |name: &str| if scope.starts_with(name) { count } else { 0 };
    json!({"node": {"mediaLibrary": {"revision": revision, "visualCount": counts("visual"), "audioCount": counts("audio"), "giftCount": counts("gift"), "latestSequence": latest}}})
}

async fn ordered_marker_scope(case: Case) {
    let stand = Stand::prepare().await;
    stand.answer("chatMediaState", media_marker("1", case.scope, 9, 9));
    for (numbers, more, cursor) in [((7..=9).rev().collect::<Vec<_>>(), true, "c1"), ((4..=6).rev().collect(), true, "c2"), ((1..=3).rev().collect(), false, "c3")] {
        let mut page = answer(case, &numbers, more, cursor);
        page["node"]["messages"]["totalCount"] = json!(9);
        stand.answer_next("chatMedia", json!({"data": page}));
    }
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    let query = QuerySpec::json(json!({"model":case.model, "relation":case.scope, "params":serde_json::from_str::<Value>(case.params).unwrap()}).to_string());
    let rows = Arc::new(Rows::default());
    let _stream = stand.database.watch(ReadRequest::List { query }, rows.clone());
    let expected: Vec<_> = (1..=9).rev().map(|n| format!("m{n}")).collect();
    settled(&stand, "the whole chain", || rows.len() == 9).await;
    assert_eq!(ids(&rows), expected, "{}.{} after the chain", case.model, case.scope);
    assert_eq!(stand.requests("chatMedia"), 3);
    bound(stand.database.refresh(epoch, case.model.into(), Some(case.scope.into()), case.params.into())).await.unwrap();
    stand.settle().await;
    assert_eq!(ids(&rows), expected, "{}.{} after a refresh with the same marker", case.model, case.scope);
    assert_eq!(stand.requests("chatMedia"), 3);
    stand.answer("chatMediaState", media_marker("2", case.scope, 2, 9));
    let mut page = answer(case, &[9, 7], false, "c1");
    page["node"]["messages"]["totalCount"] = json!(2);
    stand.answer_next("chatMedia", json!({"data": page}));
    bound(stand.database.refresh(epoch, case.model.into(), Some(case.scope.into()), case.params.into())).await.unwrap();
    settled(&stand, "the chain after a marker change", || rows.len() == 2).await;
    assert_eq!(ids(&rows), ["m9", "m7"], "{}.{} after deletion", case.model, case.scope);
    stand.database.close_now();
}

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

async fn ordered_limit(case: Case) {
    for more_on_last in [false, true] {
        let stand = Stand::prepare().await;
        stand.answer(case.operation, answer(case, &[20], true, "c1"));
        stand.sign_in().await;
        let epoch = stand.database.session_epoch();
        let query = QuerySpec::json(json!({"model":case.model, "relation":case.scope, "params":serde_json::from_str::<Value>(case.params).unwrap()}).to_string());
        let rows = Arc::new(Rows::default());
        let _stream = stand.database.watch(ReadRequest::List { query }, rows.clone());
        settled(&stand, "the first page", || rows.len() == 1).await;
        let first_requests = stand.requests(case.operation);
        for page in 2..=20 {
            let number = 21 - page;
            let more = page < 20 || more_on_last;
            stand.answer(case.operation, answer(case, &[number], more, &format!("c{page}")));
            bound(stand.database.load_more(epoch, case.model.into(), Some(case.scope.into()), case.params.into())).await.unwrap();
            settled(&stand, "the page landed", || rows.len() == page as usize).await;
        }
        assert_eq!(stand.requests(case.operation), first_requests + 19);
        let result = bound(stand.database.load_more(epoch, case.model.into(), Some(case.scope.into()), case.params.into())).await;
        if more_on_last {
            assert!(matches!(result, Err(crate::ffi::DbError::Invalid { ref detail }) if detail == "the window reached its page limit"), "{}.{}: {result:?}", case.model, case.scope);
        } else {
            assert!(matches!(result, Ok(crate::ffi::LoadMoreResult::NoMore)), "{}.{}: {result:?}", case.model, case.scope);
        }
        stand.settle().await;
        assert_eq!(stand.requests(case.operation), first_requests + 19);
        assert_eq!(rows.len(), 20);
        stand.database.close_now();
    }
}

macro_rules! ordered_case {
    ($name:ident, $model:literal, $scope:literal, $op:literal, $params:literal, $media:literal, $asc:literal) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            let case = Case { model: $model, scope: $scope, operation: $op, params: $params, media: $media, ascending: $asc };
            if $model == "Message" { ordered_marker_scope(case).await } else { ordered_scope(case).await }
        }
    };
}

ordered_case!(consumer_ordered_visual_media, "Message", "visual_media", "chatMedia", r#"{"chatId":"c1"}"#, "IMAGE", false);
ordered_case!(consumer_ordered_audio_media, "Message", "audio_media", "chatMedia", r#"{"chatId":"c1"}"#, "AUDIO", false);
ordered_case!(consumer_ordered_gift_media, "Message", "gift_media", "chatMedia", r#"{"chatId":"c1"}"#, "GIFT", false);
ordered_case!(consumer_ordered_moment_by_user, "Moment", "by_user", "userMoments", r#"{"userId":"u1"}"#, "", false);
ordered_case!(consumer_ordered_moment_owned, "Moment", "owned", "myMoments", r#"{}"#, "", false);
ordered_case!(consumer_ordered_moment_compass, "Moment", "compass", "meCompass", r#"{}"#, "", false);

#[tokio::test(flavor = "multi_thread")]
async fn consumer_ordered_by_user_page_limit() {
    ordered_limit(Case { model: "Moment", scope: "by_user", operation: "userMoments", params: r#"{"userId":"u1"}"#, media: "", ascending: false }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_ordered_owned_page_limit() {
    ordered_limit(Case { model: "Moment", scope: "owned", operation: "myMoments", params: r#"{}"#, media: "", ascending: false }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_ordered_compass_page_limit() {
    ordered_limit(Case { model: "Moment", scope: "compass", operation: "meCompass", params: r#"{}"#, media: "", ascending: false }).await;
}

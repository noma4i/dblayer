use super::load::LoadOptions;
use super::rig::*;
use super::{locked, SyncConfig};
use crate::db::Db;
use crate::store::WindowRecord;
use crate::transport::{MockTransport, Reply};
use serde_json::{json, Map, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn none() -> Map<String, Value> {
    Map::new()
}

fn specs_with_pages(max_pages: Option<u32>) -> Vec<crate::schema::ModelSpec> {
    let mut models = specs();
    let model = models.iter_mut().find(|model| model.key.as_str() == "moments").unwrap();
    let feed = model.relations.iter_mut().find(|relation| relation.name == "feed").unwrap();
    if let crate::schema::RemoteShape::Connection { max_pages: limit, .. } = &mut feed.remote.as_mut().unwrap().shape {
        *limit = max_pages;
    }
    models
}

enum Run {
    Once,
    ThenAgain,
    ThenForced,
    TwoCallers,
    CancelledOnPage(Vec<Reply>),
    ResetWhileTheFirstPageIsInFlight,
    NewerWindowStored { cursor: Option<&'static str>, has_next: bool },
}

struct Case {
    name: &'static str,
    max_pages: Option<u32>,
    replies: Vec<Reply>,
    run: Run,
    relation: &'static str,
    want: Result<(), &'static str>,
    calls: usize,
    members: Option<&'static [&'static str]>,
}

fn pages_to_the_end() -> Vec<Reply> {
    vec![feed_reply(&["a", "b"], true, "c2"), feed_reply(&["c", "d"], true, "c4"), feed_reply(&["e"], false, "c5")]
}

fn flight_count(rig: &Rig) -> usize {
    locked(&rig.sync.inner.flights).waiting.len()
}

fn list_reply() -> Reply {
    Reply::data(json!({"moments": []}))
}

#[tokio::test(start_paused = true)]
async fn load_all_reads_the_connection_to_the_confirmed_end() {
    let cases = vec![
        Case { name: "pages to the end", max_pages: None, replies: pages_to_the_end(), run: Run::Once, relation: "feed", want: Ok(()), calls: 3, members: Some(&["a", "b", "c", "d", "e"]) },
        Case { name: "complete and fresh makes no request", max_pages: None, replies: pages_to_the_end(), run: Run::ThenAgain, relation: "feed", want: Ok(()), calls: 3, members: Some(&["a", "b", "c", "d", "e"]) },
        Case { name: "force refreshes the first page and keeps the loaded tail", max_pages: None, replies: vec![feed_reply(&["a", "b"], true, "c2"), feed_reply(&["c", "d"], false, "c4"), feed_reply(&["a", "b"], true, "r2")], run: Run::ThenForced, relation: "feed", want: Ok(()), calls: 3, members: Some(&["a", "b", "c", "d"]) },
        Case { name: "transport failure is returned and the rows stay", max_pages: None, replies: vec![feed_reply(&["a", "b"], true, "c2"), Reply::graphql_messages(&["boom"])], run: Run::Once, relation: "feed", want: Err("graphql"), calls: 2, members: Some(&["a", "b"]) },
        Case { name: "max pages with pages left", max_pages: Some(2), replies: pages_to_the_end(), run: Run::Once, relation: "feed", want: Err("max_pages"), calls: 2, members: Some(&["a", "b", "c", "d"]) },
        Case { name: "same cursor again", max_pages: None, replies: vec![feed_reply(&["a", "b"], true, "c2"), feed_reply(&["c", "d"], true, "c2")], run: Run::Once, relation: "feed", want: Err("did not advance"), calls: 2, members: Some(&["a", "b", "c", "d"]) },
        Case { name: "cursor cycle", max_pages: None, replies: vec![feed_reply(&["a"], true, "c1"), feed_reply(&["b"], true, "c2"), feed_reply(&["c"], true, "c1")], run: Run::Once, relation: "feed", want: Err("did not advance"), calls: 3, members: Some(&["a", "b", "c"]) },
        Case { name: "stored next page without cursor", max_pages: None, replies: vec![feed_reply(&["a"], false, "c1")], run: Run::NewerWindowStored { cursor: None, has_next: true }, relation: "feed", want: Err("without a cursor"), calls: 1, members: Some(&[]) },
        Case { name: "concurrent callers share the pages", max_pages: None, replies: pages_to_the_end().into_iter().map(|reply| reply.after(Duration::from_millis(10))).collect(), run: Run::TwoCallers, relation: "feed", want: Ok(()), calls: 3, members: Some(&["a", "b", "c", "d", "e"]) },
        Case { name: "cancellation leaves no flight", max_pages: None, replies: vec![feed_reply(&["a", "b"], true, "c2")], run: Run::CancelledOnPage(vec![Reply::graphql_messages(&["slow"]).after(Duration::from_secs(60)), feed_reply(&["c", "d"], false, "c4")]), relation: "feed", want: Ok(()), calls: 3, members: Some(&["a", "b", "c", "d"]) },
        Case { name: "a query without a remote relation", max_pages: None, replies: vec![], run: Run::Once, relation: "recent", want: Err("has no remote"), calls: 0, members: Some(&[]) },
        Case { name: "reset while the first connection page is in flight", max_pages: None, replies: vec![feed_reply(&["a"], false, "c1").after(Duration::from_secs(10))], run: Run::ResetWhileTheFirstPageIsInFlight, relation: "feed", want: Err("reset"), calls: 1, members: None },
        Case { name: "reset while the first list response is in flight", max_pages: None, replies: vec![list_reply().after(Duration::from_secs(10))], run: Run::ResetWhileTheFirstPageIsInFlight, relation: "by_user", want: Err("reset"), calls: 1, members: None },
        Case { name: "a newer complete window answers a discarded first page", max_pages: None, replies: vec![feed_reply(&["a"], false, "c1")], run: Run::NewerWindowStored { cursor: None, has_next: false }, relation: "feed", want: Ok(()), calls: 1, members: Some(&[]) },
        Case { name: "a newer open window is read on from its cursor", max_pages: None, replies: vec![feed_reply(&["a"], false, "c1"), feed_reply(&["c", "d"], false, "c4")], run: Run::NewerWindowStored { cursor: Some("zz"), has_next: true }, relation: "feed", want: Ok(()), calls: 2, members: Some(&["c", "d"]) },
        Case { name: "a newer list window answers a discarded first response", max_pages: None, replies: vec![list_reply()], run: Run::NewerWindowStored { cursor: None, has_next: false }, relation: "by_user", want: Ok(()), calls: 1, members: Some(&[]) },
    ];
    for case in cases {
        let rig = rig_on(Db::open_in_memory(specs_with_pages(case.max_pages), None).unwrap(), MockTransport::new(), SyncConfig::default());
        let operation = if case.relation == "by_user" { "MomentsByUser" } else { "MomentsFeed" };
        let query_params = if case.relation == "by_user" { params(json!({"userId": "u1"})) } else { none() };
        for reply in case.replies {
            rig.transport.enqueue(operation, reply);
        }
        let budget = case.calls;
        let seen = Arc::new(AtomicUsize::new(0));
        rig.transport.respond_with(move |_| (seen.fetch_add(1, Ordering::SeqCst) >= budget).then(|| Reply::graphql_messages(&["request budget exhausted"])));
        let load = |force: bool| rig.sync.load_all("moments", case.relation, &query_params, LoadOptions { force });
        let outcome = match case.run {
            Run::Once => load(false).await,
            Run::ThenAgain => {
                load(false).await.unwrap();
                load(false).await
            }
            Run::ThenForced => {
                load(false).await.unwrap();
                let outcome = load(true).await;
                assert_eq!(rig.transport.calls()[2].variables, json!({"first": 2}), "{}: the forced call asks for the first page", case.name);
                outcome
            }
            Run::TwoCallers => {
                let (first, second) = tokio::join!(load(false), load(false));
                assert_eq!(first, second, "{}: both callers see the same outcome", case.name);
                first
            }
            Run::CancelledOnPage(replies) => {
                for reply in replies {
                    rig.transport.enqueue(operation, reply);
                }
                assert!(tokio::time::timeout(Duration::from_secs(5), load(false)).await.is_err(), "{}: the slow page ends the wait", case.name);
                assert_eq!(flight_count(&rig), 0, "{}: the cancelled call left a flight", case.name);
                load(false).await
            }
            Run::ResetWhileTheFirstPageIsInFlight => tokio::join!(load(false), async { rig.db.reset(&["moments"]).unwrap() }).0,
            Run::NewerWindowStored { cursor, has_next } => {
                let key = rig.sync.window_key("moments", case.relation, &query_params).unwrap();
                let newer = i64::MAX / 2;
                let record = WindowRecord { key, cursor: cursor.map(str::to_string), has_next, total: None, pages: 1, issued: newer, applied: newer, generation: i64::try_from(rig.db.generation().value()).unwrap() };
                rig.db.shared.store.save_window(&record).unwrap();
                load(true).await
            }
        };
        match (&outcome, case.want) {
            (Ok(()), Ok(())) => {}
            (Err(error), Err(text)) => assert!(error.to_string().contains(text), "{}: {error}", case.name),
            _ => panic!("{}: {outcome:?} against {:?}", case.name, case.want),
        }
        assert_eq!(rig.transport.call_count(operation), case.calls, "{}", case.name);
        if let Some(members) = case.members {
            assert_eq!(member_ids(&rig, "moments", case.relation, &query_params), members, "{}", case.name);
        }
        assert_eq!(flight_count(&rig), 0, "{}: a flight is left", case.name);
    }
}

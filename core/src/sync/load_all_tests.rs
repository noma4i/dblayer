use super::load::LoadOptions;
use super::rig::*;
use super::{locked, SyncConfig};
use crate::db::Db;
use crate::transport::{MockTransport, Reply};
use serde_json::Map;
use std::time::Duration;

fn none() -> Map<String, serde_json::Value> {
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
    ThenForced(Vec<Reply>),
    TwoCallers,
    CancelledOnPage(Vec<Reply>),
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

#[tokio::test(start_paused = true)]
async fn load_all_reads_the_connection_to_the_confirmed_end() {
    let cases = vec![
        Case { name: "pages to the end", max_pages: None, replies: pages_to_the_end(), run: Run::Once, relation: "feed", want: Ok(()), calls: 3, members: Some(&["a", "b", "c", "d", "e"]) },
        Case { name: "complete and fresh makes no request", max_pages: None, replies: pages_to_the_end(), run: Run::ThenAgain, relation: "feed", want: Ok(()), calls: 3, members: Some(&["a", "b", "c", "d", "e"]) },
        Case { name: "force refreshes the start and keeps the pages behind it", max_pages: None, replies: pages_to_the_end(), run: Run::ThenForced(vec![feed_reply(&["x", "y"], true, "n2")]), relation: "feed", want: Ok(()), calls: 4, members: None },
        Case { name: "transport failure is returned and the rows stay", max_pages: None, replies: vec![feed_reply(&["a", "b"], true, "c2"), Reply::graphql_messages(&["boom"])], run: Run::Once, relation: "feed", want: Err("graphql"), calls: 2, members: Some(&["a", "b"]) },
        Case { name: "max pages with pages left", max_pages: Some(2), replies: pages_to_the_end(), run: Run::Once, relation: "feed", want: Err("max_pages"), calls: 2, members: Some(&["a", "b", "c", "d"]) },
        Case { name: "same cursor again", max_pages: None, replies: vec![feed_reply(&["a", "b"], true, "c2"), feed_reply(&["c", "d"], true, "c2")], run: Run::Once, relation: "feed", want: Err("did not advance"), calls: 2, members: Some(&["a", "b", "c", "d"]) },
        Case { name: "concurrent callers share the pages", max_pages: None, replies: pages_to_the_end().into_iter().map(|reply| reply.after(Duration::from_millis(10))).collect(), run: Run::TwoCallers, relation: "feed", want: Ok(()), calls: 3, members: Some(&["a", "b", "c", "d", "e"]) },
        Case { name: "cancellation leaves no flight", max_pages: None, replies: vec![feed_reply(&["a", "b"], true, "c2")], run: Run::CancelledOnPage(vec![Reply::graphql_messages(&["slow"]).after(Duration::from_secs(60)), feed_reply(&["c", "d"], false, "c4")]), relation: "feed", want: Ok(()), calls: 3, members: Some(&["a", "b", "c", "d"]) },
        Case { name: "a query without a remote relation", max_pages: None, replies: vec![], run: Run::Once, relation: "recent", want: Err("has no remote"), calls: 0, members: Some(&[]) },
    ];
    for case in cases {
        let rig = rig_on(Db::open_in_memory(specs_with_pages(case.max_pages), None).unwrap(), MockTransport::new(), SyncConfig::default());
        for reply in case.replies {
            rig.transport.enqueue("MomentsFeed", reply);
        }
        let params = none();
        let load = |force: bool| rig.sync.load_all("moments", case.relation, &params, LoadOptions { force });
        let outcome = match case.run {
            Run::Once => load(false).await,
            Run::ThenAgain => {
                load(false).await.unwrap();
                load(false).await
            }
            Run::ThenForced(replies) => {
                load(false).await.unwrap();
                for reply in replies {
                    rig.transport.enqueue("MomentsFeed", reply);
                }
                let outcome = load(true).await;
                assert_eq!(rig.transport.calls()[3].variables, serde_json::json!({"first": 2}), "{}: the forced call asks for the first page", case.name);
                outcome
            }
            Run::TwoCallers => {
                let (first, second) = tokio::join!(load(false), load(false));
                assert_eq!(first, second, "{}: both callers see the same outcome", case.name);
                first
            }
            Run::CancelledOnPage(replies) => {
                for reply in replies {
                    rig.transport.enqueue("MomentsFeed", reply);
                }
                assert!(tokio::time::timeout(Duration::from_secs(5), load(false)).await.is_err(), "{}: the slow page ends the wait", case.name);
                assert_eq!(flight_count(&rig), 0, "{}: the cancelled call left a flight", case.name);
                load(false).await
            }
        };
        match (&outcome, case.want) {
            (Ok(()), Ok(())) => {}
            (Err(error), Err(text)) => assert!(error.to_string().contains(text), "{}: {error}", case.name),
            _ => panic!("{}: {outcome:?} against {:?}", case.name, case.want),
        }
        assert_eq!(rig.transport.call_count("MomentsFeed"), case.calls, "{}", case.name);
        if let Some(members) = case.members {
            assert_eq!(member_ids(&rig, "moments", "feed", &none()), members, "{}", case.name);
        }
        assert_eq!(flight_count(&rig), 0, "{}: a flight is left", case.name);
    }
}

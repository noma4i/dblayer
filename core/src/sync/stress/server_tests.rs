use serde_json::{json, Value};

use crate::transport::{GraphQLRequest, MockTransport, Transport};

use super::findings::Findings;
use super::server::FakeServer;

fn calm_server() -> (MockTransport, FakeServer, Findings) {
    let transport = MockTransport::new();
    let findings = Findings::default();
    let server = FakeServer::install(&transport, 1, findings.clone());
    server.set_calm(true);
    (transport, server, findings)
}

async fn mutate(transport: &MockTransport, name: &str, variables: Value) {
    transport.mutate(&crate::transport::test_admission(), GraphQLRequest::new("mutation", name, variables)).await.unwrap();
}

#[tokio::test]
async fn a_confirmed_create_that_arrives_again_turns_red() {
    let (transport, _, findings) = calm_server();
    mutate(&transport, "CreateMoment", json!({"text": "w0:1"})).await;
    assert_eq!(findings.all(), Vec::<String>::new());
    mutate(&transport, "CreateMoment", json!({"text": "w0:1"})).await;
    assert!(findings.all().iter().any(|finding| finding.starts_with("[send-once]")));
}

#[tokio::test]
async fn updates_of_one_row_that_arrive_out_of_order_turn_red() {
    let (transport, server, findings) = calm_server();
    server.seed_rows(1, 1, 0);
    mutate(&transport, "UpdateMoment", json!({"id": "s1", "text": "w0:2"})).await;
    mutate(&transport, "UpdateMoment", json!({"id": "s1", "text": "w0:3"})).await;
    assert_eq!(findings.all(), Vec::<String>::new());
    mutate(&transport, "UpdateMoment", json!({"id": "s1", "text": "w0:1"})).await;
    assert!(findings.all().iter().any(|finding| finding.starts_with("[write-order]")));
}

#[tokio::test]
async fn the_server_applies_creates_updates_and_deletes_in_the_order_it_receives_them() {
    let (transport, server, _) = calm_server();
    mutate(&transport, "CreateMoment", json!({"text": "w0:1"})).await;
    mutate(&transport, "UpdateMoment", json!({"id": "s1", "text": "w0:2"})).await;
    assert_eq!(server.state()["s1"], ("w0:2".to_string(), false));
    mutate(&transport, "DeleteMoment", json!({"id": "s1"})).await;
    assert!(server.state().is_empty());
}

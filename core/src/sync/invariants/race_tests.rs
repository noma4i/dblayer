use std::sync::{Arc, Barrier};

use serde_json::json;

use super::super::rig::*;
use super::fixtures::*;
use super::spec_fixtures::mentions;

#[tokio::test(start_paused = true)]
async fn inv_id_05_inv_op_18_an_update_queued_while_the_create_is_confirmed_follows_the_real_id_or_fails() {
    for round in 0..400 {
        let rig = rig();
        let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
        let temp = local.id.to_string();
        rig.transport.enqueue("CreateMoment", created("m77", "hello"));
        let start = Arc::new(Barrier::new(2));
        let writer = {
            let (db, id, start) = (rig.db.clone(), local.id.clone(), start.clone());
            std::thread::spawn(move || {
                start.wait();
                (0..100_000).take_while(|turn| db.update("moments", &id, params(json!({"text": format!("e{turn}")}))).is_ok()).count()
            })
        };
        start.wait();
        rig.sync.flush().await.unwrap();
        let queued = writer.join().unwrap();
        assert_eq!(mentions(&rig.db, &temp), Vec::<String>::new(), "round {round}, {queued} updates queued");
        let sent_with_temp = rig.transport.calls().iter().filter(|call| call.variables.to_string().contains("temp-")).count();
        assert_eq!(sent_with_temp, 0, "round {round}: calls carried the temp id");
        assert!(rig.db.outbox().unwrap().iter().all(|entry| entry.operation.row_ids.iter().all(|id| id.as_str() == "m77")), "round {round}");
    }
}

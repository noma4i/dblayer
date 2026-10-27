use serde_json::json;

use super::fixtures::*;
use super::prior::Prior;
use super::swap::{retarget, Swap};
use crate::commit::OperationTransition;

#[test]
fn inv_rollback_07_a_temp_id_swap_rewrites_the_prior_of_a_queued_update_to_the_real_id() {
    let db = booted();
    let temp = db.create("moments", obj(json!({"text": "first"}))).unwrap();
    db.update("moments", &temp.id, obj(json!({"text": "second"}))).unwrap();
    let update = db.outbox().unwrap().remove(1);
    let swap = Swap { model: "moments", temp: &temp.id, real: &rid("m77"), references: &[] };
    let Some(OperationTransition::Begin(record)) = retarget(&update, &swap).unwrap() else { panic!("the update was not retargeted") };
    let prior: Prior = serde_json::from_value(record.rollback.unwrap()).unwrap();
    assert_eq!(prior.rows.iter().map(|row| row.id()).collect::<Vec<_>>(), ["m77"]);
}

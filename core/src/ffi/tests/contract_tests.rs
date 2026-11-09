use crate::ffi::contract::{ContractAdmission, ServerContract};
use serde_json::{json, Value};

/// The loop every platform double runs: ask the contract, take a scripted token when the request is fresh, settle it.
fn answer_of(contract: &ServerContract, scope: &str, request_id: u64, operation: &str, variables: &Value) -> String {
    let request = json!({ "requestId": request_id.to_string(), "sequence": null, "body": { "operationName": operation, "variables": variables } }).to_string();
    match contract.admit(scope.into(), request.clone()).unwrap() {
        ContractAdmission::Stored { token } => format!("data:{token}"),
        ContractAdmission::Refused { .. } | ContractAdmission::TwoBodies => "refused".into(),
        ContractAdmission::Fresh => match contract.take(operation.into()) {
            Some(token) => {
                contract.settle(scope.into(), request, token.clone()).unwrap();
                format!("data:{token}")
            }
            None => "refused".into(),
        },
    }
}

#[test]
fn server_contract_object_pairs_table() {
    let cases: Vec<Value> = serde_json::from_str(&std::fs::read_to_string("../fixtures/mock-contract/pairs.json").unwrap()).unwrap();
    for case in cases {
        let contract = ServerContract::new();
        for (operation, replies) in case["replies"].as_object().unwrap() {
            for reply in replies.as_array().unwrap() { contract.script(operation.clone(), reply.to_string()); }
        }
        let seen: Vec<String> = case["calls"].as_array().unwrap().iter().map(|call| answer_of(&contract, call[0].as_str().unwrap(), call[1].as_u64().unwrap(), call[2].as_str().unwrap(), &call[3])).collect();
        assert_eq!(json!(seen), case["expect"], "{}", case["name"]);
    }
}

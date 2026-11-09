//! The one server contract that every test double of the server follows.

// TEMPORARY: stub until the contract is implemented
#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};

use serde_json::Value;

#[cfg(test)]
mod tests;

pub(crate) struct Request {
    pub scope: String,
    pub request_id: String,
    pub sequence: Option<i64>,
    pub operation: String,
    pub body: String,
    pub hash: Option<String>,
    pub document: bool,
}

pub(crate) enum Admission<A> {
    Stored(A),
    Refused(Value),
    TwoBodies,
    Fresh,
}

pub(crate) struct Contract<A> {
    known: HashSet<String>,
    last: i64,
    settled: HashMap<(String, String, Option<i64>), (String, A)>,
    script: HashMap<String, VecDeque<A>>,
    standing: Option<A>,
}

impl Request {
    pub(crate) fn from_wire(_scope: &str, _wire: &Value) -> Self { unimplemented!() }
}

impl<A: Clone> Contract<A> {
    pub(crate) fn new() -> Self { unimplemented!() }
    pub(crate) fn register(&mut self, _hash: &str) { unimplemented!() }
    pub(crate) fn forget(&mut self) { unimplemented!() }
    pub(crate) fn set_last(&mut self, _sequence: i64) { unimplemented!() }
    pub(crate) fn last(&self) -> i64 { unimplemented!() }
    pub(crate) fn script(&mut self, _operation: &str, _answer: A) { unimplemented!() }
    pub(crate) fn standing(&mut self, _answer: A) { unimplemented!() }
    pub(crate) fn take(&mut self, _operation: &str) -> Option<A> { unimplemented!() }
    pub(crate) fn admit(&mut self, _request: &Request) -> Admission<A> { unimplemented!() }
    pub(crate) fn settle(&mut self, _request: &Request, _answer: A) { unimplemented!() }
}

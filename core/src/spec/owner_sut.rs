//! Owner-level real side of the tests that the public API cannot drive (`api_gap.md`). Compiled only with the feature `spec_api`.
//! Phase C binds it to the `Lifecycle` and `Store` owners on `DetScheduler`: each input of the table is sent to the owner address, the effects and `Published` are read back. Until then every input is reported `unsupported`, so these tests are the list of work for phase C.

use async_trait::async_trait;
use serde_json::json;

use crate::check::model::Sut;
use crate::check::value::{Input, Observation};

pub struct OwnerSut;

impl OwnerSut {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Sut for OwnerSut {
    async fn apply(&mut self, step: &Input) -> Vec<Observation> {
        vec![Observation::new("unsupported", json!(step.name))]
    }
}

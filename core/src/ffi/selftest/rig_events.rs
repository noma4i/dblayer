use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{json, Value};

use super::rig_tests::{bound, Rig};
use crate::ffi::{ReadRequest, ReadStream, ReadValue};
use crate::transport::encrypted::cable::tests::peer_tests::{CablePeer, PeerEvent};

type ModelStreams = BTreeMap<(u64, String), (Arc<ReadStream>, u64)>;

pub(super) struct Events {
    pub(super) peer: CablePeer,
    token: tokio::sync::watch::Sender<Option<String>>,
    streams: tokio::sync::Mutex<ModelStreams>,
}

impl Events {
    pub(super) fn new() -> Self {
        let token = tokio::sync::watch::channel(None).0;
        let seen = token.clone();
        let peer = CablePeer::observed(crate::transport::encrypted::tests::protocol_tests::config(), move |event| {
            if let PeerEvent::Subscribed(subscribed) = event { crate::lock::effect(|| seen.send_replace(Some(subscribed))); }
        });
        Events { peer, token, streams: tokio::sync::Mutex::new(BTreeMap::new()) }
    }
}

/// The models of `edit` take the rows of the server events: each model listens to `RigEvents` at `rigEvents.<Model>` and a row with `deleted` is removed.
pub(super) fn events(edit: impl FnOnce(&mut Value)) -> impl FnOnce(&mut Value) {
    move |models| {
        edit(models);
        for model in models.as_array_mut().unwrap() {
            let key = model["key"].as_str().unwrap().to_string();
            let listen = json!({"subscription": "RigEvents", "root": format!("rigEvents.{key}"), "deleted_by": "deleted", "apply": {"effect": "upsert"}});
            model["listens"].as_array_mut().unwrap().push(listen);
        }
    }
}

impl Rig {
    pub(super) async fn land(&self, model: &str, row: Value) { self.frame(model, row).await }

    pub(super) async fn land_removed(&self, model: &str, id: &str) { self.frame(model, json!({"id": id, "deleted": true})).await }

    async fn frame(&self, model: &str, row: Value) {
        let events = &self.switch.events;
        let epoch = self.database.session_epoch();
        let (stream, reader) = {
            let mut streams = events.streams.lock().await;
            match streams.get(&(epoch, model.to_string())) {
                Some(entry) => entry.clone(),
                None => {
                    let stream = self.database.subscribe_values(epoch, ReadRequest::Events { model: model.into() });
                    let crate::ffi::ReadClaim::Claimed { reader } = stream.claim_reader().await else { panic!("the event reader claim ended") };
                    assert!(matches!(bound(stream.pull(reader)).await, Some(ReadValue::Ready)), "the cable is ready for {model}");
                    streams.insert((epoch, model.to_string()), (stream.clone(), reader));
                    (stream, reader)
                }
            }
        };
        let token = bound(events.token.subscribe().wait_for(|token| token.is_some())).await.unwrap().clone().unwrap();
        assert!(events.peer.event(&token, json!({"data": {"rigEvents": {model: row}}})), "the subscription is open");
        let seen = bound(stream.pull(reader)).await;
        assert!(matches!(seen, Some(ReadValue::Event { .. })), "{model} event landed: {seen:?}");
        self.settle().await;
    }
}

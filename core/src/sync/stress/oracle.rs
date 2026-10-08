use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use crate::db::Row;
use crate::query::Query;

use super::server::issue_of;
use super::world::World;

fn client_rows(world: &World) -> BTreeMap<String, Row> {
    world.rig.db.query(&Query::new("moments")).unwrap_or_default().into_iter().map(|row| (row.id.to_string(), row)).collect()
}

fn liked(row: &Row) -> bool {
    row.get("likedByMe").and_then(serde_json::Value::as_bool).unwrap_or(false)
}

pub fn lost_writes(world: &World) {
    for issued in world.issued() {
        if world.server.deliveries_of(&issued.token) > 0 {
            continue;
        }
        if issued.target.as_deref().is_some_and(|target| world.was_discarded(target)) {
            continue;
        }
        let creator = issued.target.as_deref().and_then(|target| world.creator_of(target));
        if creator.is_some_and(|creator| world.server.was_refused(&creator)) {
            continue;
        }
        world.findings.report("lost-write", format!("{} {} was issued but never reached the server", issued.kind, issued.token));
    }
}

pub fn settled_client(world: &World) {
    let server = world.server.state();
    let client = client_rows(world);
    let reset = world.moments_reset.load(Ordering::SeqCst);
    for (id, row) in &client {
        if row.is_local() {
            world.findings.report("temp-left", format!("temp row {id} {:?} is still stored after every operation finished", row.text("text")));
        } else if !server.contains_key(id) && !world.was_discarded(id) && !world.touched(id) {
            world.findings.report("ghost-row", format!("row {id} {:?} is stored but the server does not have it", row.text("text")));
        }
    }
    for (id, (text, server_liked)) in &server {
        let Some(row) = client.get(id) else {
            let ours = issue_of(text).is_some_and(|(_, issue)| issue > 0);
            if ours && !reset && world.server.deliveries_of(text) == 1 && !world.touched(id) {
                world.findings.report("missing-row", format!("server row {id} {text} was written by this client but is not stored"));
            }
            continue;
        };
        if world.touched(id) || world.was_discarded(id) || world.server.was_ambiguous(text) {
            continue;
        }
        if row.text("text") != Some(text.as_str()) || liked(row) != *server_liked {
            world.findings.report("divergence", format!("row {id}: client has {:?} liked {} but the server has {text} liked {server_liked}", row.text("text"), liked(row)));
        }
    }
}

pub fn reloaded_client(world: &World) {
    let server = world.server.state();
    let client = client_rows(world);
    for (id, (text, server_liked)) in &server {
        match client.get(id) {
            None => world.findings.report("reload-missing", format!("server row {id} {text} is not stored after a complete reload")),
            Some(row) if row.text("text") != Some(text.as_str()) || liked(row) != *server_liked => {
                world.findings.report("reload-divergence", format!("row {id}: client has {:?} liked {} after a complete reload, the server has {text} liked {server_liked}", row.text("text"), liked(row)));
            }
            Some(_) => {}
        }
    }
}

pub fn digest(world: &World) -> String {
    let client: Vec<String> = client_rows(world).values().map(|row| format!("{}|{}", row.text("text").unwrap_or_default(), liked(row))).collect();
    let mut client = client;
    client.sort();
    let server: Vec<String> = world.server.state().values().map(|(text, liked)| format!("{text}|{liked}")).collect();
    let mut server = server;
    server.sort();
    format!("client {client:?}\nserver {server:?}\nrequests {}\nissued {}", world.server.received(), world.issued().len())
}

use std::collections::HashSet;

use dblayer::schema::Storage;

use crate::decl::{AttributeDecl, ModelDecl, Pos};

pub(crate) fn selected(node: &ModelDecl) -> Vec<String> {
    let Some(cloud) = &node.cloud else { return Vec::new() };
    if cloud.names.is_empty() {
        node.attributes.iter().map(|attribute| attribute.name.clone()).collect()
    } else {
        cloud.names.iter().map(|name| name.item.clone()).collect()
    }
}

pub(crate) fn check_cloud(node: &ModelDecl, report: &mut impl FnMut(Pos, String)) {
    let Some(cloud) = &node.cloud else { return };
    let model = node.name.as_str();
    if node.storage == Storage::Memory {
        report(cloud.pos, format!("model {model}: `cloud` is not allowed with `storage: :memory`, memory rows never reach the cloud"));
        return;
    }
    if cloud.names.is_empty() && node.storage == Storage::Disk {
        report(cloud.pos, format!("model {model}: `cloud` on a `storage: :disk` model needs attribute names like `cloud :draft, :lastRead`"));
        return;
    }
    let mut listed = HashSet::new();
    let mut valid = true;
    for name in &cloud.names {
        let word = name.item.as_str();
        if !listed.insert(word) {
            report(name.pos, format!("model {model}: `cloud` lists `{word}` twice"));
            valid = false;
        } else if node.ephemeral.iter().flat_map(|group| &group.attributes).any(|attribute| attribute.name == word) {
            report(name.pos, format!("model {model}: `cloud` cannot select ephemeral attribute `{word}`, ephemeral values are never stored"));
            valid = false;
        } else if let Some(attribute) = node.attributes.iter().find(|attribute| attribute.name == word) {
            if node.storage == Storage::Disk && !attribute.local {
                report(name.pos, format!("model {model}: `cloud` cannot select server attribute `{word}`, only `local: true` attributes on a `storage: :disk` model"));
                valid = false;
            }
        } else {
            report(name.pos, format!("model {model}: `cloud` names unknown attribute `{word}`, it must be an `attribute` declared in this model (a parameter is not an attribute)"));
            valid = false;
        }
    }
    if !valid {
        return;
    }
    if node.attributes.is_empty() {
        report(cloud.pos, format!("model {model}: `cloud` selects no attribute because the model declares none"));
    }
    if node.storage == Storage::Local && !cloud.names.is_empty() {
        for attribute in node.attributes.iter().filter(|attribute| !listed.contains(attribute.name.as_str()) && is_required(attribute)) {
            report(cloud.pos, format!("model {model}: attribute `{}` is required and has no default, so a device that receives only the `cloud` selection cannot create the row: add it to `cloud`, give it a default or make it `null: true`", attribute.name));
        }
    }
}

fn is_required(attribute: &AttributeDecl) -> bool {
    attribute.name != "id" && !attribute.null && !attribute.optional && attribute.default.is_none()
}

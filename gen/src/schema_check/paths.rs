use super::graphql::{Followed, Miss, Select};
use super::*;
use crate::decl::{OperationDecl, SubscribeDecl};

pub(super) fn segments_of(path: &str) -> Option<Vec<&str>> {
    let segments: Vec<&str> = path.split('.').collect();
    segments.iter().all(|segment| !segment.is_empty()).then_some(segments)
}

impl<'a> Run<'a> {
    pub(super) fn check_change_payloads(&mut self, node: &'a ModelDecl, nodes: &'a [ModelDecl]) {
        let Some(feed) = nodes.iter().find_map(|node| node.changes.first()) else { return };
        let operation = feed.subscription.as_str();
        for (change, path) in node.changed_by.iter().flat_map(|change| change.payload.as_deref().map(|path| (change, path))) {
            let Some(event) = self.follow_operation(Spot { file: node.file, at: change.pos, label: "changes" }, operation, &feed.from.item) else {
                self.fail(node.file, change.pos, format!("payload requires subscription document `{operation}`"));
                continue;
            };
            let mut current = self.schema.subscription.clone();
            let mut valid = true;
            for name in &event.names {
                match self.schema.field(&current, name) {
                    Ok(ty) => current = ty.name().to_string(),
                    Err(message) => { self.fail(node.file, change.pos, message); valid = false; break; }
                }
            }
            if !valid { continue; }
            let prefix: Vec<_> = feed.from.item.split('.').collect();
            let Some(payload) = self.follow_inside(Spot { file: node.file, at: change.pos, label: "payload" }, operation, &prefix, &event.selects, path) else { continue };
            let mut reached = None;
            for name in &payload.names {
                match self.schema.field(&current, name) {
                    Ok(ty) if !ty.is_list() => { current = ty.name().to_string(); reached = Some(ty); }
                    Ok(_) => { self.fail(node.file, change.pos, "payload must name one model object".into()); valid = false; break; }
                    Err(message) => { self.fail(node.file, change.pos, message); valid = false; break; }
                }
            }
            if !valid || reached.is_none() { continue; }
            let expected = node.scopes.iter().find(|scope| scope.name == change.via).and_then(|scope| self.resolve(node, scope));
            if expected.as_deref() != Some(current.as_str()) {
                self.fail(node.file, change.pos, format!("payload `{path}` returns `{current}`, expected model `{}`", node.name));
                continue;
            }
            let deeper: Vec<_> = prefix.iter().copied().chain(path.split('.')).collect();
            self.follow_inside(Spot { file: node.file, at: change.pos, label: "payload id" }, operation, &deeper, &payload.selects, "id");
            self.require_payload_fields(node, &node.attributes, operation, &deeper, &payload.selects);
            let fields: Vec<_> = node.attributes.iter().collect();
            self.check_fields(node, &fields, &current, &Scope::inside(node.file, payload.selects), &mut BTreeSet::new(), &mut Vec::new());
        }
    }

    fn require_payload_fields(&mut self, node: &'a ModelDecl, fields: &'a [AttributeDecl], operation: &str, prefix: &[&str], starts: &[&'a Select]) {
        for field in fields.iter().filter(|field| !field.local && !field.derived) {
            let Some(path) = self.path_of(node, field, &field.name).map(|path| path.join(".")) else { continue };
            let Some(found) = self.follow_inside(Spot { file: node.file, at: field.pos, label: "payload field" }, operation, prefix, starts, &path) else { continue };
            if let Some((_, Body::Fields(children))) = self.body_of(field, node.file) {
                let deeper: Vec<_> = prefix.iter().copied().chain(path.split('.')).collect();
                self.require_payload_fields(node, children, operation, &deeper, &found.selects);
            }
        }
    }

    /// M75: a row of an operation response carries the membership field of each scope of its model that rows of a response enter.
    fn require_membership_fields(&mut self, model: &'a ModelDecl, spot: Spot, operation: &str, root: &str, starts: &[&'a Select]) {
        let prefix: Vec<&str> = root.split('.').collect();
        let fields: BTreeSet<&str> = model.scopes.iter()
            .filter(|scope| scope.insert_on.as_ref().is_some_and(|insert| insert.item == crate::decl::InsertOn::Server))
            .flat_map(|scope| scope.membership.iter())
            .map(|(_, rule, _)| match rule { dblayer::schema::Membership::Contains(field) | dblayer::schema::Membership::Equals(field) => field.as_str() })
            .collect();
        for field in fields {
            let Some(attribute) = model.attributes.iter().find(|attribute| attribute.name == field && !attribute.local) else { continue };
            let Some(path) = self.path_of(model, attribute, field).map(|path| path.join(".")) else { continue };
            self.follow_inside(Spot { file: spot.file, at: spot.at, label: spot.label }, operation, &prefix, starts, &path);
        }
    }

    pub(super) fn follow_quiet(&self, operation: &str, starts: Option<&[&'a Select]>, segments: &[&str]) -> Option<Result<Followed<'a>, Miss>> {
        let documents: &'a Operations = self.operations;
        let top;
        let starts = match starts {
            Some(starts) => starts,
            None => {
                top = [&documents.named.get(operation)?.select];
                &top
            }
        };
        Some(documents.follow(starts, segments))
    }

    pub(super) fn report_miss(&mut self, spot: Spot, operation: &str, path: &str, prefix: &[&str], segments: &[&str], miss: Miss) {
        let before: Vec<&str> = prefix.iter().copied().chain(segments[..miss.at].iter().copied()).collect();
        let place = if before.is_empty() { "at the top level".to_string() } else { format!("under `{}`", before.join(".")) };
        let keys = if miss.keys.is_empty() { "no fields".to_string() } else { miss.keys.into_iter().collect::<Vec<_>>().join(", ") };
        self.fail(spot.file, spot.at, format!("{} `{path}`: operation `{operation}` has no `{}` {place}, it has: {keys}", spot.label, segments[miss.at]));
    }

    pub(super) fn follow_operation(&mut self, spot: Spot, operation: &str, path: &str) -> Option<Followed<'a>> {
        let segments = segments_of(path)?;
        match self.follow_quiet(operation, None, &segments)? {
            Ok(found) => Some(found),
            Err(miss) => {
                self.report_miss(spot, operation, path, &[], &segments, miss);
                None
            }
        }
    }

    pub(super) fn follow_inside(&mut self, spot: Spot, operation: &str, prefix: &[&str], starts: &[&'a Select], path: &str) -> Option<Followed<'a>> {
        let segments = segments_of(path)?;
        match self.follow_quiet(operation, Some(starts), &segments)? {
            Ok(found) => Some(found),
            Err(miss) => {
                self.report_miss(spot, operation, path, prefix, &segments, miss);
                None
            }
        }
    }

    pub(super) fn check_response_paths(&mut self, node: &'a ModelDecl, nodes: &'a [ModelDecl]) {
        for operation in &node.operations {
            self.check_operation_paths(node, operation, nodes);
        }
        for subscription in node.subscriptions.iter() {
            self.check_subscription_paths(node, subscription, "subscribe");
        }
        for scope in &node.scopes {
            for subscription in &scope.subscriptions {
                self.check_subscription_paths(node, subscription, "subscribe");
            }
            if scope.is_remote() {
                self.check_cursor(node, scope);
            }
        }
    }

    fn check_operation_paths(&mut self, node: &'a ModelDecl, action: &'a OperationDecl, nodes: &'a [ModelDecl]) {
        let Some(mutation) = action.mutation.as_ref() else { return };
        let operation = mutation.item.as_str();
        let Some(declared) = self.operations.named.get(operation) else { return };
        let root = match &action.response {
            Some(crate::decl::ResponseDecl::Read(read)) => read.from.as_ref(),
            _ => None,
        };
        let lands = action.response != Some(crate::decl::ResponseDecl::Ignore) && action.name != "destroy";
        if let Some(found) = root {
            if let Some(reached) = self.follow_operation(Spot { file: node.file, at: found.pos, label: "response from" }, operation, &found.item).filter(|_| lands) {
                self.require_membership_fields(node, Spot { file: node.file, at: found.pos, label: "response field" }, operation, &found.item, &reached.selects);
            }
        } else if lands {
            if let Some((key, Some(Ok(reached)))) = declared.root.as_ref().map(|first| (first.key.as_str(), self.follow_quiet(operation, None, &[first.key.as_str()]))) {
                self.require_membership_fields(node, Spot { file: node.file, at: mutation.pos, label: "response field" }, operation, key, &reached.selects);
            }
        }
        for landing in &action.upserts {
            let Some(reached) = self.follow_operation(Spot { file: node.file, at: landing.from.pos, label: "upsert" }, operation, &landing.from.item).filter(|_| lands) else { continue };
            if let Some(model) = nodes.iter().find(|candidate| candidate.name == landing.model) {
                self.require_membership_fields(model, Spot { file: node.file, at: landing.from.pos, label: "response field" }, operation, &landing.from.item, &reached.selects);
            }
        }
    }

    fn check_subscription_paths(&mut self, node: &'a ModelDecl, subscription: &'a SubscribeDecl, label: &str) {
        let operation = subscription.name.as_str();
        let Some(declared) = self.operations.named.get(operation) else { return };
        let Some(first) = declared.root.as_ref().map(|root| root.key.as_str()) else { return };
        let root = subscription.from.as_ref();
        let path = root.map_or(first, |found| found.item.as_str());
        let followed = match root {
            Some(found) => self.follow_operation(Spot { file: node.file, at: found.pos, label: &format!("{label} from") }, operation, &found.item),
            None => self.follow_quiet(operation, None, &[first]).and_then(Result::ok),
        };
        let Some(reached) = followed else { return };
        let prefix: Vec<&str> = path.split('.').collect();
        for id in &subscription.id {
            self.follow_inside(Spot { file: node.file, at: id.pos, label: &format!("{label} id") }, operation, &prefix, &reached.selects, &id.item);
        }
    }

    fn check_cursor(&mut self, node: &'a ModelDecl, remote: &'a ScopeDecl) {
        let Some(from) = remote.paginate.as_ref().and_then(|paginate| paginate.cursor.as_ref()).map(|cursor| &cursor.from) else { return };
        let operation = query_of(remote);
        let Some(declared) = self.operations.named.get(operation) else { return };
        let Some(connection) = self.root_select(remote) else { return };
        let Some(root) = root_path(remote).or_else(|| declared.root.as_ref().map(|found| found.key.as_str())) else { return };
        let prefix: Vec<&str> = root.split('.').collect();
        self.follow_inside(Spot { file: node.file, at: from.pos, label: "cursor from" }, operation, &prefix, &[connection], &from.item);
    }
}

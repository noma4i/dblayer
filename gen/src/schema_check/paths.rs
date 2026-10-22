use super::graphql::{Followed, Miss, Select};
use super::*;
use crate::decl::{OperationDecl, SubscribeDecl};

pub(super) fn segments_of(path: &str) -> Option<Vec<&str>> {
    let segments: Vec<&str> = path.split('.').collect();
    segments.iter().all(|segment| !segment.is_empty()).then_some(segments)
}

impl<'a> Run<'a> {
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

    pub(super) fn report_miss(&mut self, file: usize, at: Pos, label: &str, operation: &str, path: &str, prefix: &[&str], segments: &[&str], miss: Miss) {
        let before: Vec<&str> = prefix.iter().copied().chain(segments[..miss.at].iter().copied()).collect();
        let place = if before.is_empty() { "at the top level".to_string() } else { format!("under `{}`", before.join(".")) };
        let keys = if miss.keys.is_empty() { "no fields".to_string() } else { miss.keys.into_iter().collect::<Vec<_>>().join(", ") };
        self.fail(file, at, format!("{label} `{path}`: operation `{operation}` has no `{}` {place}, it has: {keys}", segments[miss.at]));
    }

    pub(super) fn follow_operation(&mut self, file: usize, at: Pos, label: &str, operation: &str, path: &str) -> Option<Followed<'a>> {
        let segments = segments_of(path)?;
        match self.follow_quiet(operation, None, &segments)? {
            Ok(found) => Some(found),
            Err(miss) => {
                self.report_miss(file, at, label, operation, path, &[], &segments, miss);
                None
            }
        }
    }

    pub(super) fn follow_inside(&mut self, file: usize, at: Pos, label: &str, operation: &str, prefix: &[&str], starts: &[&'a Select], path: &str) -> Option<Followed<'a>> {
        let segments = segments_of(path)?;
        match self.follow_quiet(operation, Some(starts), &segments)? {
            Ok(found) => Some(found),
            Err(miss) => {
                self.report_miss(file, at, label, operation, path, prefix, &segments, miss);
                None
            }
        }
    }

    pub(super) fn check_response_paths(&mut self, node: &'a ModelDecl) {
        for operation in &node.operations {
            self.check_operation_paths(node, operation);
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

    fn check_operation_paths(&mut self, node: &'a ModelDecl, action: &'a OperationDecl) {
        let Some(operation) = action.mutation.as_ref().map(|found| found.item.as_str()) else { return };
        let Some(declared) = self.operations.named.get(operation) else { return };
        let root = match &action.response {
            Some(crate::decl::ResponseDecl::Read(read)) => read.from.as_ref(),
            _ => None,
        };
        if let Some(found) = root {
            self.follow_operation(node.file, found.pos, "response from", operation, &found.item);
        }
        for landing in &action.upserts {
            self.follow_operation(node.file, landing.from.pos, "upsert", operation, &landing.from.item);
        }
        let Some(result) = &action.returns else { return };
        let payload = root.map(|found| found.item.split('.').next().unwrap_or_default()).or_else(|| declared.root.as_ref().map(|found| found.key.as_str()));
        let Some(payload) = payload else { return };
        let Some(Ok(reached)) = self.follow_quiet(operation, None, &[payload]) else { return };
        self.check_result(node.file, operation, &[payload], &reached.selects, result);
    }

    pub(super) fn check_standalone_result(&mut self, file: usize, operation: &str, result: &[AttributeDecl]) {
        let Some(payload) = self.operations.named.get(operation).and_then(|declared| declared.root.as_ref()).map(|root| root.key.as_str()) else { return };
        let Some(Ok(reached)) = self.follow_quiet(operation, None, &[payload]) else { return };
        self.check_result(file, operation, &[payload], &reached.selects, result);
    }

    fn check_result(&mut self, file: usize, operation: &str, prefix: &[&str], starts: &[&'a Select], attributes: &[AttributeDecl]) {
        for attribute in attributes {
            let Some(found) = self.follow_inside(file, attribute.pos, "returns", operation, prefix, starts, &attribute.name) else { continue };
            if let AttributeBlock::Fields(inner) = &attribute.block {
                let deeper: Vec<&str> = prefix.iter().copied().chain([attribute.name.as_str()]).collect();
                self.check_result(file, operation, &deeper, &found.selects, inner);
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
            Some(found) => self.follow_operation(node.file, found.pos, &format!("{label} from"), operation, &found.item),
            None => self.follow_quiet(operation, None, &[first]).and_then(Result::ok),
        };
        let (Some(reached), Some(id)) = (followed, subscription.id.as_ref()) else { return };
        let prefix: Vec<&str> = path.split('.').collect();
        self.follow_inside(node.file, id.pos, &format!("{label} id"), operation, &prefix, &reached.selects, &id.item);
    }

    fn check_cursor(&mut self, node: &'a ModelDecl, remote: &'a ScopeDecl) {
        let Some(from) = remote.paginate.as_ref().and_then(|paginate| paginate.cursor.as_ref()).map(|cursor| &cursor.from) else { return };
        let operation = query_of(remote);
        let Some(declared) = self.operations.named.get(operation) else { return };
        let Some(connection) = self.root_select(remote) else { return };
        let root = root_path(remote).or_else(|| declared.root.as_ref().map(|found| found.key.as_str())).unwrap_or_default();
        let prefix: Vec<&str> = root.split('.').collect();
        self.follow_inside(node.file, from.pos, "cursor from", operation, &prefix, &[connection], &from.item);
    }
}

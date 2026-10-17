use super::graphql::{Followed, Miss, Select};
use super::*;

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

    pub(super) fn report_miss(&mut self, file: usize, at: (usize, usize), label: &str, operation: &str, path: &str, prefix: &[&str], segments: &[&str], miss: Miss) {
        let before: Vec<&str> = prefix.iter().copied().chain(segments[..miss.at].iter().copied()).collect();
        let place = if before.is_empty() { "at the top level".to_string() } else { format!("under `{}`", before.join(".")) };
        let keys = if miss.keys.is_empty() { "no fields".to_string() } else { miss.keys.into_iter().collect::<Vec<_>>().join(", ") };
        self.fail(file, at.0, at.1, format!("{label} `{path}`: operation `{operation}` has no `{}` {place}, it has: {keys}", segments[miss.at]));
    }

    pub(super) fn follow_operation(&mut self, file: usize, at: (usize, usize), label: &str, operation: &str, path: &str) -> Option<Followed<'a>> {
        let segments = segments_of(path)?;
        match self.follow_quiet(operation, None, &segments)? {
            Ok(found) => Some(found),
            Err(miss) => {
                self.report_miss(file, at, label, operation, path, &[], &segments, miss);
                None
            }
        }
    }

    pub(super) fn follow_inside(&mut self, file: usize, at: (usize, usize), label: &str, operation: &str, prefix: &[&str], starts: &[&'a Select], path: &str) -> Option<Followed<'a>> {
        let segments = segments_of(path)?;
        match self.follow_quiet(operation, Some(starts), &segments)? {
            Ok(found) => Some(found),
            Err(miss) => {
                self.report_miss(file, at, label, operation, path, prefix, &segments, miss);
                None
            }
        }
    }

    pub(super) fn check_response_paths(&mut self, node: &'a ModelNode) {
        for call in &node.body {
            match call.word.as_str() {
                "create" | "update" | "destroy" => {
                    let (Some(operation), Some(root)) = (option_text(call, "with"), option(call, "root")) else { continue };
                    if let Some(path) = text(&root.value.item) {
                        self.follow_operation(node.file, (root.value.line, root.value.column), "root", operation, path);
                    }
                }
                "action" => self.check_action_paths(node, call),
                "listen" => self.check_subscription_paths(node, call, "listen"),
                "remote" | "scope" => {
                    for updates in call.block.iter().flatten().filter(|line| line.word == "updates") {
                        self.check_subscription_paths(node, updates, "updates");
                    }
                    if call.word == "remote" { self.check_cursor(node, call); }
                }
                _ => {}
            }
        }
    }

    fn check_action_paths(&mut self, node: &'a ModelNode, action: &'a Call) {
        let lines = action.block.as_deref().unwrap_or_default();
        let line = |word: &str| lines.iter().find(|candidate| candidate.word == word);
        let Some(operation) = line("mutation").or_else(|| line("upload")).map(name_of).or_else(|| option_text(action, "mutation")) else { return };
        let Some(declared) = self.operations.named.get(operation) else { return };
        let root = line("root").map(|call| (name_of(call), (call.line, call.column)));
        if let Some((path, at)) = root {
            self.follow_operation(node.file, at, "root", operation, path);
        }
        for landing in lines.iter().filter(|call| call.word == "lands") {
            if let Some(path) = option_text(landing, "root") {
                let at = option(landing, "root").map_or((landing.line, landing.column), |pair| (pair.value.line, pair.value.column));
                self.follow_operation(node.file, at, "lands", operation, path);
            }
        }
        let Some(result) = line("result") else { return };
        let payload = root.map(|(path, _)| path.split('.').next().unwrap_or_default()).or_else(|| declared.root.as_ref().map(|found| found.key.as_str()));
        let Some(payload) = payload else { return };
        let Some(Ok(reached)) = self.follow_quiet(operation, None, &[payload]) else { return };
        self.check_result(node.file, operation, &[payload], &reached.selects, &result.options);
    }

    pub(super) fn check_standalone_result(&mut self, file: usize, operation: &str, result: &Call) {
        let Some(payload) = self.operations.named.get(operation).and_then(|declared| declared.root.as_ref()).map(|root| root.key.as_str()) else { return };
        let Some(Ok(reached)) = self.follow_quiet(operation, None, &[payload]) else { return };
        self.check_result(file, operation, &[payload], &reached.selects, &result.options);
    }

    fn check_result(&mut self, file: usize, operation: &str, prefix: &[&str], starts: &[&'a Select], pairs: &[Pair]) {
        for pair in pairs {
            let Some(found) = self.follow_inside(file, (pair.line, pair.column), "result", operation, prefix, starts, &pair.key) else { continue };
            let nested = match &pair.value.item {
                Value::Hash(inner) => Some(inner),
                Value::Array(items) => items.first().and_then(|item| match &item.item {
                    Value::Hash(inner) => Some(inner),
                    _ => None,
                }),
                _ => None,
            };
            if let Some(inner) = nested {
                let deeper: Vec<&str> = prefix.iter().copied().chain([pair.key.as_str()]).collect();
                self.check_result(file, operation, &deeper, &found.selects, inner);
            }
        }
    }

    fn check_subscription_paths(&mut self, node: &'a ModelNode, call: &'a Call, label: &str) {
        let operation = name_of(call);
        let Some(declared) = self.operations.named.get(operation) else { return };
        let Some(first) = declared.root.as_ref().map(|root| root.key.as_str()) else { return };
        let root = option(call, "root").and_then(|pair| text(&pair.value.item).map(|path| (path, (pair.value.line, pair.value.column))));
        let path = root.map_or(first, |(path, _)| path);
        let followed = match root {
            Some((path, at)) => self.follow_operation(node.file, at, &format!("{label} root"), operation, path),
            None => self.follow_quiet(operation, None, &[first]).and_then(Result::ok),
        };
        let (Some(reached), Some(id)) = (followed, option(call, "id")) else { return };
        let Some(key) = text(&id.value.item) else { return };
        let prefix: Vec<&str> = path.split('.').collect();
        self.follow_inside(node.file, (id.value.line, id.value.column), &format!("{label} id"), operation, &prefix, &reached.selects, key);
    }

    fn check_cursor(&mut self, node: &'a ModelNode, remote: &'a Call) {
        let lines = remote.block.as_deref().unwrap_or_default();
        let Some(from) = lines.iter().find(|call| call.word == "paginate").and_then(|paginate| option(paginate, "from")) else { return };
        let (Some(query), Some(path)) = (lines.iter().find(|call| call.word == "query"), text(&from.value.item)) else { return };
        let operation = name_of(query);
        let Some(declared) = self.operations.named.get(operation) else { return };
        let Some(connection) = self.root_select(remote) else { return };
        let root = option_text(query, "root").or_else(|| declared.root.as_ref().map(|found| found.key.as_str())).unwrap_or_default();
        let prefix: Vec<&str> = root.split('.').collect();
        self.follow_inside(node.file, (from.value.line, from.value.column), "paginate from", operation, &prefix, &[connection], path);
    }
}

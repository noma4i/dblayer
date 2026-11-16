use super::*;

impl Run<'_> {
    pub(super) fn check_standalone(&mut self, node: &CallDecl) {
        let Some((kind, name, pos)) = &node.kind else { return };
        let (name, pos) = (name.as_str(), *pos);
        let word = if *kind == CallKind::Query { "query" } else { "mutation" };
        let Some(operation) = self.operations.named.get(name) else {
            self.fail(node.file, pos, format!("operation `{name}` is not in the operation documents"));
            return;
        };
        if operation.kind != word {
            self.fail(node.file, pos, format!("operation `{name}` is a {}, not a {word}", operation.kind));
        }
        let accepted = infer::input_of(self.schema, operation, node.wrap.as_deref());
        for arg in &node.parameters {
            let Some(ty) = accepted.get(&arg.name) else {
                self.fail(node.file, arg.pos, format!("`{}` is not an input of operation `{name}`", arg.name));
                continue;
            };
            let kind = arg.ty.kind_text(arg.null);
            if !self.arg_fits(kind.trim_end_matches('?'), ty) || (kind.ends_with('?') && !ty.optional()) {
                self.fail(node.file, arg.pos, format!("parameter `{}` of kind `{}` does not fit `{}`{}", arg.name, crate::operation::display_kind(&kind), ty.describe(), self.unmapped_hint(ty.name())));
            }
        }
        for (key, ty) in accepted {
            if !ty.optional() && !node.parameters.iter().any(|arg| arg.name == key) {
                self.fail(node.file, pos, format!("operation `{name}` requires input `{key}: {}`", ty.describe()));
            }
        }
    }
}

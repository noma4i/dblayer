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
        if let Some(result) = &node.returns {
            self.check_standalone_result(node.file, name, result);
        }
        let accepted = match node.wrap.as_deref() {
            None => operation.variables.clone(),
            Some(key) => {
                let Some(variable) = operation.variables.get(key) else {
                    self.fail(node.file, pos, format!("operation `{name}` has no variable `${key}`"));
                    return;
                };
                if variable.is_list() {
                    self.fail(node.file, pos, format!("variable `${key}` must be an input object"));
                    return;
                }
                let Some(fields) = self.schema.input_fields(variable.name()) else {
                    self.fail(node.file, pos, format!("variable `${key}` must be an input object"));
                    return;
                };
                for (other, ty) in &operation.variables {
                    if other != key && !ty.optional() {
                        self.fail(node.file, pos, format!("operation `{name}` requires variable `${other}` outside `{key}`"));
                    }
                }
                fields
            }
        };
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

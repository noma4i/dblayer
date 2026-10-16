use super::*;

impl Run<'_> {
    pub(super) fn check_standalone(&mut self, node: &OperationNode) {
        let Some(named) = node.body.iter().find(|line| matches!(line.word.as_str(), "query" | "mutation")) else { return };
        let name = name_of(named);
        let Some(operation) = self.operations.named.get(name) else {
            self.fail(node.file, named.line, named.column, format!("operation `{name}` is not in the operation documents"));
            return;
        };
        if operation.kind != named.word {
            self.fail(node.file, named.line, named.column, format!("operation `{name}` is a {}, not a {}", operation.kind, named.word));
        }
        let wrap = node.body.iter().find(|line| line.word == "wrap").map(name_of);
        let accepted = match wrap {
            None => operation.variables.clone(),
            Some(key) => {
                let Some(variable) = operation.variables.get(key) else {
                    self.fail(node.file, named.line, named.column, format!("operation `{name}` has no variable `${key}`"));
                    return;
                };
                if variable.is_list() {
                    self.fail(node.file, named.line, named.column, format!("variable `${key}` must be an input object"));
                    return;
                }
                let Some(fields) = self.schema.input_fields(variable.name()) else {
                    self.fail(node.file, named.line, named.column, format!("variable `${key}` must be an input object"));
                    return;
                };
                for (other, ty) in &operation.variables {
                    if other != key && !ty.optional() {
                        self.fail(node.file, named.line, named.column, format!("operation `{name}` requires variable `${other}` outside `{key}`"));
                    }
                }
                fields
            }
        };
        let args = node.body.iter().find(|line| line.word == "args").map_or(&[][..], |line| line.options.as_slice());
        for arg in args {
            let Some(ty) = accepted.get(&arg.key) else {
                self.fail(node.file, arg.line, arg.column, format!("`{}` is not an input of operation `{name}`", arg.key));
                continue;
            };
            let Some(kind) = text(&arg.value.item) else { continue };
            if !self.arg_fits(kind.trim_end_matches('?'), ty) || (kind.ends_with('?') && !ty.optional()) {
                self.fail(node.file, arg.line, arg.column, format!("arg `{}` of kind `:{kind}` does not fit `{}`{}", arg.key, ty.describe(), self.unmapped_hint(ty.name())));
            }
        }
        for (key, ty) in accepted {
            if !ty.optional() && !args.iter().any(|arg| arg.key == key) {
                self.fail(node.file, named.line, named.column, format!("operation `{name}` requires input `{key}: {}`", ty.describe()));
            }
        }
    }
}

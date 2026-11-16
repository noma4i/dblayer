use dblayer::schema::FieldSpec;
use dblayer::value::ScalarKind;

use super::graphql::{Operation, Operations, Schema, Select, Ty, TyKind, TypeKind};
use super::infer::Failure;
use crate::decl::{CallDecl, ModelDecl, Pos, ResponseDecl};
use crate::naming::camel;
use crate::scalars::{self, Scalars};

/// M53: the fields of an answer, the selection of the document under its root field typed by the schema.
struct Typer<'a> {
    schema: &'a Schema,
    scalars: &'a Scalars,
    documents: &'a Operations,
}

type Child<'a> = (String, String, Vec<&'a Select>);

impl<'a> Typer<'a> {
    fn answer(&self, document: &'a Operation, payload: Option<&str>, name: &str, app_type: String) -> Result<Option<FieldSpec>, String> {
        let root = document.root.as_ref().ok_or("the document selects no field")?;
        let key = match payload {
            Some(key) => key,
            None => &root.key,
        };
        let followed = self.documents.follow(&[&document.select], &[key]).map_err(|_| format!("the document does not select `{key}`"))?;
        let operation_type = match document.kind {
            "query" => &self.schema.query,
            "mutation" => &self.schema.mutation,
            _ => &self.schema.subscription,
        };
        let reached = self.schema.field(operation_type, followed.names[0])?;
        let mut answer = FieldSpec::object(name);
        let named = reached.name();
        let (variants, _) = self.variants(named, &followed.selects);
        let only = if let [only] = variants[..] { only } else { named };
        answer.fields = self.fields_of(only, &followed.selects, &|on| on.is_none_or(|on| on == named || on == only), &app_type)?;
        answer.app_type = Some(app_type);
        Ok((!answer.fields.is_empty()).then_some(answer))
    }

    fn children(&self, selects: &[&'a Select], applies: &dyn Fn(Option<&str>) -> bool) -> Vec<Child<'a>> {
        let mut found: Vec<Child<'a>> = Vec::new();
        for select in selects {
            self.documents.each_select(select, None, &mut |inner, on| {
                if !applies(on) {
                    return;
                }
                for field in &inner.fields {
                    match found.iter_mut().find(|(key, _, _)| *key == field.key) {
                        Some((_, _, nested)) => nested.push(&field.select),
                        None => found.push((field.key.clone(), field.name.clone(), vec![&field.select])),
                    }
                }
            }, &mut Vec::new());
        }
        found
    }

    fn fields_of(&self, type_name: &str, selects: &[&'a Select], applies: &dyn Fn(Option<&str>) -> bool, owner: &str) -> Result<Vec<FieldSpec>, String> {
        self.children(selects, applies).into_iter().map(|(key, name, nested)| {
            let reached = self.schema.field(type_name, &name)?;
            self.field(&key, &reached, &nested, owner)
        }).collect()
    }

    fn field(&self, key: &str, reached: &Ty, selects: &[&'a Select], owner: &str) -> Result<FieldSpec, String> {
        let named = reached.name();
        if matches!(&reached.kind, TyKind::List(item) if item.is_list()) {
            return Err(format!("field `{key}` is a list of lists: an answer has no nested lists"));
        }
        let list = reached.is_list();
        let field = match (Schema::is_builtin_scalar(named), self.schema.types.get(named)) {
            (true, _) => self.scalar(key, named, list)?,
            (false, Some(TypeKind::Scalar)) => self.scalar(key, named, list)?,
            (false, Some(TypeKind::Enum(values))) => {
                let mut field = if list { FieldSpec::list(key, ScalarKind::Str) } else { FieldSpec::enumeration(key, &values.iter().map(String::as_str).collect::<Vec<_>>()) };
                field.app_type = Some(named.to_string());
                if list {
                    field.values = values.clone();
                }
                field
            }
            (false, Some(TypeKind::Fields(_) | TypeKind::Union(_))) => self.composite(key, named, list, selects, owner)?,
            _ => return Err(format!("field `{key}` has the type `{named}`, which is not an output type of the schema")),
        };
        Ok(field.nullable())
    }

    fn scalar(&self, key: &str, named: &str, list: bool) -> Result<FieldSpec, String> {
        let mapped = match named {
            "String" | "ID" => if named == "ID" { "id" } else { "string" },
            "Int" => "int",
            "Float" => "num",
            "Boolean" => "bool",
            "JSON" => "json",
            other => self.scalars.get(other).map(String::as_str).ok_or_else(|| format!("field `{key}`: {}", scalars::unmapped(other)))?,
        };
        let kind = match mapped {
            "id" => ScalarKind::Id,
            "string" | "date" => ScalarKind::Str,
            "int" | "long" => ScalarKind::Int,
            "num" => ScalarKind::Num,
            "bool" => ScalarKind::Bool,
            _ => return Ok(FieldSpec::raw(key)),
        };
        Ok(if list { FieldSpec::list(key, kind) } else { FieldSpec::scalar(key, kind) })
    }

    /// M53: the types of `... on T` under an interface or union field, and whether `__typename` is selected.
    fn variants<'b>(&self, named: &str, selects: &[&'b Select]) -> (Vec<&'b str>, bool) where 'a: 'b {
        let abstract_type = matches!(self.schema.types.get(named), Some(TypeKind::Union(_))) || self.schema.is_interface(named);
        let (mut variants, mut typename) = (Vec::new(), false);
        for select in selects {
            self.documents.each_select(select, None, &mut |inner, on| {
                typename |= inner.typename;
                if let Some(on) = on.filter(|on| abstract_type && *on != named && !variants.contains(on)) {
                    variants.push(on);
                }
            }, &mut Vec::new());
        }
        (variants, typename)
    }

    fn composite(&self, key: &str, named: &str, list: bool, selects: &[&'a Select], owner: &str) -> Result<FieldSpec, String> {
        let app_type = format!("{owner}{}", camel(key));
        let (variants, typename) = self.variants(named, selects);
        if variants.len() <= 1 {
            let mut object = if list { FieldSpec::array(key) } else { FieldSpec::object(key) };
            object.fields = match variants.first() {
                Some(only) => self.fields_of(only, selects, &|on| on.is_none_or(|on| on == named || on == *only), &app_type)?,
                None => self.fields_of(named, selects, &|_| true, &app_type)?,
            };
            object.app_type = Some(app_type);
            return Ok(object);
        }
        if !typename {
            return Err(format!("field `{key}`: the selection of `{named}` must select `__typename`"));
        }
        let cases = crate::unions::case_names(&variants);
        crate::unions::check_cases(named, &cases).map_err(|message| format!("field `{key}`: {message}"))?;
        let built = variants.iter().zip(&cases).map(|(variant, case)| {
            let type_name = format!("{app_type}{variant}");
            let fields = self.fields_of(variant, selects, &|on| on.is_none_or(|on| on == named || on == *variant), &type_name)?;
            Ok(FieldSpec { app_type: Some(type_name), ..FieldSpec::variant(case, variant, fields) })
        }).collect::<Result<Vec<_>, String>>()?;
        Ok(if list { FieldSpec::union_array(key, &app_type, built) } else { FieldSpec::union(key, &app_type, built) })
    }
}

fn failure(file: usize, pos: Pos, name: &str, message: String) -> Failure {
    (file, pos, format!("operation `{name}`: the answer type: {message}"))
}

/// M53 and M68: the answer type of every standalone operation, and of every model operation that says `returns`.
pub(super) fn infer(schema: &Schema, scalars: &Scalars, documents: &Operations, models: &mut [ModelDecl], calls: &mut [CallDecl]) -> Vec<Failure> {
    let typer = Typer { schema, scalars, documents };
    let mut failures = Vec::new();
    for node in models.iter_mut().filter(|node| node.storage != dblayer::schema::Storage::Local) {
        for operation in node.operations.iter_mut() {
            let Some(at) = operation.returns else { continue };
            let Some(document) = operation.mutation.as_ref().and_then(|found| documents.named.get(&found.item)) else { continue };
            let from = match &operation.response {
                Some(ResponseDecl::Read(read)) => read.from.as_ref().and_then(|found| found.item.split('.').next()),
                _ => None,
            };
            let app_type = format!("{}{}Result", node.name, camel(&operation.name));
            match typer.answer(document, from, &operation.name, app_type) {
                Ok(Some(answer)) => operation.answer = Some(answer),
                Ok(None) => failures.push(failure(node.file, at, &operation.name, "the document selects no fields under its root field: delete `returns`".to_string())),
                Err(message) => failures.push(failure(node.file, at, &operation.name, message)),
            }
        }
    }
    for call in calls.iter_mut() {
        let Some(document) = call.kind.as_ref().and_then(|(_, name, _)| documents.named.get(name)) else { continue };
        match typer.answer(document, None, &call.name, format!("{}Result", camel(&call.name))) {
            Ok(answer) => call.answer = answer,
            Err(message) => failures.push(failure(call.file, call.pos, &call.name, message)),
        }
    }
    failures
}

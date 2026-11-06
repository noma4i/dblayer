use dblayer::schema::ModelSpec;

use super::{at, Found};
use crate::decl::ModelDecl;

/// `removed_by Model, match:` names a model of the project and fields of equal kind on both sides.
pub(super) fn check_removed_by(nodes: &[ModelDecl], models: &[ModelSpec], errors: &mut Vec<Found>) {
    for (node, model) in nodes.iter().zip(models) {
        for removal in &node.removed_by {
            let mut report = |message: String| errors.push(at(node.file, removal.pos, message));
            let Some(source) = models.iter().find(|candidate| candidate.key.as_str() == removal.model) else {
                report(format!("removed_by names the unknown model `{}`", removal.model));
                continue;
            };
            for (own, theirs) in &removal.matches {
                match (model.field(own), source.field(theirs)) {
                    (None, _) => report(format!("removed_by matches `{own}`, which `{}` does not declare", model.key)),
                    (_, None) => report(format!("removed_by matches `{theirs}`, which `{}` does not declare", source.key)),
                    (Some(left), Some(right)) if left.kind != right.kind => report(format!("removed_by matches `{own}` with `{theirs}`, which have another type")),
                    _ => {}
                }
            }
        }
    }
}

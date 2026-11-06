//! inv_dsl_03: the page size of a remote declaration reaches the request as `first`, and the page size of the operation is the default.

use serde_json::json;

use super::window_io::WindowCodec;
use super::windows::WindowKey;
use super::{ReadOperation, ReadOperations, SchemaWindowCodec};
use crate::store::spec_fixtures::{all_specs, moments_spec, obj};
use crate::store::Store;
use crate::schema::{Coverage, PageDirection, RelationSpec, RemoteShape, RemoteSpec};

fn codec(page_size: Option<u32>) -> SchemaWindowCodec {
    let mut moments = moments_spec();
    let mut mine = RelationSpec::new("mine", &["userId"]);
    mine.remote = Some(RemoteSpec {
        shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        operation: "Mine".into(),
        stale_time: "1.minute".into(),
        empty_stale_time: None,
        selects_page_info: true,
        page_size,
        variables: Default::default(),
        root: None,
        total_from: None,
    });
    moments.relations.push(mine);
    let specs = all_specs().into_iter().map(|spec| if spec.key.as_str() == "moments" { moments.clone() } else { spec }).collect();
    let operations = ReadOperations::default();
    operations.declare([ReadOperation { name: "Mine".into(), document: "query Mine($first: Int) { moments { id } }".into(), root_path: "moments".into(), page_size: 20 }]);
    SchemaWindowCodec::new(Store::open_models(None, specs, None).map(std::sync::Arc::new).unwrap(), operations)
}

#[test]
fn inv_dsl_03_the_declared_page_size_reaches_the_request_and_the_operation_size_is_the_default() {
    let key = WindowKey::relation("moments", "mine", &obj(json!({"userId": "u1"})));
    assert_eq!(codec(Some(7)).request(&key, 1, None).unwrap().variables["first"], json!(7));
    assert_eq!(codec(None).request(&key, 1, None).unwrap().variables["first"], json!(20));
}

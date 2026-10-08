use crate::schema::{Coverage, RemoteSpec};
use crate::store::MembershipChange;
use crate::store::MembershipPlan;

pub fn coverage_change(coverage: Coverage, current: &[String], incoming: &[String]) -> MembershipChange {
    match coverage {
        Coverage::Complete => MembershipPlan::complete(current, incoming),
        Coverage::Page => MembershipPlan::page(current, incoming),
    }
}

pub fn replaces_membership(coverage: Coverage) -> bool {
    coverage == Coverage::Complete
}

pub fn coverage_of(remote: Option<&RemoteSpec>) -> Coverage {
    remote.map_or(Coverage::Page, RemoteSpec::coverage)
}

pub fn drops_missing(coverage: Coverage, is_first_page: bool) -> bool {
    coverage == Coverage::Complete && is_first_page
}

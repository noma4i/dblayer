mod filter;
mod place;
mod run;
mod statement;
pub(crate) use statement::compile_tail;

pub use filter::{CompareOp, Where};
pub use place::place_of;
pub use run::{fetch_count, fetch_keys, fetch_ranked, RankedRow};
pub use statement::{compile_count, compile_keys, compile_ranked, ranking, windowable, CompiledStatement, Order, Query, QueryError, RankedStatement};

#[cfg(test)]
mod filter_tests;
#[cfg(test)]
mod ranked_scope_tests;
#[cfg(test)]
mod run_tests;
#[cfg(test)]
mod statement_tests;
#[cfg(test)]
mod union_tests;

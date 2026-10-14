mod filter;
mod run;
mod statement;

pub use filter::{CompareOp, Where};
pub use run::{fetch_count, fetch_keys};
pub use statement::{compile_count, compile_keys, CompiledStatement, Order, Query, QueryError};
#[cfg(test)]
pub use statement::{scope_count_statement, scope_statement};

#[cfg(test)]
mod filter_tests;
#[cfg(test)]
mod run_tests;
#[cfg(test)]
mod statement_tests;

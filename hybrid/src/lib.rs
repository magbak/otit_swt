use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod change_types;
pub mod combiner;
pub mod constants;
pub mod constraints;
mod exists_helper;
mod find_query_variables;
mod groupby_pushdown;
pub mod orchestrator;
pub mod preprocessing;
pub mod query_context;
pub mod rewriting;
mod sparql_result_to_polars;
pub mod splitter;
pub mod static_sparql;
pub mod timeseries_database;
pub mod timeseries_query;

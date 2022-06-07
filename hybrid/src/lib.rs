pub mod splitter;
pub mod preprocessing;
pub mod rewriting;
pub mod constants;
pub mod constraints;
pub mod timeseries_query;
pub mod change_types;
pub mod timeseries_database;
pub mod combiner;
pub mod static_sparql;
pub mod orchestrator;
mod groupby_pushdown;
mod sparql_result_to_polars;
pub(crate) mod find_query_variables;
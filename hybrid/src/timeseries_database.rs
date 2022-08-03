pub mod arrow_flight_sql_database;
pub mod timeseries_sql_rewrite;
pub mod simple_in_memory_timeseries;
pub mod opcua_history_read;

use crate::timeseries_query::TimeSeriesQuery;
use async_trait::async_trait;
use polars::frame::DataFrame;
use std::error::Error;

#[async_trait]
pub trait TimeSeriesQueryable {
    async fn execute(&mut self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>>;
}

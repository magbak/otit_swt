pub mod arrow_flight_sql_database;

use crate::timeseries_query::TimeSeriesQuery;
use polars::frame::DataFrame;
use std::error::Error;
use async_trait::async_trait;

#[async_trait]
pub trait TimeSeriesQueryable {
    async fn execute(&mut self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>>;
}

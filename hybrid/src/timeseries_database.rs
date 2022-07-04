use crate::timeseries_query::TimeSeriesQuery;
use polars::frame::DataFrame;
use std::error::Error;

pub trait TimeSeriesQueryable {
    fn execute(&self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>>;
}

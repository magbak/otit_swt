use std::error::Error;
use polars::frame::DataFrame;
use crate::timeseries_query::TimeSeriesQuery;


pub trait TimeSeriesQueryable {
    fn execute(&self, tsq:&TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>>;
}
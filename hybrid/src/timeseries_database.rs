use polars::frame::DataFrame;
use crate::timeseries_query::TimeSeriesQuery;

pub struct TimeSeriesQueryError {
    pub message: String
}

pub trait TimeSeriesQueriable {
    fn execute(&self, tsq:TimeSeriesQuery) -> Result<DataFrame, TimeSeriesQueryError>;
}
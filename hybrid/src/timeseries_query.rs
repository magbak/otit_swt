use std::time::Duration;
use spargebra::algebra::Expression;
use spargebra::term::Variable;

pub enum AggregationType {
    First,
    Last,
    Mean,
    Min,
    Max,
    Sum
}

pub struct Grouping {
    interval: Duration,
    aggregation: AggregationType
}

pub struct TimeSeriesQuery {
    identifier_variable: Option<Variable>,
    value_variable: Option<Variable>,
    timestamp_variable: Option<Variable>,
    ids: Option<Vec<String>>,
    grouping: Option<Grouping>,
    condition: Option<Expression>
}

impl TimeSeriesQuery {
    pub fn new() -> TimeSeriesQuery {
        TimeSeriesQuery{
            identifier_variable: None,
            value_variable: None,
            timestamp_variable: None,
            ids: None,
            grouping: None,
            condition: None
        }
    }
}
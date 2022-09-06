mod aggregate_expression;
mod expressions;
mod graph_patterns;
mod order_expression;
mod synchronization;

use crate::change_types::ChangeType;
use crate::pushdown_setting::PushdownSetting;
use crate::query_context::Context;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use polars_core::frame::DataFrame;
use spargebra::Query;
use std::collections::HashSet;

#[derive(Debug)]
pub struct TimeSeriesQueryPrepper<'a> {
    pushdown_settings: HashSet<PushdownSetting>,
    allow_compound_timeseries_queries: bool,
    basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
    static_result_df: &'a DataFrame,
}

impl TimeSeriesQueryPrepper<'_> {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        allow_compound_timeseries_queries: bool,
        basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
        static_result_df: &DataFrame,
    ) -> TimeSeriesQueryPrepper {
        TimeSeriesQueryPrepper {
            allow_compound_timeseries_queries,
            pushdown_settings,
            basic_time_series_queries,
            static_result_df,
        }
    }

    pub fn prepare(&mut self, query: Query) -> Vec<TimeSeriesQuery> {
        if let Query::Select {
            dataset,
            pattern,
            base_iri,
        } = &query
        {
            let mut pattern_prepared =
                self.prepare_graph_pattern(pattern, false, &Context::new());
            pattern_prepared.drained_time_series_queries().collect()
        } else {
            panic!("Only support for Select");
        }
    }
}

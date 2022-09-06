use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::pushdown_setting::PushdownSetting;
use crate::query_context::{Context, PathEntry};
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::pushups::apply_pushups;
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;

impl TimeSeriesQueryPrepper {
    pub fn prepare_filter(
        &mut self,
        expression: &Expression,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut inner_prepare = self.prepare_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::FilterInner),
        );
        let mut out_tsqs=  vec![];
        for t in inner_prepare.drain_time_series_queries() {
            let (time_series_condition, lost_value) =
                t.rewrite_filter_expression(expression, context, pushdown_settings);
            if try_groupby_complex_query && lost_value {
                return GPPrepReturn::fail_groupby_complex_query();
            }
            if let Some(expr) = time_series_condition {
                out_tsqs.push(TimeSeriesQuery::Filtered(
                    Box::new(t),
                    expr,
                ))
            } else {
                out_tsqs.push(t);
            }
        }
        GPPrepReturn::new(out_tsqs)
    }
}
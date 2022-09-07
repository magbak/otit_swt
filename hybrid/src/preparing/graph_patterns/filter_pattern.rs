use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::preparing::graph_patterns::filter_expression_rewrites::rewrite_filter_expression;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::{Expression, GraphPattern};

impl TimeSeriesQueryPrepper {
    pub fn prepare_filter(
        &mut self,
        expression: &Expression,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut expression_prepare =
            self.prepare_expression(expression, try_groupby_complex_query, &context.extension_with(PathEntry::FilterExpression));
        let mut inner_prepare = self.prepare_graph_pattern(
            inner,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::FilterInner),
        );
        if expression_prepare.fail_groupby_complex_query && inner_prepare.fail_groupby_complex_query
        {
            return GPPrepReturn::fail_groupby_complex_query();
        }

        let mut out_tsqs = vec![];
        out_tsqs.extend(expression_prepare.drained_time_series_queries());
        println!("Out filter tsqs: {:?}", out_tsqs);
        for t in inner_prepare.drained_time_series_queries() {
            let use_change_type = if try_groupby_complex_query {
                ChangeType::NoChange
            } else {
                ChangeType::Relaxed
            };
            let (time_series_condition, lost_value) = rewrite_filter_expression(
                &t,
                expression,
                &use_change_type,
                context,
                &self.pushdown_settings,
            );
            if try_groupby_complex_query && lost_value {
                return GPPrepReturn::fail_groupby_complex_query();
            }
            if let Some(expr) = time_series_condition {
                out_tsqs.push(TimeSeriesQuery::Filtered(Box::new(t), expr))
            } else {
                out_tsqs.push(t);
            }
        }
        GPPrepReturn::new(out_tsqs)
    }
}

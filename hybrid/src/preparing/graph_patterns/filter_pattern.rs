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
        let mut expression_prepare = self.prepare_expression(
            expression,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::FilterExpression),
        );
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
        for t in inner_prepare.drained_time_series_queries() {
            let use_change_type = if try_groupby_complex_query {
                ChangeType::NoChange
            } else {
                ChangeType::Relaxed
            };
            let conj_vec = conjunction_to_vec(self.rewritten_filters.get(&context));
            let (time_series_condition, lost_value) = rewrite_filter_expression(
                &t,
                expression,
                &use_change_type,
                context,
                &conj_vec,
                &self.pushdown_settings,
            );
            if try_groupby_complex_query && (lost_value || time_series_condition.is_none()) {
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

fn conjunction_to_vec(expr_opt: Option<&Expression>) -> Option<Vec<&Expression>> {
    let mut out = vec![];
    if let Some(expr) = expr_opt {
        match expr {
            Expression::And(left, right) => {
                let left_conj = conjunction_to_vec(Some(left));
                if let Some(left_vec) = left_conj {
                    out.extend(left_vec);
                }
                let right_conj = conjunction_to_vec(Some(right));
                if let Some(right_vec) = right_conj {
                    out.extend(right_vec);
                }
            }
            _ => {
                out.push(expr);
            }
        }
    }
    if out.len() > 0 {
        Some(out)
    } else {
        None
    }
}

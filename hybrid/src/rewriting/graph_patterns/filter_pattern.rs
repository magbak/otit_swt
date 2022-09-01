use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::pushdown_setting::PushdownSetting;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::pushups::apply_pushups;
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;

impl StaticQueryRewriter {
    pub fn rewrite_filter(
        &mut self,
        expression: &Expression,
        inner: &GraphPattern,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::FilterInner),
        );

        if inner_rewrite.graph_pattern.is_some() {
            pushdown_expression(
                inner_rewrite.time_series_queries.as_mut(),
                expression,
                &context.extension_with(PathEntry::FilterExpression),
                &self.pushdown_settings,
            );

            let mut expression_rewrite = self.rewrite_expression(
                expression,
                required_change_direction,
                &inner_rewrite.variables_in_scope,
                &context.extension_with(PathEntry::FilterExpression),
            );
            if expression_rewrite.expression.is_some() {
                let use_change;
                if expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange {
                    use_change = inner_rewrite.change_type.clone();
                } else if expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed {
                    if &inner_rewrite.change_type == &ChangeType::Relaxed
                        || &inner_rewrite.change_type == &ChangeType::NoChange
                    {
                        use_change = ChangeType::Relaxed;
                    } else {
                        return GPReturn::only_timeseries_queries(
                            inner_rewrite.drained_time_series_queries(),
                        );
                    }
                } else if expression_rewrite.change_type.as_ref().unwrap()
                    == &ChangeType::Constrained
                {
                    if &inner_rewrite.change_type == &ChangeType::Constrained {
                        use_change = ChangeType::Constrained;
                    } else {
                        return GPReturn::only_timeseries_queries(
                            inner_rewrite.drained_time_series_queries(),
                        );
                    }
                } else {
                    panic!("Should never happen");
                }
                let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
                inner_rewrite
                    .with_graph_pattern(GraphPattern::Filter {
                        expr: expression_rewrite.expression.take().unwrap(),
                        inner: Box::new(apply_pushups(
                            inner_graph_pattern,
                            &mut expression_rewrite.graph_pattern_pushups,
                        )),
                    })
                    .with_change_type(use_change);
                return inner_rewrite;
            } else {
                let mut inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
                inner_graph_pattern = apply_pushups(
                    inner_graph_pattern,
                    &mut expression_rewrite.graph_pattern_pushups,
                );
                inner_rewrite.with_graph_pattern(inner_graph_pattern);
                return inner_rewrite;
            }
        }
        GPReturn::only_timeseries_queries(inner_rewrite.drained_time_series_queries())
    }
}

fn pushdown_expression(
    tsqs: &mut Vec<TimeSeriesQuery>,
    expr: &Expression,
    context: &Context,
    pushdown_settings: &HashSet<PushdownSetting>,
) {
    //Todo check if expr is a synchronizer, else do stuff below
    let mut out_tsqs = vec![];

    for t in tsqs.drain(0..tsqs.len()) {
        let (time_series_condition, lost_value) =
            t.rewrite_filter_expression(expr, context, pushdown_settings);
        if let Some(expr) = time_series_condition {
            out_tsqs.push(TimeSeriesQuery::Filtered(
                Box::new(t),
                Some(expr),
                lost_value,
            ))
        } else if lost_value {
            out_tsqs.push(TimeSeriesQuery::Filtered(Box::new(t), None, lost_value))
        } else {
            out_tsqs.push(t);
        }
    }
    tsqs.extend(out_tsqs.into_iter());
}

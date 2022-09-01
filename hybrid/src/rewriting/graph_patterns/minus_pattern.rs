use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> GPReturn {
        let mut left_rewrite = self.rewrite_graph_pattern(
            left,
            required_change_direction,
            &context.extension_with(PathEntry::MinusLeftSide),
        );
        let mut right_rewrite = self.rewrite_graph_pattern(
            right,
            &required_change_direction.opposite(),
            &context.extension_with(PathEntry::MinusRightSide),
        );
        let mut all_tsqs: Vec<TimeSeriesQuery> = left_rewrite
            .time_series_queries
            .drain(0..left_rewrite.time_series_queries.len())
            .collect();
        all_tsqs.extend(
            right_rewrite
                .time_series_queries
                .drain(0..right_rewrite.time_series_queries.len()),
        );

        if left_rewrite.graph_pattern.is_some() {
            if right_rewrite.graph_pattern.is_some() {
                if &left_rewrite.change_type == &ChangeType::NoChange
                    && &right_rewrite.change_type == &ChangeType::NoChange
                {
                    let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                    let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                    left_rewrite
                        .with_graph_pattern(GraphPattern::Minus {
                            left: Box::new(left_graph_pattern),
                            right: Box::new(right_graph_pattern),
                        })
                        .with_time_series_queries(all_tsqs);
                    return left_rewrite;
                } else if (&left_rewrite.change_type == &ChangeType::Relaxed
                    || &left_rewrite.change_type == &ChangeType::NoChange)
                    && (&right_rewrite.change_type == &ChangeType::Constrained
                        || &right_rewrite.change_type == &ChangeType::NoChange)
                {
                    let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                    let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                    left_rewrite
                        .with_graph_pattern(GraphPattern::Minus {
                            left: Box::new(left_graph_pattern),
                            right: Box::new(right_graph_pattern),
                        })
                        .with_change_type(ChangeType::Relaxed)
                        .with_time_series_queries(all_tsqs);
                    return left_rewrite;
                } else if (&left_rewrite.change_type == &ChangeType::Constrained
                    || &left_rewrite.change_type == &ChangeType::NoChange)
                    && (&right_rewrite.change_type == &ChangeType::Relaxed
                        || &right_rewrite.change_type == &ChangeType::NoChange)
                {
                    let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                    let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                    left_rewrite
                        .with_graph_pattern(GraphPattern::Minus {
                            left: Box::new(left_graph_pattern),
                            right: Box::new(right_graph_pattern),
                        })
                        .with_change_type(ChangeType::Constrained)
                        .with_time_series_queries(all_tsqs);
                    return left_rewrite;
                }
            } else {
                //left some, right none
                if &left_rewrite.change_type == &ChangeType::NoChange
                    || &left_rewrite.change_type == &ChangeType::Relaxed
                {
                    left_rewrite
                        .with_change_type(ChangeType::Relaxed)
                        .with_time_series_queries(all_tsqs);
                    return left_rewrite;
                }
            }
        }
        GPReturn::only_timeseries_queries(all_tsqs)
    }
}

use super::TimeSeriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry};
use log::debug;
use spargebra::algebra::{Expression, GraphPattern};

impl TimeSeriesQueryPrepper {
    pub fn prepare_left_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        _expression_opt: &Option<Expression>,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            debug!(
                "Encountered graph inside left join, not supported for complex groupby pushdown"
            );
            return GPPrepReturn::fail_groupby_complex_query();
        } else {
            let mut left_prepare = self.prepare_graph_pattern(
                left,
                try_groupby_complex_query,
                &context.extension_with(PathEntry::LeftJoinLeftSide),
            );
            let mut right_prepare = self.prepare_graph_pattern(
                right,
                try_groupby_complex_query,
                &context.extension_with(PathEntry::LeftJoinRightSide),
            );
            left_prepare.with_time_series_queries_from(&mut right_prepare);
            left_prepare
        }
    }
}

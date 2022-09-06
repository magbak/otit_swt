use log::debug;
use super::TimeSeriesQueryPrepper;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;
use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeSeriesQueryPrepper<'_> {
    pub fn prepare_union(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            debug!("Encountered union inside left join, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query()
        } else {
            let mut left_prepare = self.prepare_graph_pattern(
                left,
                try_groupby_complex_query,
                &context.extension_with(PathEntry::JoinLeftSide),
            );
            let mut right_prepare = self.prepare_graph_pattern(
                right,
                try_groupby_complex_query,
                &context.extension_with(PathEntry::JoinRightSide),
            );
            left_prepare.with_time_series_queries_from(&mut right_prepare);
            left_prepare
        }
    }
}

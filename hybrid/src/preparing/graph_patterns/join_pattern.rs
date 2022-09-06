use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::synchronization::create_identity_synchronized_queries;
use spargebra::algebra::GraphPattern;

impl TimeSeriesQueryPrepper {
    pub fn prepare_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut left_prepare = self.prepare_graph_pattern(
            left,
            required_change_direction,
            &context.extension_with(PathEntry::JoinLeftSide),
        );
        let mut right_prepare = self.prepare_graph_pattern(
            right,
            required_change_direction,
            &context.extension_with(PathEntry::JoinRightSide),
        );
    }
}

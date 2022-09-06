use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::GraphPattern;
use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_union(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut left_prepare = self.prepare_graph_pattern(
            left,
            &context.extension_with(PathEntry::UnionLeftSide),
        );
        let mut right_prepare = self.prepare_graph_pattern(
            right,
            &context.extension_with(PathEntry::UnionRightSide),
        );
    }
}

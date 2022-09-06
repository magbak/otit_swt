use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::GraphPattern;

impl TimeSeriesQueryPrepper {
    pub fn prepare_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut left_prepare = self.prepare_graph_pattern(
            left,
            required_change_direction,
            &context.extension_with(PathEntry::MinusLeftSide),
        );
        let mut right_prepare = self.prepare_graph_pattern(
            right,
            &required_change_direction.opposite(),
            &context.extension_with(PathEntry::MinusRightSide),
        );
    }
}

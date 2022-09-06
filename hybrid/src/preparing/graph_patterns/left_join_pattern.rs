use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::{Expression, GraphPattern};

impl TimeSeriesQueryPrepper {
    pub fn prepare_left_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression_opt: &Option<Expression>,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut left_prepare = self.prepare_graph_pattern(
            left,
            required_change_direction,
            &context.extension_with(PathEntry::LeftJoinLeftSide),
        );
        let mut right_prepare = self.prepare_graph_pattern(
            right,
            required_change_direction,
            &context.extension_with(PathEntry::LeftJoinRightSide),
        );
    }
}

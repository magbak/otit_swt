use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;
use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_reduced(
        &mut self,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut inner_prepare = self.prepare_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::ReducedInner),
        );
    }
}

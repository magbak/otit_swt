use super::TimeSeriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry};
use log::debug;
use spargebra::algebra::GraphPattern;

impl TimeSeriesQueryPrepper {
    pub fn prepare_reduced(
        &mut self,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            debug!("Encountered graph inside reduced, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query();
        } else {
            let inner_prepare = self.prepare_graph_pattern(
                inner,
                try_groupby_complex_query,
                &context.extension_with(PathEntry::ReducedInner),
            );
            inner_prepare
        }
    }
}

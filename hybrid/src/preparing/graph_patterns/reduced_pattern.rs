use log::debug;
use super::TimeSeriesQueryPrepper;
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
        if try_groupby_complex_query {
            debug!("Encountered graph inside reduced, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query()
        } else {
            let mut inner_prepare = self.prepare_graph_pattern(
                inner,
                try_groupby_complex_query,
                &context.extension_with(PathEntry::ReducedInner),
            );
            inner_prepare
        }
    }
}

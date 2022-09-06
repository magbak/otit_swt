use log::debug;
use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::preparing::graph_patterns::GPPrepReturn;
use spargebra::algebra::GraphPattern;

impl TimeSeriesQueryPrepper {
    pub fn prepare_distinct(
        &mut self,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,

        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            debug!("Encountered distinct inside groupby, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query()
        }
        let mut gpr_inner =
            self.prepare_graph_pattern(inner, try_groupby_complex_query, &context.extension_with(PathEntry::DistinctInner));
        gpr_inner
    }
}

use log::debug;
use super::TimeSeriesQueryPrepper;

use crate::query_context::Context;
use crate::preparing::graph_patterns::GPPrepReturn;
use spargebra::algebra::GraphPattern;
use spargebra::term::NamedNodePattern;

impl TimeSeriesQueryPrepper {
    pub fn prepare_graph(
        &mut self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            debug!("Encountered graph inside groupby, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query()
        } else {
            let mut inner_gpr = self.prepare_graph_pattern(inner, try_groupby_complex_query, context);
            inner_gpr
        }

    }
}

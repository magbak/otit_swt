use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
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
        let mut inner_gpr = self.prepare_graph_pattern(inner, context);
    }
}

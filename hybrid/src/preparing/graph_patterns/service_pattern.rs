use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::GraphPattern;
use spargebra::term::NamedNodePattern;
use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_service(
        &mut self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        silent: &bool,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut inner_prepare = self.prepare_graph_pattern(
            inner,
            &context.extension_with(PathEntry::ServiceInner),
        );
    }
}

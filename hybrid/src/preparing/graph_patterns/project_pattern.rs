use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
         let mut inner_rewrite = self.rewrite_graph_pattern(
            inner,
            &context.extension_with(PathEntry::ProjectInner),
        );
    }
}

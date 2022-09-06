use log::debug;
use super::TimeSeriesQueryPrepper;
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
        if try_groupby_complex_query {
            debug!("Encountered graph inside project, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query()
        } else {
            let mut inner_rewrite = self.prepare_graph_pattern(
                inner,
                try_groupby_complex_query,
                &context.extension_with(PathEntry::ProjectInner),
            );
            inner_rewrite
        }
    }
}

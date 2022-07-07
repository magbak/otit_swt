use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_distinct(
        &mut self,
        inner: &GraphPattern,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<GPReturn> {
        if let Some(mut gpr_inner) = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::DistinctInner),
        ) {
            let inner_graph_pattern = gpr_inner.graph_pattern.take().unwrap();
            gpr_inner.with_graph_pattern(GraphPattern::Distinct {
                inner: Box::new(inner_graph_pattern),
            });
            Some(gpr_inner)
        } else {
            None
        }
    }
}

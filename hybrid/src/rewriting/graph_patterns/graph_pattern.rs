use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::Context;
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;
use spargebra::term::NamedNodePattern;

impl StaticQueryRewriter {
    pub fn rewrite_graph(
        &mut self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<GPReturn> {
        if let Some(mut inner_gpr) =
            self.rewrite_graph_pattern(inner, required_change_direction, context)
        {
            let inner_rewrite = inner_gpr.graph_pattern.take().unwrap();
            inner_gpr.with_graph_pattern(GraphPattern::Graph {
                name: name.clone(),
                inner: Box::new(inner_rewrite),
            });
            return Some(inner_gpr);
        }
        None
    }
}

use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::order_expression::OEReturn;
use crate::rewriting::pushups::apply_pushups;
use spargebra::algebra::{GraphPattern, OrderExpression};

impl StaticQueryRewriter {
    pub fn rewrite_order_by(
        &mut self,
        inner: &GraphPattern,
        order_expressions: &Vec<OrderExpression>,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<GPReturn> {
        if let Some(mut gpr_inner) = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::OrderByInner),
        ) {
            let mut order_expressions_rewrite = order_expressions
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    self.rewrite_order_expression(
                        e,
                        &gpr_inner.variables_in_scope,
                        &context.extension_with(PathEntry::OrderByExpression(i as u16)),
                    )
                })
                .collect::<Vec<OEReturn>>();
            let mut inner_graph_pattern = gpr_inner.graph_pattern.take().unwrap();
            for oer in order_expressions_rewrite.iter_mut() {
                inner_graph_pattern =
                    apply_pushups(inner_graph_pattern, &mut oer.graph_pattern_pushups);
            }
            if order_expressions_rewrite
                .iter()
                .any(|oer| oer.order_expression.is_some())
            {
                gpr_inner.with_graph_pattern(GraphPattern::OrderBy {
                    inner: Box::new(inner_graph_pattern),
                    expression: order_expressions_rewrite
                        .iter_mut()
                        .filter(|oer| oer.order_expression.is_some())
                        .map(|oer| oer.order_expression.take().unwrap())
                        .collect(),
                });
            } else {
                gpr_inner.with_graph_pattern(inner_graph_pattern);
            }
            return Some(gpr_inner);
        }
        None
    }
}

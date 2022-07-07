use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::pushups::apply_pushups;
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_filter(
        &mut self,
        expression: &Expression,
        inner: &GraphPattern,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<GPReturn> {
        let inner_rewrite_opt = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::FilterInner),
        );
        self.pushdown_expression(
            expression,
            &context.extension_with(PathEntry::FilterExpression),
        );
        if let Some(mut gpr_inner) = inner_rewrite_opt {
            let mut expression_rewrite = self.rewrite_expression(
                expression,
                required_change_direction,
                &gpr_inner.variables_in_scope,
                &context.extension_with(PathEntry::FilterExpression),
            );
            if expression_rewrite.expression.is_some() {
                let use_change;
                if expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange {
                    use_change = gpr_inner.change_type.clone();
                } else if expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed {
                    if &gpr_inner.change_type == &ChangeType::Relaxed
                        || &gpr_inner.change_type == &ChangeType::NoChange
                    {
                        use_change = ChangeType::Relaxed;
                    } else {
                        return None;
                    }
                } else if expression_rewrite.change_type.as_ref().unwrap()
                    == &ChangeType::Constrained
                {
                    if &gpr_inner.change_type == &ChangeType::Constrained {
                        use_change = ChangeType::Constrained;
                    } else {
                        return None;
                    }
                } else {
                    panic!("Should never happen");
                }
                let inner_graph_pattern = gpr_inner.graph_pattern.take().unwrap();
                gpr_inner
                    .with_graph_pattern(GraphPattern::Filter {
                        expr: expression_rewrite.expression.take().unwrap(),
                        inner: Box::new(apply_pushups(
                            inner_graph_pattern,
                            &mut expression_rewrite.graph_pattern_pushups,
                        )),
                    })
                    .with_change_type(use_change);
                return Some(gpr_inner);
            } else {
                let mut inner_graph_pattern = gpr_inner.graph_pattern.take().unwrap();
                inner_graph_pattern = apply_pushups(
                    inner_graph_pattern,
                    &mut expression_rewrite.graph_pattern_pushups,
                );
                gpr_inner.with_graph_pattern(inner_graph_pattern);
                return Some(gpr_inner);
            }
        }
        None
    }
}

use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::pushups::apply_pushups;
use oxrdf::Variable;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;

impl StaticQueryRewriter {
    pub(crate) fn rewrite_extend(
        &mut self,
        inner: &GraphPattern,
        var: &Variable,
        expr: &Expression,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::ExtendInner),
        );
        if inner_rewrite.graph_pattern.is_some() {
            let mut expr_rewrite = self.rewrite_expression(
                expr,
                &ChangeType::NoChange,
                &inner_rewrite.variables_in_scope,
                &context.extension_with(PathEntry::ExtendExpression),
            );
            if expr_rewrite.expression.is_some() {
                inner_rewrite.variables_in_scope.insert(var.clone());
                let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
                inner_rewrite.with_graph_pattern(GraphPattern::Extend {
                    inner: Box::new(inner_graph_pattern), //No need for push up since there should be no change
                    variable: var.clone(),
                    expression: expr_rewrite.expression.take().unwrap(),
                });
                return inner_rewrite;
            } else {
                let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
                inner_rewrite.with_graph_pattern(apply_pushups(
                    inner_graph_pattern,
                    &mut expr_rewrite.graph_pattern_pushups,
                ));
                return inner_rewrite;
            }
        }
        let expr_rewrite = self.rewrite_expression(
            expr,
            &ChangeType::NoChange,
            &HashSet::new(),
            &context.extension_with(PathEntry::ExtendExpression),
        );
        if expr_rewrite.graph_pattern_pushups.len() > 0 {
            todo!("Solution will require graph pattern pushups for graph patterns!!");
        }
        return GPReturn::none();
    }
}

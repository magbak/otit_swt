use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::expressions::ExReturn;
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_exists_expression(
        &mut self,
        wrapped: &GraphPattern,
        context: &Context,
    ) -> ExReturn {
        let mut wrapped_rewrite = self.rewrite_graph_pattern(
            wrapped,
            &ChangeType::NoChange,
            &context.extension_with(PathEntry::Exists),
        );
        let mut exr = ExReturn::new();
        if wrapped_rewrite.graph_pattern.is_some() {
            if wrapped_rewrite.change_type == ChangeType::NoChange {
                exr.with_expression(Expression::Exists(Box::new(
                    wrapped_rewrite.graph_pattern.take().unwrap(),
                )))
                .with_change_type(ChangeType::NoChange);
                return exr;
            } else {
                for (v, vs) in &wrapped_rewrite.external_ids_in_scope {
                    self.additional_projections.insert(v.clone());
                    for vprime in vs {
                        self.additional_projections.insert(vprime.clone());
                    }
                }
                for (v, vs) in &wrapped_rewrite.datatypes_in_scope {
                    self.additional_projections.insert(v.clone());
                    for vprime in vs {
                        self.additional_projections.insert(vprime.clone());
                    }
                }
                if let GraphPattern::Project { inner, .. } =
                    wrapped_rewrite.graph_pattern.take().unwrap()
                {
                    exr.with_graph_pattern_pushup(*inner);
                } else {
                    todo!("Not supported")
                }
                return exr;
            }
        }
        exr
    }
}

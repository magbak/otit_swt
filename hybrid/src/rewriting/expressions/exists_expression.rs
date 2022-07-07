use spargebra::algebra::{Expression, GraphPattern};
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::expressions::ExReturn;
use super::StaticQueryRewriter;

impl StaticQueryRewriter {
    pub fn rewrite_exists_expression(
        &mut self,
        wrapped: &GraphPattern,
        context: &Context,
    ) -> ExReturn {
        let wrapped_rewrite = self.rewrite_graph_pattern(
                    wrapped,
                    &ChangeType::NoChange,
                    &context.extension_with(PathEntry::Exists),
                );
                let mut exr = ExReturn::new();
                if let Some(mut gpret) = wrapped_rewrite {
                    if gpret.change_type == ChangeType::NoChange {
                        exr.with_expression(Expression::Exists(Box::new(
                            gpret.graph_pattern.take().unwrap(),
                        )))
                        .with_change_type(ChangeType::NoChange);
                        return exr;
                    } else {
                        for (v, vs) in &gpret.external_ids_in_scope {
                            self.additional_projections.insert(v.clone());
                            for vprime in vs {
                                self.additional_projections.insert(vprime.clone());
                            }
                        }
                        if let GraphPattern::Project { inner, .. } =
                            gpret.graph_pattern.take().unwrap()
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
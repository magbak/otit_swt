use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::expressions::ExReturn;
use oxrdf::Variable;
use spargebra::algebra::Expression;
use std::collections::HashSet;

impl StaticQueryRewriter {
    pub fn rewrite_or_expression(
        &mut self,
        left: &Expression,
        right: &Expression,
        required_change_direction: &ChangeType,
        variables_in_scope: &HashSet<Variable>,
        context: &Context,
    ) -> ExReturn {
        let mut left_rewrite = self.rewrite_expression(
            left,
            required_change_direction,
            variables_in_scope,
            &context.extension_with(PathEntry::OrLeft),
        );
        let mut right_rewrite = self.rewrite_expression(
            right,
            required_change_direction,
            variables_in_scope,
            &context.extension_with(PathEntry::OrRight),
        );
        let mut exr = ExReturn::new();
        exr.with_pushups(&mut left_rewrite)
            .with_pushups(&mut right_rewrite);
        if left_rewrite.expression.is_some()
            && right_rewrite.expression.is_some()
            && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
            && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
        {
            let left_expression_rewrite = left_rewrite.expression.take().unwrap();
            let right_expression_rewrite = right_rewrite.expression.take().unwrap();
            exr.with_expression(Expression::Or(
                Box::new(left_expression_rewrite),
                Box::new(right_expression_rewrite),
            ))
            .with_change_type(ChangeType::NoChange);
            return exr;
        } else {
            match required_change_direction {
                ChangeType::Relaxed => {
                    if left_rewrite.expression.is_some() && right_rewrite.expression.is_some() {
                        if (left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            || left_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed)
                            && (right_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::NoChange
                                || right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Relaxed)
                        {
                            let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                            let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                            exr.with_expression(Expression::Or(
                                Box::new(left_expression_rewrite),
                                Box::new(right_expression_rewrite),
                            ))
                            .with_change_type(ChangeType::Relaxed);
                            return exr;
                        }
                    }
                }
                ChangeType::Constrained => {
                    if left_rewrite.expression.is_some() {
                        if right_rewrite.expression.is_some() {
                            if (left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                                || left_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Constrained)
                                && (right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                                    || right_rewrite.change_type.as_ref().unwrap()
                                        == &ChangeType::Constrained)
                            {
                                let left_expression_rewrite =
                                    left_rewrite.expression.take().unwrap();
                                let right_expression_rewrite =
                                    right_rewrite.expression.take().unwrap();
                                exr.with_expression(Expression::Or(
                                    Box::new(left_expression_rewrite),
                                    Box::new(right_expression_rewrite),
                                ))
                                .with_change_type(ChangeType::Constrained);
                                return exr;
                            }
                        } else {
                            //left some
                            if left_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Constrained
                                || left_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                            {
                                let left_expression_rewrite =
                                    left_rewrite.expression.take().unwrap();
                                exr.with_expression(left_expression_rewrite)
                                    .with_change_type(ChangeType::Constrained);
                                return exr;
                            }
                        }
                    } else if right_rewrite.expression.is_some() {
                        if right_rewrite.change_type.as_ref().unwrap() == &ChangeType::Constrained
                            || right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        {
                            let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                            exr.with_expression(right_expression_rewrite)
                                .with_change_type(ChangeType::Constrained);
                            return exr;
                        }
                    }
                }
                ChangeType::NoChange => {}
            }
        }
        self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
        exr
    }
}

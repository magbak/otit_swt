use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::pushups::apply_pushups;
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_left_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression_opt: &Option<Expression>,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<GPReturn> {
        let left_rewrite_opt = self.rewrite_graph_pattern(
            left,
            required_change_direction,
            &context.extension_with(PathEntry::LeftJoinLeftSide),
        );
        let right_rewrite_opt = self.rewrite_graph_pattern(
            right,
            required_change_direction,
            &context.extension_with(PathEntry::LeftJoinRightSide),
        );
        if let Some(expression) = expression_opt {
            self.pushdown_expression(expression, &context);
        }
        let mut expression_rewrite_opt = None;

        if let Some(mut gpr_left) = left_rewrite_opt {
            if let Some(mut gpr_right) = right_rewrite_opt {
                gpr_left.with_scope(&mut gpr_right);

                if let Some(expression) = expression_opt {
                    expression_rewrite_opt = Some(self.rewrite_expression(
                        expression,
                        required_change_direction,
                        &gpr_left.variables_in_scope,
                        &context.extension_with(PathEntry::LeftJoinExpression),
                    ));
                }
                if let Some(mut expression_rewrite) = expression_rewrite_opt {
                    if expression_rewrite.expression.is_some() {
                        let use_change;
                        if expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            && &gpr_left.change_type == &ChangeType::NoChange
                            && &gpr_right.change_type == &ChangeType::NoChange
                        {
                            use_change = ChangeType::NoChange;
                        } else if (expression_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || expression_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Relaxed)
                            && (&gpr_left.change_type == &ChangeType::NoChange
                                || &gpr_left.change_type == &ChangeType::Relaxed)
                            && (&gpr_right.change_type == &ChangeType::NoChange
                                || &gpr_right.change_type == &ChangeType::Relaxed)
                        {
                            use_change = ChangeType::Relaxed;
                        } else if (expression_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || expression_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Constrained)
                            && (&gpr_left.change_type == &ChangeType::NoChange
                                || &gpr_left.change_type == &ChangeType::Constrained)
                            && (&gpr_right.change_type == &ChangeType::NoChange
                                || &gpr_right.change_type == &ChangeType::Constrained)
                        {
                            use_change = ChangeType::Constrained;
                        } else {
                            return None;
                        }
                        let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                        let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                        gpr_left
                            .with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(apply_pushups(
                                    left_graph_pattern,
                                    &mut expression_rewrite.graph_pattern_pushups,
                                )),
                                right: Box::new(right_graph_pattern),
                                expression: Some(expression_rewrite.expression.take().unwrap()),
                            })
                            .with_change_type(use_change);
                        return Some(gpr_left);
                    } else {
                        //Expression rewrite is none, but we had an original expression
                        if (&gpr_left.change_type == &ChangeType::NoChange
                            || &gpr_left.change_type == &ChangeType::Relaxed)
                            && (&gpr_right.change_type == &ChangeType::NoChange
                                || &gpr_right.change_type == &ChangeType::Relaxed)
                        {
                            let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                            let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                            gpr_left
                                .with_graph_pattern(GraphPattern::LeftJoin {
                                    left: Box::new(apply_pushups(
                                        left_graph_pattern,
                                        &mut expression_rewrite.graph_pattern_pushups,
                                    )),
                                    right: Box::new(right_graph_pattern),
                                    expression: None,
                                })
                                .with_change_type(ChangeType::Relaxed);
                            return Some(gpr_left);
                        } else {
                            return None;
                        }
                    }
                } else {
                    //No original expression
                    if &gpr_left.change_type == &ChangeType::NoChange
                        && &gpr_right.change_type == &ChangeType::NoChange
                    {
                        let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                        let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                        gpr_left
                            .with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(left_graph_pattern),
                                right: Box::new(right_graph_pattern),
                                expression: None,
                            })
                            .with_change_type(ChangeType::NoChange);
                        return Some(gpr_left);
                    } else if (&gpr_left.change_type == &ChangeType::NoChange
                        || &gpr_left.change_type == &ChangeType::Relaxed)
                        && (&gpr_right.change_type == &ChangeType::NoChange
                            || &gpr_right.change_type == &ChangeType::Relaxed)
                    {
                        let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                        let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                        gpr_left
                            .with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(left_graph_pattern),
                                right: Box::new(right_graph_pattern),
                                expression: None,
                            })
                            .with_change_type(ChangeType::Relaxed);
                        return Some(gpr_left);
                    } else if (&gpr_left.change_type == &ChangeType::NoChange
                        || &gpr_left.change_type == &ChangeType::Constrained)
                        && (&gpr_right.change_type == &ChangeType::NoChange
                            || &gpr_right.change_type == &ChangeType::Constrained)
                    {
                        let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                        let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                        gpr_left
                            .with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(left_graph_pattern),
                                right: Box::new(right_graph_pattern),
                                expression: None,
                            })
                            .with_change_type(ChangeType::Constrained);
                        return Some(gpr_left);
                    }
                }
            } else {
                //left some, right none
                if let Some(expression) = expression_opt {
                    expression_rewrite_opt = Some(self.rewrite_expression(
                        expression,
                        required_change_direction,
                        &gpr_left.variables_in_scope,
                        &context.extension_with(PathEntry::LeftJoinExpression),
                    ));
                }
                if expression_rewrite_opt.is_some()
                    && expression_rewrite_opt
                        .as_ref()
                        .unwrap()
                        .expression
                        .is_some()
                {
                    if let Some(mut expression_rewrite) = expression_rewrite_opt {
                        if (expression_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || expression_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Relaxed)
                            && (&gpr_left.change_type == &ChangeType::NoChange
                                || &gpr_left.change_type == &ChangeType::Relaxed)
                        {
                            let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                            gpr_left
                                .with_graph_pattern(GraphPattern::Filter {
                                    expr: expression_rewrite.expression.take().unwrap(),
                                    inner: Box::new(apply_pushups(
                                        left_graph_pattern,
                                        &mut expression_rewrite.graph_pattern_pushups,
                                    )),
                                })
                                .with_change_type(ChangeType::Relaxed);
                            return Some(gpr_left);
                        }
                    }
                } else {
                    if &gpr_left.change_type == &ChangeType::NoChange
                        || &gpr_left.change_type == &ChangeType::Relaxed
                    {
                        gpr_left.with_change_type(ChangeType::Relaxed);
                        return Some(gpr_left);
                    }
                }
            }
        } else if let Some(mut gpr_right) = right_rewrite_opt
        //left none, right some
        {
            if let Some(expression) = expression_opt {
                expression_rewrite_opt = Some(self.rewrite_expression(
                    expression,
                    required_change_direction,
                    &gpr_right.variables_in_scope,
                    &context.extension_with(PathEntry::LeftJoinExpression),
                ));
            }
            if let Some(mut expression_rewrite) = expression_rewrite_opt {
                if expression_rewrite.expression.is_some()
                    && (expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        || expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed)
                    && (&gpr_right.change_type == &ChangeType::NoChange
                        || &gpr_right.change_type == &ChangeType::Relaxed)
                {
                    let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                    gpr_right
                        .with_graph_pattern(GraphPattern::Filter {
                            inner: Box::new(apply_pushups(
                                right_graph_pattern,
                                &mut expression_rewrite.graph_pattern_pushups,
                            )),
                            expr: expression_rewrite.expression.take().unwrap(),
                        })
                        .with_change_type(ChangeType::Relaxed);
                    return Some(gpr_right);
                }
            } else {
                if &gpr_right.change_type == &ChangeType::NoChange
                    || &gpr_right.change_type == &ChangeType::Relaxed
                {
                    gpr_right.with_change_type(ChangeType::Relaxed);
                    return Some(gpr_right);
                }
            }
        }
        None
    }
}

mod and_expression;
mod function_call_expression;
mod if_expression;
mod in_expression;
mod or_expression;

use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use oxrdf::Variable;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;

pub struct ExReturn {
    pub expression: Option<Expression>,
    pub change_type: Option<ChangeType>,
    pub graph_pattern_pushups: Vec<GraphPattern>,
}

impl ExReturn {
    fn new() -> ExReturn {
        ExReturn {
            expression: None,
            change_type: None,
            graph_pattern_pushups: vec![],
        }
    }

    fn with_expression(&mut self, expression: Expression) -> &mut ExReturn {
        self.expression = Some(expression);
        self
    }

    fn with_change_type(&mut self, change_type: ChangeType) -> &mut ExReturn {
        self.change_type = Some(change_type);
        self
    }

    fn with_graph_pattern_pushup(&mut self, graph_pattern: GraphPattern) -> &mut ExReturn {
        self.graph_pattern_pushups.push(graph_pattern);
        self
    }

    fn with_pushups(&mut self, exr: &mut ExReturn) -> &mut ExReturn {
        self.graph_pattern_pushups.extend(
            exr.graph_pattern_pushups
                .drain(0..exr.graph_pattern_pushups.len()),
        );
        self
    }
}

impl StaticQueryRewriter {
    pub fn rewrite_expression(
        &mut self,
        expression: &Expression,
        required_change_direction: &ChangeType,
        variables_in_scope: &HashSet<Variable>,
        context: &Context,
    ) -> ExReturn {
        match expression {
            Expression::NamedNode(nn) => {
                let mut exr = ExReturn::new();
                exr.with_expression(Expression::NamedNode(nn.clone()))
                    .with_change_type(ChangeType::NoChange);
                exr
            }
            Expression::Literal(l) => {
                let mut exr = ExReturn::new();
                exr.with_expression(Expression::Literal(l.clone()))
                    .with_change_type(ChangeType::NoChange);
                exr
            }
            Expression::Variable(v) => {
                if let Some(rewritten_variable) = self.rewrite_variable(v, context) {
                    if variables_in_scope.contains(v) {
                        let mut exr = ExReturn::new();
                        exr.with_expression(Expression::Variable(rewritten_variable))
                            .with_change_type(ChangeType::NoChange);
                        return exr;
                    }
                }
                ExReturn::new()
            }
            Expression::Or(left, right) => self.rewrite_or_expression(
                left,
                right,
                required_change_direction,
                variables_in_scope,
                context,
            ),

            Expression::And(left, right) => self.rewrite_and_expression(
                left,
                right,
                required_change_direction,
                variables_in_scope,
                context,
            ),
            Expression::Equal(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::EqualLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::EqualRight),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.expression.is_some()
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                    let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::Equal(
                        Box::new(left_expression_rewrite),
                        Box::new(right_expression_rewrite),
                    ))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                exr
            }
            Expression::SameTerm(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::SameTermLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::SameTermRight),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    if right_rewrite.expression.is_some()
                        && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    {
                        let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                        let right_expression_rewrite = right_rewrite.expression.take().unwrap();

                        exr.with_expression(Expression::SameTerm(
                            Box::new(left_expression_rewrite),
                            Box::new(right_expression_rewrite),
                        ))
                        .with_change_type(ChangeType::NoChange);
                        return exr;
                    }
                }
                exr
            }
            Expression::Greater(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::GreaterLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::GreaterRight),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    if right_rewrite.expression.is_some()
                        && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    {
                        let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                        let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                        exr.with_expression(Expression::Greater(
                            Box::new(left_expression_rewrite),
                            Box::new(right_expression_rewrite),
                        ))
                        .with_change_type(ChangeType::NoChange);
                        return exr;
                    }
                }
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                exr
            }
            Expression::GreaterOrEqual(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::GreaterOrEqualLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::GreaterOrEqualRight),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.expression.is_some()
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                    let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::GreaterOrEqual(
                        Box::new(left_expression_rewrite),
                        Box::new(right_expression_rewrite),
                    ))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                exr
            }
            Expression::Less(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::LessLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::LessLeft),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.expression.is_some()
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                    let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::Less(
                        Box::new(left_expression_rewrite),
                        Box::new(right_expression_rewrite),
                    ))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                exr
            }
            Expression::LessOrEqual(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::LessOrEqualLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::LessOrEqualLeft),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.expression.is_some()
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                    let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::LessOrEqual(
                        Box::new(left_expression_rewrite),
                        Box::new(right_expression_rewrite),
                    ))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                exr
            }
            Expression::In(left, expressions) => self.rewrite_in_expression(
                left,
                expressions,
                required_change_direction,
                variables_in_scope,
                context,
            ),
            Expression::Add(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::AddLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::AddRight),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.expression.is_some()
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                    let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::Add(
                        Box::new(left_expression_rewrite),
                        Box::new(right_expression_rewrite),
                    ))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                exr
            }
            Expression::Subtract(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::SubtractLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::SubtractRight),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.expression.is_some()
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                    let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::Subtract(
                        Box::new(left_expression_rewrite),
                        Box::new(right_expression_rewrite),
                    ))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                exr
            }
            Expression::Multiply(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::MultiplyLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::MultiplyRight),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.expression.is_some()
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                    let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::Multiply(
                        Box::new(left_expression_rewrite),
                        Box::new(right_expression_rewrite),
                    ))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                exr
            }
            Expression::Divide(left, right) => {
                let mut left_rewrite = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::DivideLeft),
                );
                let mut right_rewrite = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::DivideRight),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut left_rewrite)
                    .with_pushups(&mut right_rewrite);
                if left_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.expression.is_some()
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let left_expression_rewrite = left_rewrite.expression.take().unwrap();
                    let right_expression_rewrite = right_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::Divide(
                        Box::new(left_expression_rewrite),
                        Box::new(right_expression_rewrite),
                    ))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&left_rewrite, &right_rewrite], context);
                exr
            }
            Expression::UnaryPlus(wrapped) => {
                let mut wrapped_rewrite = self.rewrite_expression(
                    wrapped,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::UnaryPlus),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut wrapped_rewrite);
                if wrapped_rewrite.expression.is_some()
                    && wrapped_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let wrapped_expression_rewrite = wrapped_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::UnaryPlus(Box::new(
                        wrapped_expression_rewrite,
                    )))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&wrapped_rewrite], context);
                exr
            }
            Expression::UnaryMinus(wrapped) => {
                let mut wrapped_rewrite = self.rewrite_expression(
                    wrapped,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::UnaryMinus),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut wrapped_rewrite);
                if wrapped_rewrite.expression.is_some()
                    && wrapped_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let wrapped_expression_rewrite = wrapped_rewrite.expression.take().unwrap();
                    exr.with_expression(Expression::UnaryPlus(Box::new(
                        wrapped_expression_rewrite,
                    )))
                    .with_change_type(ChangeType::NoChange);
                    return exr;
                }
                self.project_all_static_variables(vec![&wrapped_rewrite], context);
                exr
            }
            Expression::Not(wrapped) => {
                let mut wrapped_rewrite = self.rewrite_expression(
                    wrapped,
                    &required_change_direction.opposite(),
                    variables_in_scope,
                    &context.extension_with(PathEntry::Not),
                );
                let mut exr = ExReturn::new();
                exr.with_pushups(&mut wrapped_rewrite);
                if wrapped_rewrite.expression.is_some() {
                    let wrapped_change = wrapped_rewrite.change_type.take().unwrap();
                    let use_change_type = match wrapped_change {
                        ChangeType::NoChange => ChangeType::NoChange,
                        ChangeType::Relaxed => ChangeType::Constrained,
                        ChangeType::Constrained => ChangeType::Relaxed,
                    };
                    if use_change_type == ChangeType::NoChange
                        || &use_change_type == required_change_direction
                    {
                        let wrapped_expression_rewrite = wrapped_rewrite.expression.take().unwrap();
                        exr.with_expression(Expression::Not(Box::new(wrapped_expression_rewrite)))
                            .with_change_type(use_change_type);
                        return exr;
                    }
                }
                self.project_all_static_variables(vec![&wrapped_rewrite], context);
                exr
            }
            Expression::Exists(wrapped) => {
                let wrapped_rewrite = self.rewrite_graph_pattern(
                    &wrapped,
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
            Expression::Bound(v) => {
                let mut exr = ExReturn::new();
                if let Some(v_rewritten) = self.rewrite_variable(v, context) {
                    exr.with_expression(Expression::Bound(v_rewritten))
                        .with_change_type(ChangeType::NoChange);
                }
                exr
            }
            Expression::If(left, mid, right) => {
                self.rewrite_if_expression(left, mid, right, variables_in_scope, context)
            }
            Expression::Coalesce(wrapped) => {
                let mut rewritten = wrapped
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        self.rewrite_expression(
                            e,
                            &ChangeType::NoChange,
                            variables_in_scope,
                            &context.extension_with(PathEntry::Coalesce(i as u16)),
                        )
                    })
                    .collect::<Vec<ExReturn>>();
                let mut exr = ExReturn::new();
                for e in rewritten.iter_mut() {
                    exr.with_pushups(e);
                }
                if rewritten.iter().all(|x| {
                    x.expression.is_some()
                        && x.change_type.as_ref().unwrap() == &ChangeType::NoChange
                }) {
                    {
                        exr.with_expression(Expression::Coalesce(
                            rewritten
                                .iter_mut()
                                .map(|x| x.expression.take().unwrap())
                                .collect(),
                        ))
                        .with_change_type(ChangeType::NoChange);
                        return exr;
                    }
                }
                self.project_all_static_variables(rewritten.iter().collect(), context);
                exr
            }
            Expression::FunctionCall(fun, args) => {
                self.rewrite_function_call_expression(fun, args, variables_in_scope, context)
            }
        }
    }
}

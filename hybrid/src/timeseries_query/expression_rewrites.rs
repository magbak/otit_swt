use super::TimeSeriesQuery;
use crate::change_types::ChangeType;
use crate::pushdown_setting::PushdownSetting;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TimeSeriesExpressionRewriteContext {
    Condition,
    Aggregate,
}

pub(crate) struct RecursiveRewriteReturn {
    pub expression: Option<Expression>,
    pub change_type: Option<ChangeType>,
    pub lost_value: bool,
}

impl RecursiveRewriteReturn {
    fn new(
        expression: Option<Expression>,
        change_type: Option<ChangeType>,
        lost_value: bool,
    ) -> RecursiveRewriteReturn {
        RecursiveRewriteReturn {
            expression,
            change_type,
            lost_value,
        }
    }
    fn none() -> RecursiveRewriteReturn {
        RecursiveRewriteReturn {
            expression: None,
            change_type: None,
            lost_value: false,
        }
    }
}

impl TimeSeriesQuery {
    pub(crate) fn rewrite_filter_expression(
        &self,
        expression: &Expression,
        context: &Context,
        pushdown_settings: &HashSet<PushdownSetting>,
    ) -> (Option<Expression>, bool) {
        let mut rewrite = self.try_recursive_rewrite_expression(
            &TimeSeriesExpressionRewriteContext::Condition,
            expression,
            &ChangeType::Relaxed,
            context,
            pushdown_settings,
        );
        return (rewrite.expression.take(), rewrite.lost_value);
    }

    pub(crate) fn try_recursive_rewrite_expression(
        &self,
        rewrite_context: &TimeSeriesExpressionRewriteContext,
        expression: &Expression,
        required_change_direction: &ChangeType,
        context: &Context,
        pushdown_settings: &HashSet<PushdownSetting>,
    ) -> RecursiveRewriteReturn {
        match &expression {
            Expression::Literal(lit) => {
                return RecursiveRewriteReturn::new(
                    Some(Expression::Literal(lit.clone())),
                    Some(ChangeType::NoChange),
                    false,
                );
            }
            Expression::Variable(v) => {
                if self.has_equivalent_timestamp_variable(v, context)
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Variable(v.clone())),
                        Some(ChangeType::NoChange),
                        false,
                    );
                } else if self.get_value_variables().into_iter().find(|x|&x.variable == v).is_some() {
                    if rewrite_context == &TimeSeriesExpressionRewriteContext::Aggregate
                        || pushdown_settings.contains(&PushdownSetting::ValueConditions)
                    {
                        return RecursiveRewriteReturn::new(
                            Some(Expression::Variable(v.clone())),
                            Some(ChangeType::NoChange),
                            false,
                        );
                    } else {
                        return RecursiveRewriteReturn::new(None, None, true);
                    }
                } else {
                    return RecursiveRewriteReturn::none();
                }
            }
            Expression::Or(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::OrLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::OrRight),
                    pushdown_settings,
                );
                match required_change_direction {
                    ChangeType::Relaxed => {
                        if left_rewrite.expression.is_some() && right_rewrite.expression.is_some() {
                            if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                                && right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(Expression::Or(
                                        Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                        Box::new(
                                            right_rewrite.expression.as_ref().unwrap().clone(),
                                        ),
                                    )),
                                    Some(ChangeType::NoChange),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            } else if (left_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::NoChange
                                || left_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Relaxed)
                                && (right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                                    || right_rewrite.change_type.as_ref().unwrap()
                                        == &ChangeType::Relaxed)
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(Expression::Or(
                                        Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                        Box::new(
                                            right_rewrite.expression.as_ref().unwrap().clone(),
                                        ),
                                    )),
                                    Some(ChangeType::Relaxed),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            }
                        }
                    }
                    ChangeType::Constrained => {
                        if left_rewrite.expression.is_some() && right_rewrite.expression.is_some() {
                            if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                                && right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(Expression::Or(
                                        Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                        Box::new(
                                            right_rewrite.expression.as_ref().unwrap().clone(),
                                        ),
                                    )),
                                    Some(ChangeType::NoChange),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            } else if (left_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::NoChange
                                || left_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Constrained)
                                && (right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                                    || right_rewrite.change_type.as_ref().unwrap()
                                        == &ChangeType::Constrained)
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(Expression::Or(
                                        Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                        Box::new(
                                            right_rewrite.expression.as_ref().unwrap().clone(),
                                        ),
                                    )),
                                    Some(ChangeType::Constrained),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            }
                        } else if left_rewrite.expression.is_none()
                            && right_rewrite.expression.is_some()
                        {
                            if right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                                || right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Constrained
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(right_rewrite.expression.as_ref().unwrap().clone()),
                                    Some(ChangeType::Constrained),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            }
                        } else if left_rewrite.expression.is_some()
                            && right_rewrite.expression.is_none()
                        {
                            if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                                || left_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Constrained
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Some(ChangeType::Constrained),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            }
                        }
                    }
                    ChangeType::NoChange => {
                        if left_rewrite.expression.is_some() && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange &&
                           right_rewrite.expression.is_some() && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::Or(
                                    Box::new(left_rewrite.expression.take().unwrap()),
                                    Box::new(right_rewrite.expression.take().unwrap()),
                                )),
                                Some(ChangeType::NoChange),
                                left_rewrite.lost_value || right_rewrite.lost_value,
                            );
                        }
                    }
                }
                RecursiveRewriteReturn::none()
            }
            Expression::And(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::AndLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::AndRight),
                    pushdown_settings,
                );
                match required_change_direction {
                    ChangeType::Constrained => {
                        if left_rewrite.expression.is_some() && right_rewrite.expression.is_some() {
                            if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                                && right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(Expression::And(
                                        Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                        Box::new(
                                            right_rewrite.expression.as_ref().unwrap().clone(),
                                        ),
                                    )),
                                    Some(ChangeType::NoChange),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            } else if (left_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::NoChange
                                || left_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Constrained)
                                && (right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                                    || right_rewrite.change_type.as_ref().unwrap()
                                        == &ChangeType::Constrained)
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(Expression::And(
                                        Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                        Box::new(
                                            right_rewrite.expression.as_ref().unwrap().clone(),
                                        ),
                                    )),
                                    Some(ChangeType::Constrained),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            }
                        }
                    }
                    ChangeType::Relaxed => {
                        if left_rewrite.expression.is_some() && right_rewrite.expression.is_some() {
                            if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                                && right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(Expression::And(
                                        Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                        Box::new(
                                            right_rewrite.expression.as_ref().unwrap().clone(),
                                        ),
                                    )),
                                    Some(ChangeType::NoChange),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            } else if (left_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::NoChange
                                || left_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Relaxed)
                                && (right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::NoChange
                                    || right_rewrite.change_type.as_ref().unwrap()
                                        == &ChangeType::Relaxed)
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(Expression::And(
                                        Box::new(left_rewrite.expression.as_ref().unwrap().clone()),
                                        Box::new(
                                            right_rewrite.expression.as_ref().unwrap().clone(),
                                        ),
                                    )),
                                    Some(ChangeType::Relaxed),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            }
                        } else if left_rewrite.expression.is_none()
                            && right_rewrite.expression.is_some()
                        {
                            if right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                                || right_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Relaxed
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(right_rewrite.expression.as_ref().unwrap().clone()),
                                    Some(ChangeType::Relaxed),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            }
                        } else if left_rewrite.expression.is_some()
                            && right_rewrite.expression.is_none()
                        {
                            if left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                                || left_rewrite.change_type.as_ref().unwrap()
                                    == &ChangeType::Relaxed
                            {
                                return RecursiveRewriteReturn::new(
                                    Some(left_rewrite.expression.as_ref().unwrap().clone()),
                                    Some(ChangeType::Relaxed),
                                    left_rewrite.lost_value || right_rewrite.lost_value,
                                );
                            }
                        }
                    }
                    ChangeType::NoChange => {
                        if left_rewrite.expression.is_some() && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange && right_rewrite.expression.is_some() &&
                        right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::And(
                                    Box::new(left_rewrite.expression.take().unwrap()),
                                    Box::new(right_rewrite.expression.take().unwrap()),
                                )),
                                Some(ChangeType::NoChange),
                                left_rewrite.lost_value || right_rewrite.lost_value,
                            );
                        }
                    }
                }
                RecursiveRewriteReturn::none()
            }
            Expression::Equal(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::EqualLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::EqualRight),
                    pushdown_settings,
                );
                if left_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Equal(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::Greater(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::GreaterLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::GreaterRight),
                    pushdown_settings,
                );
                if left_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Greater(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::GreaterOrEqual(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::GreaterOrEqualLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::GreaterOrEqualRight),
                    pushdown_settings,
                );
                if left_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::GreaterOrEqual(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::Less(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::LessLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::LessRight),
                    pushdown_settings,
                );
                if left_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Less(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::LessOrEqual(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::LessOrEqualLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::LessOrEqualRight),
                    pushdown_settings,
                );
                if left_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::LessOrEqual(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::In(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    &ChangeType::NoChange,
                    &context.extension_with(PathEntry::InLeft),
                    pushdown_settings,
                );
                if left_rewrite.change_type.as_ref().is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    let mut right_rewrites = right
                        .iter()
                        .enumerate()
                        .map(|(i, e)| {
                            self.try_recursive_rewrite_expression(
                                rewrite_context,
                                e,
                                required_change_direction,
                                &context.extension_with(PathEntry::InRight(i as u16)),
                                pushdown_settings,
                            )
                        })
                        .collect::<Vec<RecursiveRewriteReturn>>();
                    if right_rewrites.iter().all(|x| x.expression.is_some()) {
                        if right_rewrites
                            .iter()
                            .all(|x| x.change_type.as_ref().unwrap() == &ChangeType::NoChange)
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::In(
                                    Box::new(left_rewrite.expression.take().unwrap()),
                                    right_rewrites.iter_mut().map(|x|x.expression.take().unwrap()).collect(),
                                )),
                                Some(ChangeType::NoChange),
                                right_rewrites
                                    .iter()
                                    .fold(left_rewrite.lost_value, |acc, elem| {
                                        acc || elem.lost_value
                                    }),
                            );
                        }
                    } else if required_change_direction == &ChangeType::Constrained
                        && right_rewrites.iter().any(|x| x.expression.is_some())
                    {
                        let use_lost_value = right_rewrites
                            .iter()
                            .fold(left_rewrite.lost_value, |acc, elem| acc || elem.lost_value);
                        let right_rewrites = right_rewrites
                            .into_iter()
                            .filter(|x| x.expression.is_some())
                            .collect::<Vec<RecursiveRewriteReturn>>();
                        if right_rewrites
                            .iter()
                            .all(|x| x.change_type.as_ref().unwrap() == &ChangeType::NoChange)
                        {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::In(
                                    Box::new(left_rewrite.expression.take().unwrap()),
                                    right_rewrites
                                        .into_iter()
                                        .map(|mut x| x.expression.take().unwrap())
                                        .collect(),
                                )),
                                Some(ChangeType::Constrained),
                                use_lost_value,
                            );
                        }
                    }
                }
                RecursiveRewriteReturn::none()
            }
            Expression::Add(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::AddLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::AddRight),
                    pushdown_settings,
                );

                if left_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Add(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::Subtract(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::SubtractLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::SubtractRight),
                    pushdown_settings,
                );
                if left_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Subtract(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::Multiply(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::MultiplyLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::MultiplyRight),
                    pushdown_settings,
                );
                if left_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Multiply(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::Divide(left, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::DivideLeft),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::DivideRight),
                    pushdown_settings,
                );
                if left_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::Divide(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::UnaryPlus(inner) => {
                let mut inner_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    inner,
                    required_change_direction,
                    &context.extension_with(PathEntry::UnaryPlus),
                    pushdown_settings,
                );
                if inner_rewrite.change_type.is_some()
                    && inner_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::UnaryPlus(Box::new(
                            inner_rewrite.expression.take().unwrap(),
                        ))),
                        Some(ChangeType::NoChange),
                        inner_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::UnaryMinus(inner) => {
                let mut inner_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    inner,
                    required_change_direction,
                    &context.extension_with(PathEntry::UnaryMinus),
                    pushdown_settings,
                );
                if inner_rewrite.expression.is_some()
                    && inner_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::UnaryMinus(Box::new(
                            inner_rewrite.expression.take().unwrap(),
                        ))),
                        Some(ChangeType::NoChange),
                        inner_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::Not(inner) => {
                let use_direction = match required_change_direction {
                    ChangeType::Relaxed => ChangeType::Constrained,
                    ChangeType::Constrained => ChangeType::Relaxed,
                    ChangeType::NoChange => ChangeType::NoChange,
                };
                let mut inner_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    inner,
                    &use_direction,
                    &context.extension_with(PathEntry::Not),
                    pushdown_settings,
                );
                if inner_rewrite.expression.is_some() {
                    match inner_rewrite.change_type.as_ref().unwrap() {
                        ChangeType::Relaxed => {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::Not(Box::new(
                                    inner_rewrite.expression.take().unwrap(),
                                ))),
                                Some(ChangeType::Constrained),
                                inner_rewrite.lost_value,
                            );
                        }
                        ChangeType::Constrained => {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::Not(Box::new(
                                    inner_rewrite.expression.take().unwrap(),
                                ))),
                                Some(ChangeType::Relaxed),
                                inner_rewrite.lost_value,
                            );
                        }
                        ChangeType::NoChange => {
                            return RecursiveRewriteReturn::new(
                                Some(Expression::Not(Box::new(
                                    inner_rewrite.expression.take().unwrap(),
                                ))),
                                Some(ChangeType::NoChange),
                                inner_rewrite.lost_value,
                            );
                        }
                    }
                }
                RecursiveRewriteReturn::none()
            }
            Expression::If(left, middle, right) => {
                let mut left_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::IfLeft),
                    pushdown_settings,
                );
                let mut middle_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    middle,
                    required_change_direction,
                    &context.extension_with(PathEntry::IfMiddle),
                    pushdown_settings,
                );
                let mut right_rewrite = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::IfRight),
                    pushdown_settings,
                );
                if left_rewrite.expression.is_some()
                    && middle_rewrite.expression.is_some()
                    && right_rewrite.expression.is_some()
                    && left_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && middle_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                    && right_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                {
                    return RecursiveRewriteReturn::new(
                        Some(Expression::If(
                            Box::new(left_rewrite.expression.take().unwrap()),
                            Box::new(middle_rewrite.expression.take().unwrap()),
                            Box::new(right_rewrite.expression.take().unwrap()),
                        )),
                        Some(ChangeType::NoChange),
                        left_rewrite.lost_value
                            || middle_rewrite.lost_value
                            || right_rewrite.lost_value,
                    );
                }
                RecursiveRewriteReturn::none()
            }
            Expression::Coalesce(inner) => {
                let inner_rewrites = inner
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        self.try_recursive_rewrite_expression(
                            rewrite_context,
                            e,
                            required_change_direction,
                            &context.extension_with(PathEntry::Coalesce(i as u16)),
                            pushdown_settings,
                        )
                    })
                    .collect::<Vec<RecursiveRewriteReturn>>();
                if inner_rewrites.iter().all(|x| x.expression.is_some()) {
                    if inner_rewrites
                        .iter()
                        .all(|x| x.change_type.as_ref().unwrap() == &ChangeType::NoChange)
                    {
                        let use_lost_value = inner_rewrites.iter().fold(false, |b, x| b || x.lost_value);
                        return RecursiveRewriteReturn::new(
                            Some(Expression::Coalesce(
                                inner_rewrites.into_iter().map(|mut x|x.expression.take().unwrap()).collect(),
                            )),
                            Some(ChangeType::NoChange),
                            use_lost_value,
                        );
                    }
                }
                RecursiveRewriteReturn::none()
            }
            Expression::FunctionCall(left, right) => {
                let right_rewrites = right
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        self.try_recursive_rewrite_expression(
                            rewrite_context,
                            e,
                            required_change_direction,
                            &context.extension_with(PathEntry::FunctionCall(i as u16)),
                            pushdown_settings,
                        )
                    })
                    .collect::<Vec<RecursiveRewriteReturn>>();
                if right_rewrites.iter().all(|x| x.expression.is_some()) {
                    if right_rewrites
                        .iter()
                        .all(|x| x.change_type.as_ref().unwrap() == &ChangeType::NoChange)
                    {
                        let use_lost_value =
                            right_rewrites.iter().fold(false, |b, x| b || x.lost_value);
                        return RecursiveRewriteReturn::new(
                            Some(Expression::FunctionCall(
                                left.clone(),
                                right_rewrites
                                    .into_iter()
                                    .map(|mut x| x.expression.take().unwrap())
                                    .collect(),
                            )),
                            Some(ChangeType::NoChange),
                            use_lost_value,
                        );
                    }
                }
                RecursiveRewriteReturn::none()
            }
            _ => RecursiveRewriteReturn::none(),
        }
    }
}

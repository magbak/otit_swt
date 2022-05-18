use crate::change_types::ChangeType;
use spargebra::algebra::{AggregateExpression, Expression};
use spargebra::term::Variable;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Grouping {
    interval: Duration,
    aggregations: Vec<(Variable, AggregateExpression)>,
}

#[derive(Debug, Clone)]
pub struct TimeSeriesQuery {
    pub identifier_variable: Option<Variable>,
    pub timeseries_variable: Option<Variable>,
    pub data_point_variable: Option<Variable>,
    pub value_variable: Option<Variable>,
    pub timestamp_variable: Option<Variable>,
    pub ids: Option<Vec<String>>,
    pub grouping: Option<Grouping>,
    pub conditions: Vec<Expression>,
}

impl TimeSeriesQuery {
    pub(crate) fn try_pushdown_aggregates(
        &self,
        variables: &Vec<Variable>,
        aggregations: &Vec<(Variable, AggregateExpression)>,
    ) {
            for (v, a) in aggregations {
                match a {
                    AggregateExpression::Count { expr, distinct } => {
                        if let Some(inner_expr) = expr {
                            if let Some((expr_rewrite_opt,_)) = self.try_recursive_rewrite_expression(inner_expr, &ChangeType::NoChange) {
                                todo!();
                            }
                        }
                    }
                    AggregateExpression::Sum { expr, distinct } => {}
                    AggregateExpression::Avg { expr, distinct } => {}
                    AggregateExpression::Min { expr, distinct } => {}
                    AggregateExpression::Max { expr, distinct } => {}
                    AggregateExpression::GroupConcat { expr, distinct, separator } => {}
                    AggregateExpression::Sample { expr, distinct } => {}
                    AggregateExpression::Custom { name, expr, distinct } => {}
                }
            }
    }

    pub(crate) fn try_rewrite_expression(&mut self, expr: &Expression) {
        if let Some((expression_rewrite, _)) =
            self.try_recursive_rewrite_expression(expr, &ChangeType::Relaxed)
        {
            self.conditions.push(expression_rewrite);
        }
    }

    fn try_recursive_rewrite_expression(
        &self,
        expression: &Expression,
        required_change_direction: &ChangeType,
    ) -> Option<(Expression, ChangeType)> {
        match expression {
            Expression::Literal(lit) => {
                return Some((Expression::Literal(lit.clone()), ChangeType::NoChange));
            }
            Expression::Variable(v) => {
                if (self.timeseries_variable.is_some() && self.timeseries_variable.as_ref().unwrap() == v)
                    || (self.value_variable.is_some() && self.value_variable.as_ref().unwrap() == v)
                {
                    return Some((Expression::Variable(v.clone()), ChangeType::NoChange));
                } else {
                    return None;
                }
            }
            Expression::Or(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                match required_change_direction {
                    ChangeType::Relaxed => {
                        if let (
                            Some((left_rewrite, left_change)),
                            Some((right_rewrite, right_change)),
                        ) = (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if left_change == &ChangeType::NoChange
                                && right_change == &ChangeType::NoChange
                            {
                                return Some((
                                    Expression::Or(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::NoChange,
                                ));
                            } else if (left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Relaxed)
                                && (right_change == &ChangeType::NoChange
                                    || right_change == &ChangeType::Relaxed)
                            {
                                return Some((
                                    Expression::Or(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::Relaxed,
                                ));
                            }
                        }
                    }
                    ChangeType::Constrained => {
                        if let (
                            Some((left_rewrite, left_change)),
                            Some((right_rewrite, right_change)),
                        ) = (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if left_change == &ChangeType::NoChange
                                && right_change == &ChangeType::NoChange
                            {
                                return Some((
                                    Expression::Or(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::NoChange,
                                ));
                            } else if (left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Constrained)
                                && (right_change == &ChangeType::NoChange
                                    || right_change == &ChangeType::Constrained)
                            {
                                return Some((
                                    Expression::Or(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::Constrained,
                                ));
                            }
                        } else if let (None, Some((right_rewrite, right_change))) =
                            (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if right_change == &ChangeType::NoChange
                                || right_change == &ChangeType::Constrained
                            {
                                return Some((right_rewrite.clone(), ChangeType::Constrained));
                            }
                        } else if let (Some((left_rewrite, left_change)), None) =
                            (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Constrained
                            {
                                return Some((left_rewrite.clone(), ChangeType::Constrained));
                            }
                        }
                    }
                    ChangeType::NoChange => {
                        if let (
                            Some((left_rewrite, ChangeType::NoChange)),
                            Some((right_rewrite, ChangeType::NoChange)),
                        ) = (left_rewrite_opt, right_rewrite_opt)
                        {
                            return Some((
                                Expression::Or(Box::new(left_rewrite), Box::new(right_rewrite)),
                                ChangeType::NoChange,
                            ));
                        }
                    }
                }
                None
            }
            Expression::And(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                match required_change_direction {
                    ChangeType::Constrained => {
                        if let (
                            Some((left_rewrite, left_change)),
                            Some((right_rewrite, right_change)),
                        ) = (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if left_change == &ChangeType::NoChange
                                && right_change == &ChangeType::NoChange
                            {
                                return Some((
                                    Expression::And(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::NoChange,
                                ));
                            } else if (left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Constrained)
                                && (right_change == &ChangeType::NoChange
                                    || right_change == &ChangeType::Constrained)
                            {
                                return Some((
                                    Expression::And(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::Constrained,
                                ));
                            }
                        }
                    }
                    ChangeType::Relaxed => {
                        if let (
                            Some((left_rewrite, left_change)),
                            Some((right_rewrite, right_change)),
                        ) = (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if left_change == &ChangeType::NoChange
                                && right_change == &ChangeType::NoChange
                            {
                                return Some((
                                    Expression::And(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::NoChange,
                                ));
                            } else if (left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Relaxed)
                                && (right_change == &ChangeType::NoChange
                                    || right_change == &ChangeType::Relaxed)
                            {
                                return Some((
                                    Expression::And(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::Relaxed,
                                ));
                            }
                        } else if let (None, Some((right_rewrite, right_change))) =
                            (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if right_change == &ChangeType::NoChange
                                || right_change == &ChangeType::Relaxed
                            {
                                return Some((right_rewrite.clone(), ChangeType::Relaxed));
                            }
                        } else if let (Some((left_rewrite, left_change)), None) =
                            (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Relaxed
                            {
                                return Some((left_rewrite.clone(), ChangeType::Relaxed));
                            }
                        }
                    }
                    ChangeType::NoChange => {
                        if let (
                            Some((left_rewrite, ChangeType::NoChange)),
                            Some((right_rewrite, ChangeType::NoChange)),
                        ) = (left_rewrite_opt, right_rewrite_opt)
                        {
                            return Some((
                                Expression::And(Box::new(left_rewrite), Box::new(right_rewrite)),
                                ChangeType::NoChange,
                            ));
                        }
                    }
                }
                None
            }
            Expression::Equal(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Equal(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::Greater(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Greater(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::GreaterOrEqual(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::Less(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Less(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::LessOrEqual(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::LessOrEqual(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::In(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, &ChangeType::NoChange);
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    let right_rewrites_opt = right
                        .iter()
                        .map(|e| self.try_recursive_rewrite_expression(e, required_change_direction)).collect::<Vec<Option<(Expression, ChangeType)>>>();
                    if right_rewrites_opt.iter().all(|x| x.is_some()) {
                        let right_rewrites = right_rewrites_opt
                            .into_iter()
                            .map(|x| x.unwrap())
                            .collect::<Vec<(Expression, ChangeType)>>();
                        if right_rewrites
                            .iter()
                            .all(|(_, c)| c == &ChangeType::NoChange)
                        {
                            return Some((
                                Expression::In(
                                    Box::new(left_rewrite),
                                    right_rewrites.into_iter().map(|(e, _)| e).collect(),
                                ),
                                ChangeType::NoChange,
                            ));
                        }
                    }
                    else if required_change_direction == &ChangeType::Constrained && right_rewrites_opt.iter().any(|x| x.is_some()) {
                        let right_rewrites = right_rewrites_opt
                            .into_iter().filter(|x|x.is_some())
                            .map(|x| x.unwrap())
                            .collect::<Vec<(Expression, ChangeType)>>();
                        if right_rewrites
                            .iter()
                            .all(|(_, c)| c == &ChangeType::NoChange)
                        {
                            return Some((
                                Expression::In(
                                    Box::new(left_rewrite),
                                    right_rewrites.into_iter().map(|(e, _)| e).collect(),
                                ),
                                ChangeType::Constrained,
                            ));
                        }
                    }
                }
                None
            }
            Expression::Add(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Add(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::Subtract(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Subtract(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::Multiply(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Multiply(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::Divide(left, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Divide(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::UnaryPlus(inner) => {
                let inner_rewrite_opt =
                    self.try_recursive_rewrite_expression(inner, required_change_direction);
                if let Some((inner_rewrite, ChangeType::NoChange)) = inner_rewrite_opt {
                    return Some((
                        Expression::UnaryPlus(Box::new(inner_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::UnaryMinus(inner) => {
                let inner_rewrite_opt =
                    self.try_recursive_rewrite_expression(inner, required_change_direction);
                if let Some((inner_rewrite, ChangeType::NoChange)) = inner_rewrite_opt {
                    return Some((
                        Expression::UnaryMinus(Box::new(inner_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::Not(inner) => {
                let use_direction = match required_change_direction {
                    ChangeType::Relaxed => ChangeType::Constrained,
                    ChangeType::Constrained => ChangeType::Relaxed,
                    ChangeType::NoChange => ChangeType::NoChange,
                };
                let inner_rewrite_opt =
                    self.try_recursive_rewrite_expression(inner, &use_direction);
                if let Some((inner_rewrite, inner_change)) = inner_rewrite_opt {
                    match inner_change {
                        ChangeType::Relaxed => {
                            return Some((
                                Expression::Not(Box::new(inner_rewrite)),
                                ChangeType::Constrained,
                            ));
                        }
                        ChangeType::Constrained => {
                            return Some((
                                Expression::Not(Box::new(inner_rewrite)),
                                ChangeType::Relaxed,
                            ));
                        }
                        ChangeType::NoChange => {
                            return Some((
                                Expression::Not(Box::new(inner_rewrite)),
                                ChangeType::NoChange,
                            ));
                        }
                    }
                }
                None
            }
            Expression::If(left, middle, right) => {
                let left_rewrite_opt =
                    self.try_recursive_rewrite_expression(left, required_change_direction);
                let middle_rewrite_opt =
                    self.try_recursive_rewrite_expression(middle, required_change_direction);
                let right_rewrite_opt =
                    self.try_recursive_rewrite_expression(right, required_change_direction);
                if let (Some((left_rewrite, ChangeType::NoChange)), Some((middle_rewrite, ChangeType::NoChange)), Some((right_rewrite, ChangeType::NoChange))) = (left_rewrite_opt, middle_rewrite_opt, right_rewrite_opt) {
                    return Some((Expression::If(Box::new(left_rewrite), Box::new(middle_rewrite), Box::new(right_rewrite)), ChangeType::NoChange));
                }
                None
            }
            Expression::Coalesce(inner) => {
                let inner_rewrites_opt = inner
                    .iter()
                    .map(|e| self.try_recursive_rewrite_expression(e, required_change_direction))
                    .collect::<Vec<Option<(Expression, ChangeType)>>>();
                if inner_rewrites_opt.iter().all(|x| x.is_some()) {
                    let inner_rewrites = inner_rewrites_opt
                        .into_iter()
                        .map(|x| x.unwrap())
                        .collect::<Vec<(Expression, ChangeType)>>();
                    if inner_rewrites
                        .iter()
                        .all(|(_, c)| c == &ChangeType::NoChange)
                    {
                        return Some((
                            Expression::Coalesce(
                                inner_rewrites.into_iter().map(|(e, _)| e).collect(),
                            ),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::FunctionCall(left, right) => {
                let right_rewrites_opt = right
                    .iter()
                    .map(|e| self.try_recursive_rewrite_expression(e, required_change_direction))
                    .collect::<Vec<Option<(Expression, ChangeType)>>>();
                if right_rewrites_opt.iter().all(|x| x.is_some()) {
                    let right_rewrites = right_rewrites_opt
                        .into_iter()
                        .map(|x| x.unwrap())
                        .collect::<Vec<(Expression, ChangeType)>>();
                    if right_rewrites
                        .iter()
                        .all(|(_, c)| c == &ChangeType::NoChange)
                    {
                        return Some((
                            Expression::FunctionCall(
                                left.clone(),
                                right_rewrites.into_iter().map(|(e, _)| e).collect(),
                            ),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            _ => None,
        }
    }
}

impl TimeSeriesQuery {
    pub fn new() -> TimeSeriesQuery {
        TimeSeriesQuery {
            identifier_variable: None,
            timeseries_variable: None,
            data_point_variable: None,
            value_variable: None,
            timestamp_variable: None,
            ids: None,
            grouping: None,
            conditions: vec![],
        }
    }
}

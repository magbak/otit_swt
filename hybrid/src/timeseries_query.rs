use crate::change_types::ChangeType;
use crate::pushdown_setting::PushdownSetting;
use crate::query_context::{
    AggregateExpressionInContext, Context, ExpressionInContext, PathEntry, VariableInContext,
};
use crate::rewriting::hash_graph_pattern;
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern};
use spargebra::term::Variable;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TimeSeriesExpressionRewriteContext {
    Condition,
    Aggregate,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Grouping {
    pub graph_pattern_hash: u64,
    pub by: Vec<Variable>,
    pub aggregations: Vec<(Variable, AggregateExpressionInContext)>,
    pub timeseries_funcs: Vec<(Variable, ExpressionInContext)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimeSeriesQuery {
    pub pushdown_settings: HashSet<PushdownSetting>,
    pub dropped_value_expression: bool, //Used to hinder pushdown when a value filter is dropped
    pub identifier_variable: Option<Variable>,
    pub timeseries_variable: Option<VariableInContext>,
    pub data_point_variable: Option<VariableInContext>,
    pub value_variable: Option<VariableInContext>,
    pub datatype_variable: Option<Variable>,
    pub datatype: Option<NamedNode>,
    pub timestamp_variable: Option<VariableInContext>,
    pub ids: Option<Vec<String>>,
    pub grouping: Option<Grouping>,
    pub conditions: Vec<ExpressionInContext>,
}

#[derive(Debug)]
pub struct TimeSeriesValidationError {
    missing_columns: Vec<String>,
    extra_columns: Vec<String>,
}

impl Display for TimeSeriesValidationError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Missing columns: {}, Extra columns: {}",
            &self.missing_columns.join(","),
            &self.extra_columns.join(",")
        )
    }
}

impl Error for TimeSeriesValidationError {}

impl TimeSeriesQuery {
    pub(crate) fn validate(&self, df: &DataFrame) -> Result<(), TimeSeriesValidationError> {
        let mut expected_columns = HashSet::new();
        expected_columns.insert(self.identifier_variable.as_ref().unwrap().as_str());
        if let Some(grouping) = &self.grouping {
            for v in &grouping.by {
                expected_columns.insert(v.as_str());
            }
            for (v, _) in &grouping.aggregations {
                expected_columns.insert(v.as_str());
            }
        } else {
            if let Some(vv) = &self.value_variable {
                expected_columns.insert(vv.variable.as_str());
            }
            if let Some(tsv) = &self.timestamp_variable {
                expected_columns.insert(tsv.variable.as_str());
            }
        }

        let df_columns: HashSet<&str> = df.get_column_names().into_iter().collect();
        if expected_columns != df_columns {
            let err = TimeSeriesValidationError {
                missing_columns: expected_columns
                    .difference(&df_columns)
                    .map(|x| x.to_string())
                    .collect(),
                extra_columns: df_columns
                    .difference(&expected_columns)
                    .map(|x| x.to_string())
                    .collect(),
            };
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl TimeSeriesQuery {
    pub(crate) fn try_pushdown_aggregates(
        &mut self,
        aggregations: &Vec<(Variable, AggregateExpression)>,
        group_graph_pattern: &GraphPattern,
        timeseries_funcs: Vec<(Variable, ExpressionInContext)>,
        by: Vec<Variable>,
        context: &Context,
    ) {
        let rewrite_context = TimeSeriesExpressionRewriteContext::Aggregate;
        let mut keep_aggregates = vec![];
        for (v, a) in aggregations {
            let mut keep_aggregate = None;
            match a {
                AggregateExpression::Count { expr, distinct } => {
                    if let Some(inner_expr) = expr {
                        if let Some((expr_rewrite, _)) = self.try_recursive_rewrite_expression(
                            &rewrite_context,
                            inner_expr,
                            &ChangeType::NoChange,
                            &context.extension_with(PathEntry::AggregationOperation),
                        ) {
                            keep_aggregate = Some(AggregateExpression::Count {
                                expr: Some(Box::new(expr_rewrite)),
                                distinct: distinct.clone(),
                            });
                        }
                    }
                }
                AggregateExpression::Sum { expr, distinct } => {
                    if let Some((expr_rewrite, _)) = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                    ) {
                        keep_aggregate = Some(AggregateExpression::Sum {
                            expr: Box::new(expr_rewrite),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::Avg { expr, distinct } => {
                    if let Some((expr_rewrite, _)) = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                    ) {
                        keep_aggregate = Some(AggregateExpression::Avg {
                            expr: Box::new(expr_rewrite),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::Min { expr, distinct } => {
                    if let Some((expr_rewrite, _)) = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                    ) {
                        keep_aggregate = Some(AggregateExpression::Min {
                            expr: Box::new(expr_rewrite),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::Max { expr, distinct } => {
                    if let Some((expr_rewrite, _)) = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                    ) {
                        keep_aggregate = Some(AggregateExpression::Max {
                            expr: Box::new(expr_rewrite),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::GroupConcat {
                    expr,
                    distinct,
                    separator,
                } => {
                    if let Some((expr_rewrite, _)) = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                    ) {
                        keep_aggregate = Some(AggregateExpression::GroupConcat {
                            expr: Box::new(expr_rewrite),
                            distinct: distinct.clone(),
                            separator: separator.clone(),
                        });
                    }
                }
                AggregateExpression::Sample { expr, distinct } => {
                    if let Some((expr_rewrite, _)) = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                    ) {
                        keep_aggregate = Some(AggregateExpression::Sample {
                            expr: Box::new(expr_rewrite),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::Custom {
                    name,
                    expr,
                    distinct,
                } => {
                    if let Some((expr_rewrite, _)) = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                    ) {
                        keep_aggregate = Some(AggregateExpression::Custom {
                            name: name.clone(),
                            expr: Box::new(expr_rewrite),
                            distinct: distinct.clone(),
                        });
                    }
                }
            }
            if let Some(agg) = keep_aggregate {
                keep_aggregates.push((v.clone(), agg));
            }
        }
        if keep_aggregates.len() == aggregations.len() {
            self.grouping = Some(Grouping {
                graph_pattern_hash: hash_graph_pattern(group_graph_pattern),
                by,
                aggregations: keep_aggregates
                    .into_iter()
                    .map(|(v, a)| (v, AggregateExpressionInContext::new(a, context.clone())))
                    .collect(),
                timeseries_funcs,
            });
        }
    }

    pub(crate) fn try_rewrite_condition_expression(
        &mut self,
        expr: &Expression,
        context: &Context,
    ) {
        if let Some((expression_rewrite, _)) = self.try_recursive_rewrite_expression(
            &TimeSeriesExpressionRewriteContext::Condition,
            expr,
            &ChangeType::Relaxed,
            context,
        ) {
            self.conditions.push(ExpressionInContext::new(
                expression_rewrite,
                context.clone(),
            ));
        }
    }

    fn try_recursive_rewrite_expression(
        &mut self,
        rewrite_context: &TimeSeriesExpressionRewriteContext,
        expression: &Expression,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<(Expression, ChangeType)> {
        match expression {
            Expression::Literal(lit) => {
                return Some((Expression::Literal(lit.clone()), ChangeType::NoChange));
            }
            Expression::Variable(v) => {
                if self.timestamp_variable.is_some()
                    && self
                        .timestamp_variable
                        .as_ref()
                        .unwrap()
                        .equivalent(v, context)
                {
                    return Some((Expression::Variable(v.clone()), ChangeType::NoChange));
                } else if self.value_variable.is_some()
                    && self.value_variable.as_ref().unwrap().equivalent(v, context)
                {
                    if rewrite_context == &TimeSeriesExpressionRewriteContext::Aggregate
                        || self
                            .pushdown_settings
                            .contains(&PushdownSetting::ValueConditions)
                    {
                        return Some((Expression::Variable(v.clone()), ChangeType::NoChange));
                    } else {
                        self.dropped_value_expression = true;
                        return None;
                    }
                } else {
                    return None;
                }
            }
            Expression::Or(left, right) => {
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::OrLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::OrRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::AndLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::AndRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::EqualLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::EqualRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::GreaterLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::GreaterRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::GreaterOrEqualLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::GreaterOrEqualRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::LessLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::LessRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::LessOrEqualLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::LessOrEqualRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    &ChangeType::NoChange,
                    &context.extension_with(PathEntry::InLeft),
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    let right_rewrites_opt = right
                        .iter()
                        .enumerate()
                        .map(|(i, e)| {
                            self.try_recursive_rewrite_expression(
                                rewrite_context,
                                e,
                                required_change_direction,
                                &context.extension_with(PathEntry::InRight(i as u16)),
                            )
                        })
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
                                Expression::In(
                                    Box::new(left_rewrite),
                                    right_rewrites.into_iter().map(|(e, _)| e).collect(),
                                ),
                                ChangeType::NoChange,
                            ));
                        }
                    } else if required_change_direction == &ChangeType::Constrained
                        && right_rewrites_opt.iter().any(|x| x.is_some())
                    {
                        let right_rewrites = right_rewrites_opt
                            .into_iter()
                            .filter(|x| x.is_some())
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::AddLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::AddRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::SubtractLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::SubtractRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::MultiplyLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::MultiplyRight),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::DivideLeft),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::DivideRight),
                );
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
                let inner_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    inner,
                    required_change_direction,
                    &context.extension_with(PathEntry::UnaryPlus),
                );
                if let Some((inner_rewrite, ChangeType::NoChange)) = inner_rewrite_opt {
                    return Some((
                        Expression::UnaryPlus(Box::new(inner_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::UnaryMinus(inner) => {
                let inner_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    inner,
                    required_change_direction,
                    &context.extension_with(PathEntry::UnaryMinus),
                );
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
                let inner_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    inner,
                    &use_direction,
                    &context.extension_with(PathEntry::Not),
                );
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
                let left_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    left,
                    required_change_direction,
                    &context.extension_with(PathEntry::IfLeft),
                );
                let middle_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    middle,
                    required_change_direction,
                    &context.extension_with(PathEntry::IfMiddle),
                );
                let right_rewrite_opt = self.try_recursive_rewrite_expression(
                    rewrite_context,
                    right,
                    required_change_direction,
                    &context.extension_with(PathEntry::IfRight),
                );
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((middle_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, middle_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::If(
                            Box::new(left_rewrite),
                            Box::new(middle_rewrite),
                            Box::new(right_rewrite),
                        ),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::Coalesce(inner) => {
                let inner_rewrites_opt = inner
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        self.try_recursive_rewrite_expression(
                            rewrite_context,
                            e,
                            required_change_direction,
                            &context.extension_with(PathEntry::Coalesce(i as u16)),
                        )
                    })
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
                    .enumerate()
                    .map(|(i, e)| {
                        self.try_recursive_rewrite_expression(
                            rewrite_context,
                            e,
                            required_change_direction,
                            &context.extension_with(PathEntry::FunctionCall(i as u16)),
                        )
                    })
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
    pub fn new_empty(pushdown_settings: HashSet<PushdownSetting>) -> TimeSeriesQuery {
        TimeSeriesQuery {
            pushdown_settings,
            dropped_value_expression: false,
            identifier_variable: None,
            timeseries_variable: None,
            data_point_variable: None,
            value_variable: None,
            datatype_variable: None,
            datatype: None,
            timestamp_variable: None,
            ids: None,
            grouping: None,
            conditions: vec![],
        }
    }
}

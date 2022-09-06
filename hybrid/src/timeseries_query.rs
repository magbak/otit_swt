pub(crate) mod expression_rewrites;

use crate::change_types::ChangeType;
use crate::pushdown_setting::PushdownSetting;
use crate::query_context::{
    AggregateExpressionInContext, Context, ExpressionInContext, PathEntry, VariableInContext,
};
use crate::rewriting::hash_graph_pattern;
use crate::timeseries_query::expression_rewrites::TimeSeriesExpressionRewriteContext;
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern};
use spargebra::term::Variable;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum TimeSeriesQuery {
    Basic(BasicTimeSeriesQuery),
    Filtered(Box<TimeSeriesQuery>, Expression), //Flag lets us know if filtering is complete.
    InnerSynchronized(Vec<Box<TimeSeriesQuery>>, Vec<Synchronizer>),
    LeftSynchronized(
        Box<TimeSeriesQuery>,
        Box<TimeSeriesQuery>,
        Vec<Synchronizer>,
        Expression,
        bool,
    ), //Left, Right, Filter, complete
    Grouped(GroupedTimeSeriesQuery),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Synchronizer {
    Identity(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupedTimeSeriesQuery {
    pub tsq: Box<TimeSeriesQuery>,
    pub graph_pattern_context: Context,
    pub by: Vec<Variable>,
    pub aggregations: Vec<(Variable, AggregateExpressionInContext)>,
    pub timeseries_funcs: Vec<(Variable, ExpressionInContext)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicTimeSeriesQuery {
    pub identifier_variable: Option<Variable>,
    pub timeseries_variable: Option<VariableInContext>,
    pub data_point_variable: Option<VariableInContext>,
    pub value_variable: Option<VariableInContext>,
    pub datatype_variable: Option<Variable>,
    pub datatype: Option<NamedNode>,
    pub timestamp_variable: Option<VariableInContext>,
    pub ids: Option<Vec<String>>,
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
        let expected_columns = self.expected_columns();
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

    fn expected_columns<'a>(&'a self) -> HashSet<&'a str> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                let mut expected_columns = HashSet::new();
                expected_columns.insert(b.identifier_variable.as_ref().unwrap().as_str());
                if let Some(vv) = &b.value_variable {
                    expected_columns.insert(vv.variable.as_str());
                }
                if let Some(tsv) = &b.timestamp_variable {
                    expected_columns.insert(tsv.variable.as_str());
                }
                expected_columns
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.expected_columns(),
            TimeSeriesQuery::InnerSynchronized(inners, _synchronizers) => {
                inners.iter().fold(HashSet::new(), |mut exp, tsq| {
                    exp.extend(tsq.expected_columns());
                    exp
                })
            }
            TimeSeriesQuery::LeftSynchronized(left, right, _, _, _) => {
                let mut expected_columns = left.expected_columns();
                expected_columns.extend(right.expected_columns());
                expected_columns
            }
            TimeSeriesQuery::Grouped(g) => {
                let mut expected_columns = HashSet::new();
                for v in &g.by {
                    expected_columns.insert(v.as_str());
                }
                for (v, _) in &g.aggregations {
                    expected_columns.insert(v.as_str());
                }
                expected_columns
            }
        }
    }

    pub(crate) fn get_mut_basic_queries(&mut self) -> Vec<&mut BasicTimeSeriesQuery> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                vec![b]
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_mut_basic_queries(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut basics = vec![];
                for inner in inners {
                    basics.extend(inner.get_mut_basic_queries())
                }
                basics
            }
            TimeSeriesQuery::LeftSynchronized(left, right, _, _, _) => {
                let mut basics = left.get_mut_basic_queries();
                basics.extend(right.get_mut_basic_queries());
                basics
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_mut_basic_queries(),
        }
    }

    pub(crate) fn has_equivalent_value_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for value_variable in self.get_value_variables() {
            if value_variable.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn has_equivalent_data_point_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for data_point_variable in self.get_data_point_variables() {
            if data_point_variable.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn has_equivalent_timeseries_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for timeseries_variable in self.get_timeseries_variables() {
            if timeseries_variable.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_ids(&self) -> Vec<&String> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_ids(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut ss = vec![];
                for inner in inners {
                    ss.extend(inner.get_ids())
                }
                ss
            }
            TimeSeriesQuery::LeftSynchronized(left, right, _, _, _) => {
                let mut ss = left.get_ids();
                ss.extend(right.get_ids());
                ss
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_ids(),
        }
    }

    pub(crate) fn get_data_point_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(id_var) = &b.data_point_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_data_point_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_data_point_variables())
                }
                vs
            }
            TimeSeriesQuery::LeftSynchronized(left, right, _, _, _) => {
                let mut vs = left.get_data_point_variables();
                vs.extend(right.get_data_point_variables());
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_data_point_variables(),
        }
    }

    pub(crate) fn get_timeseries_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(var) = &b.timeseries_variable {
                    vec![var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_timeseries_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_timeseries_variables())
                }
                vs
            }
            TimeSeriesQuery::LeftSynchronized(left, right, _, _, _) => {
                let mut vs = left.get_timeseries_variables();
                vs.extend(right.get_timeseries_variables());
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_timeseries_variables(),
        }
    }

    pub(crate) fn get_value_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(val_var) = &b.value_variable {
                    vec![val_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_value_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_value_variables())
                }
                vs
            }
            TimeSeriesQuery::LeftSynchronized(left, right, _, _, _) => {
                let mut vs = left.get_value_variables();
                vs.extend(right.get_value_variables());
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_value_variables(),
        }
    }

    pub(crate) fn get_identifier_variables(&self) -> Vec<&Variable> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(id_var) = &b.identifier_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_identifier_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_identifier_variables())
                }
                vs
            }
            TimeSeriesQuery::LeftSynchronized(left, right, _, _, _) => {
                let mut vs = left.get_identifier_variables();
                vs.extend(right.get_identifier_variables());
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_identifier_variables(),
        }
    }

    pub(crate) fn has_equivalent_timestamp_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for ts in self.get_timestamp_variables() {
            if ts.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_timestamp_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(v) = &b.timestamp_variable {
                    vec![v]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(t, _) => t.get_timestamp_variables(),
            TimeSeriesQuery::InnerSynchronized(ts, _) => {
                let mut vs = vec![];
                for t in ts {
                    vs.extend(t.get_timestamp_variables())
                }
                vs
            }
            TimeSeriesQuery::LeftSynchronized(l, r, _, _, _) => {
                let mut vs = l.get_timestamp_variables();
                vs.extend(r.get_timestamp_variables());
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_timestamp_variables(),
        }
    }
}

impl TimeSeriesQuery {
    pub(crate) fn try_pushdown_aggregates(
        &self,
        aggregations: &Vec<(Variable, AggregateExpression)>,
        group_graph_pattern: &GraphPattern,
        timeseries_funcs: Vec<(Variable, ExpressionInContext)>,
        by: Vec<Variable>,
        context: &Context,
        pushdown_settings: &HashSet<PushdownSetting>,
    ) -> Option<TimeSeriesQuery> {
        let rewrite_context = TimeSeriesExpressionRewriteContext::Aggregate;
        let mut keep_aggregates = vec![];
        for (v, a) in aggregations {
            let mut keep_aggregate = None;
            match a {
                AggregateExpression::Count { expr, distinct } => {
                    if let Some(inner_expr) = expr {
                        let mut expr_rewrite = self.try_recursive_rewrite_expression(
                            &rewrite_context,
                            inner_expr,
                            &ChangeType::NoChange,
                            &context.extension_with(PathEntry::AggregationOperation),
                            pushdown_settings,
                        );
                        if expr_rewrite.expression.is_some() {
                            keep_aggregate = Some(AggregateExpression::Count {
                                expr: Some(Box::new(expr_rewrite.expression.take().unwrap())),
                                distinct: distinct.clone(),
                            });
                        }
                    }
                }
                AggregateExpression::Sum { expr, distinct } => {
                    let mut expr_rewrite = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                        pushdown_settings,
                    );
                    if expr_rewrite.expression.is_some() {
                        keep_aggregate = Some(AggregateExpression::Sum {
                            expr: Box::new(expr_rewrite.expression.take().unwrap()),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::Avg { expr, distinct } => {
                    let mut expr_rewrite = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                        pushdown_settings,
                    );
                    if expr_rewrite.expression.is_some() {
                        keep_aggregate = Some(AggregateExpression::Avg {
                            expr: Box::new(expr_rewrite.expression.take().unwrap()),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::Min { expr, distinct } => {
                    let mut expr_rewrite = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                        pushdown_settings,
                    );
                    if expr_rewrite.expression.is_some() {
                        keep_aggregate = Some(AggregateExpression::Min {
                            expr: Box::new(expr_rewrite.expression.take().unwrap()),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::Max { expr, distinct } => {
                    let mut expr_rewrite = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                        pushdown_settings,
                    );
                    if expr_rewrite.expression.is_some() {
                        keep_aggregate = Some(AggregateExpression::Max {
                            expr: Box::new(expr_rewrite.expression.take().unwrap()),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::GroupConcat {
                    expr,
                    distinct,
                    separator,
                } => {
                    let mut expr_rewrite = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                        pushdown_settings,
                    );
                    if expr_rewrite.expression.is_some() {
                        keep_aggregate = Some(AggregateExpression::GroupConcat {
                            expr: Box::new(expr_rewrite.expression.take().unwrap()),
                            distinct: distinct.clone(),
                            separator: separator.clone(),
                        });
                    }
                }
                AggregateExpression::Sample { expr, distinct } => {
                    let mut expr_rewrite = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                        pushdown_settings,
                    );
                    if expr_rewrite.expression.is_some() {
                        keep_aggregate = Some(AggregateExpression::Sample {
                            expr: Box::new(expr_rewrite.expression.take().unwrap()),
                            distinct: distinct.clone(),
                        });
                    }
                }
                AggregateExpression::Custom {
                    name,
                    expr,
                    distinct,
                } => {
                    let mut expr_rewrite = self.try_recursive_rewrite_expression(
                        &rewrite_context,
                        expr,
                        &ChangeType::NoChange,
                        &context.extension_with(PathEntry::AggregationOperation),
                        pushdown_settings,
                    );
                    if expr_rewrite.expression.is_some() {
                        keep_aggregate = Some(AggregateExpression::Custom {
                            name: name.clone(),
                            expr: Box::new(expr_rewrite.expression.take().unwrap()),
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
            let grouped = GroupedTimeSeriesQuery {
                tsq: Box::new(self.clone()),
                graph_pattern_hash: hash_graph_pattern(group_graph_pattern),
                by,
                aggregations: keep_aggregates
                    .into_iter()
                    .map(|(v, a)| (v, AggregateExpressionInContext::new(a, context.clone())))
                    .collect(),
                timeseries_funcs,
            };
            let new_tsq = TimeSeriesQuery::Grouped(grouped);
            return Some(new_tsq);
        } else {
            return None;
        }
    }
}

impl BasicTimeSeriesQuery {
    pub fn new_empty() -> BasicTimeSeriesQuery {
        BasicTimeSeriesQuery {
            identifier_variable: None,
            timeseries_variable: None,
            data_point_variable: None,
            value_variable: None,
            datatype_variable: None,
            datatype: None,
            timestamp_variable: None,
            ids: None,
        }
    }
}

use crate::constraints::Constraint;
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::Variable;
use polars::prelude::{col, ChunkAgg, DataFrame, Expr};
use spargebra::algebra::{Expression, Function, GraphPattern};
use spargebra::term::TermPattern;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::os::linux::raw::stat;
use polars::prelude::GroupByMethod::Var;

pub(crate) fn find_all_groupby_pushdowns(
    static_query: &Query,
    static_query_df: &DataFrame,
    time_series_queries: &mut Vec<TimeSeriesQuery>,
    has_constraint: &HashMap<Variable, Constraint>,
) {
    if let Query::Select {
        dataset: _,
        pattern,
        base_iri: _,
    } = static_query
    {
        find_groupby_pushdowns_in_graph_pattern(pattern, static_query_df, time_series_queries, has_constraint)
    }
}

fn find_groupby_pushdowns_in_graph_pattern(
    graph_pattern: &GraphPattern,
    static_query_df: &DataFrame,
    time_series_queries: &mut Vec<TimeSeriesQuery>,
    has_constraint: &HashMap<Variable, Constraint>,
) {
    match graph_pattern {
        GraphPattern::Join { left, right } => {
            find_groupby_pushdowns_in_graph_pattern(
                left,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
            find_groupby_pushdowns_in_graph_pattern(
                right,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                left,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
            find_groupby_pushdowns_in_graph_pattern(
                right,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Filter { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Union { left, right } => {
            find_groupby_pushdowns_in_graph_pattern(
                left,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
            find_groupby_pushdowns_in_graph_pattern(
                right,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Graph { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Extend { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Minus { left, right } => {
            find_groupby_pushdowns_in_graph_pattern(
                left,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
            find_groupby_pushdowns_in_graph_pattern(
                right,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::OrderBy { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Project { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Distinct { inner } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Reduced { inner } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Slice { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                has_constraint,
            );
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let mut used_vars = HashSet::new();
            for v in variables {
                used_vars.insert(v.clone());
            }
            find_all_used_variables_in_graph_pattern(inner, &mut used_vars);
            let vs_and_cs: Vec<(Variable, Constraint)> = used_vars
                .iter()
                .filter(|v| has_constraint.contains_key(v))
                .map(|v| (v.clone(), has_constraint.get(v).unwrap().clone()))
                .collect();
            'outer: for tsq in time_series_queries {
                for (v, c) in &vs_and_cs {
                    let in_tsq = match c {
                        Constraint::ExternalTimeseries => {
                            tsq.timeseries_variable.is_some()
                                && tsq.timeseries_variable.as_ref().unwrap() == v
                        }
                        Constraint::ExternalDataPoint => {
                            tsq.data_point_variable.is_some()
                                && tsq.data_point_variable.as_ref().unwrap() == v
                        }
                        Constraint::ExternalDataValue => {
                            tsq.value_variable.is_some()
                                && tsq.value_variable.as_ref().unwrap() == v
                        }
                        Constraint::ExternalTimestamp => {
                            tsq.timestamp_variable.is_some()
                                && tsq.timestamp_variable.as_ref().unwrap() == v
                        }
                    };
                    if !in_tsq {
                        continue 'outer;
                    }
                }
                let mut timeseries_funcs = vec![];
                find_all_timeseries_funcs_in_graph_pattern(inner, &mut timeseries_funcs, tsq);
                let mut static_grouping_variables = vec![];
                let mut dynamic_grouping_variables = vec![];
                'forvar: for v in variables {
                    if let Some(tsv) = &tsq.timestamp_variable {
                        if tsv == v {
                            dynamic_grouping_variables.push(tsv.clone());
                            continue 'forvar
                        }
                    }
                    if let Some(vv) = &tsq.value_variable {
                        if vv == v {
                            dynamic_grouping_variables.push(vv.clone());
                            continue 'forvar
                        }
                    }
                    for (fv, _) in &timeseries_funcs {
                        if fv == v {
                            dynamic_grouping_variables.push(fv.clone());
                            continue 'forvar
                        }
                    }
                    static_grouping_variables.push(v.clone())
                }
                //Todo: impose constraints on graph pattern structure here ..
                if (static_grouping_variables.is_empty() && !dynamic_grouping_variables.is_empty()) || variables_isomorphic_to_time_series_id(
                    &static_grouping_variables,
                    tsq.identifier_variable.as_ref().unwrap().as_str(),
                    static_query_df,
                ) {
                    let mut by= dynamic_grouping_variables;
                    if !static_grouping_variables.is_empty() {
                        by.push(tsq.identifier_variable.as_ref().unwrap().clone());
                    }
                    tsq.try_pushdown_aggregates(aggregates, graph_pattern, timeseries_funcs, by);
                }
            }
        }
        _ => {}
    }
}

fn find_all_timeseries_funcs_in_graph_pattern(graph_pattern: &GraphPattern, timeseries_funcs: &mut Vec<(Variable, Expression)>, tsq:&TimeSeriesQuery) {
    match graph_pattern {
        GraphPattern::Join { left, right } => {
            find_all_timeseries_funcs_in_graph_pattern(left, timeseries_funcs, tsq);
            find_all_timeseries_funcs_in_graph_pattern(right, timeseries_funcs, tsq);
        }
        GraphPattern::LeftJoin { left, right, expression } => {
            find_all_timeseries_funcs_in_graph_pattern(left, timeseries_funcs, tsq);
            find_all_timeseries_funcs_in_graph_pattern(right, timeseries_funcs, tsq);
        }
        GraphPattern::Filter { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(inner, timeseries_funcs, tsq);
        }
        GraphPattern::Union { left, right } => {
            find_all_timeseries_funcs_in_graph_pattern(left, timeseries_funcs, tsq);
            find_all_timeseries_funcs_in_graph_pattern(right, timeseries_funcs, tsq);
        }
        GraphPattern::Graph { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(inner, timeseries_funcs, tsq);
        }
        GraphPattern::Extend { inner, variable, expression } => {
            //Very important to process inner first here to detect nested functions.
            find_all_timeseries_funcs_in_graph_pattern(inner, timeseries_funcs, tsq);
            let mut function_vars = HashSet::new();
            find_all_used_variables_in_expression(expression, &mut function_vars);
            if !function_vars.is_empty() {
                let mut exists_var_in_timeseries = false;
                'outer: for v in &function_vars {
                    if let Some(vv) = &tsq.value_variable {
                        if vv == v {
                            exists_var_in_timeseries = true;
                            break 'outer;
                        }
                    }
                    if let Some(tsv) = &tsq.timestamp_variable {
                        if tsv == v {
                            exists_var_in_timeseries = true;
                            break 'outer;
                        }
                    }
                    for (outvar, _) in timeseries_funcs.iter() {
                        if outvar == v {
                            exists_var_in_timeseries = true;
                            break 'outer;
                        }
                    }
                }
                if exists_var_in_timeseries {
                    timeseries_funcs.push((variable.clone(), expression.clone()))
                }
            }
        }
        GraphPattern::Minus { left, right } => {
            find_all_timeseries_funcs_in_graph_pattern(left, timeseries_funcs, tsq);
            find_all_timeseries_funcs_in_graph_pattern(right, timeseries_funcs, tsq);
        }
        GraphPattern::OrderBy { inner, expression } => {
            find_all_timeseries_funcs_in_graph_pattern(inner, timeseries_funcs, tsq);
            todo!("Consider expression");
        }
        GraphPattern::Project { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(inner, timeseries_funcs, tsq);
        }
        GraphPattern::Distinct { inner } => {
            find_all_timeseries_funcs_in_graph_pattern(inner, timeseries_funcs, tsq);
        }
        GraphPattern::Reduced { inner } => {
            find_all_timeseries_funcs_in_graph_pattern(inner, timeseries_funcs, tsq);
        }
        GraphPattern::Slice { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(inner, timeseries_funcs, tsq);
        }
        GraphPattern::Group { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(inner, timeseries_funcs, tsq);
        }
        _ => {}
    }
}

fn variables_isomorphic_to_time_series_id(
    variables: &Vec<Variable>,
    time_series_identifier: &str,
    static_query_df: &DataFrame,
) -> bool {
    let colnames = static_query_df.get_column_names();
    for v in variables {
        if !colnames.contains(&v.as_str()) {
            //This can happen when there is an aggregation variable which is not part of the static query and not registered as a timeseries func.
            return false;
        }
    }
    let n_unique_identifiers = static_query_df
        .column(time_series_identifier)
        .expect("Column problem")
        .is_unique()
        .expect("Unique problem")
        .sum()
        .expect("Sum problem");
    let columns: Vec<&str> = variables.iter().map(|v| v.as_str()).collect();
    let n_unique_n_tuples = static_query_df
        .select(columns.as_slice())
        .expect("Columns problem")
        .is_unique()
        .expect("Unique problem")
        .sum()
        .expect("Sum problem");
    n_unique_identifiers == n_unique_n_tuples
}

fn find_all_used_variables_in_graph_pattern(
    graph_pattern: &GraphPattern,
    used_vars: &mut HashSet<Variable>,
) {
    match graph_pattern {
        GraphPattern::Bgp { patterns } => {
            for p in patterns {
                if let TermPattern::Variable(v) = &p.subject {
                    used_vars.insert(v.clone());
                }
                if let TermPattern::Variable(v) = &p.object {
                    used_vars.insert(v.clone());
                }
            }
        }
        GraphPattern::Path {
            subject, object, ..
        } => {
            if let TermPattern::Variable(v) = subject {
                used_vars.insert(v.clone());
            }
            if let TermPattern::Variable(v) = object {
                used_vars.insert(v.clone());
            }
        }
        GraphPattern::Join { left, right } => {
            find_all_used_variables_in_graph_pattern(left, used_vars);
            find_all_used_variables_in_graph_pattern(right, used_vars);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            find_all_used_variables_in_graph_pattern(left, used_vars);
            find_all_used_variables_in_graph_pattern(right, used_vars);
            if let Some(e) = expression {
                find_all_used_variables_in_expression(e, used_vars);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
            find_all_used_variables_in_expression(expr, used_vars);
        }
        GraphPattern::Union { left, right } => {
            find_all_used_variables_in_graph_pattern(left, used_vars);
            find_all_used_variables_in_graph_pattern(right, used_vars);
        }
        GraphPattern::Graph { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
            find_all_used_variables_in_expression(expression, used_vars);
        }
        GraphPattern::Minus { left, right } => {
            find_all_used_variables_in_graph_pattern(left, used_vars);
            find_all_used_variables_in_graph_pattern(right, used_vars);
        }
        GraphPattern::OrderBy { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Project { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Distinct { inner } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Reduced { inner } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Slice { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        GraphPattern::Group { inner, .. } => {
            find_all_used_variables_in_graph_pattern(inner, used_vars);
        }
        _ => {}
    }
}

fn find_all_used_variables_in_expression(
    expression: &Expression,
    used_vars: &mut HashSet<Variable>,
) {
    match expression {
        Expression::Variable(v) => {
            used_vars.insert(v.clone());
        }
        Expression::Or(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::And(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Equal(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::SameTerm(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Greater(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::GreaterOrEqual(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Less(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::LessOrEqual(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::In(left, rights) => {
            find_all_used_variables_in_expression(left, used_vars);
            for e in rights {
                find_all_used_variables_in_expression(e, used_vars);
            }
        }
        Expression::Add(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Subtract(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Multiply(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Divide(left, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::UnaryPlus(inner) => {
            find_all_used_variables_in_expression(inner, used_vars);
        }
        Expression::UnaryMinus(inner) => {
            find_all_used_variables_in_expression(inner, used_vars);
        }
        Expression::Not(inner) => {
            find_all_used_variables_in_expression(inner, used_vars);
        }
        Expression::Exists(graph_pattern) => {
            find_all_used_variables_in_graph_pattern(graph_pattern, used_vars);
        }
        Expression::Bound(inner) => {used_vars.insert(inner.clone());},
        Expression::If(left, middle, right) => {
            find_all_used_variables_in_expression(left, used_vars);
            find_all_used_variables_in_expression(middle, used_vars);
            find_all_used_variables_in_expression(right, used_vars);
        }
        Expression::Coalesce(inner) => {
            for e in inner {
                find_all_used_variables_in_expression(e, used_vars);
            }
        }
        Expression::FunctionCall(_, args) => {
            for e in args {
                find_all_used_variables_in_expression(e, used_vars);
            }
        }
        _ => {}
    }
}

use crate::constraints::{Constraint, VariableConstraints};
use crate::find_query_variables::{
    find_all_used_variables_in_expression, find_all_used_variables_in_graph_pattern,
};
use crate::pushdown_setting::PushdownSetting;
use crate::query_context::{Context, ExpressionInContext, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::Variable;
use polars::prelude::{ChunkAgg, DataFrame};
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashSet;

pub(crate) fn find_all_groupby_pushdowns(
    static_query: &Query,
    static_query_df: &DataFrame,
    time_series_queries: &mut Vec<TimeSeriesQuery>,
    variable_constraints: &VariableConstraints,
    pushdown_settings: &HashSet<PushdownSetting>,
) {
    assert!(static_query_df.height() > 0);
    if let Query::Select {
        dataset: _,
        pattern,
        base_iri: _,
    } = static_query
    {
        find_groupby_pushdowns_in_graph_pattern(
            pattern,
            static_query_df,
            time_series_queries,
            variable_constraints,
            &Context::new(),
            pushdown_settings,
        )
    }
}

fn find_groupby_pushdowns_in_graph_pattern(
    graph_pattern: &GraphPattern,
    static_query_df: &DataFrame,
    time_series_queries: &mut Vec<TimeSeriesQuery>,
    variable_constraints: &VariableConstraints,
    context: &Context,
    pushdown_settings: &HashSet<PushdownSetting>,
) {
    match graph_pattern {
        GraphPattern::Join { left, right } => {
            find_groupby_pushdowns_in_graph_pattern(
                left,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::JoinLeftSide),
                pushdown_settings,
            );
            find_groupby_pushdowns_in_graph_pattern(
                right,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::JoinRightSide),
                pushdown_settings,
            );
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                left,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::LeftJoinLeftSide),
                pushdown_settings,
            );
            find_groupby_pushdowns_in_graph_pattern(
                right,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::LeftJoinRightSide),
                pushdown_settings,
            );
        }
        GraphPattern::Filter { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::FilterInner),
                pushdown_settings,
            );
        }
        GraphPattern::Union { left, right } => {
            find_groupby_pushdowns_in_graph_pattern(
                left,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::UnionLeftSide),
                pushdown_settings,
            );
            find_groupby_pushdowns_in_graph_pattern(
                right,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::UnionRightSide),
                pushdown_settings,
            );
        }
        GraphPattern::Graph { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::GraphInner),
                pushdown_settings,
            );
        }
        GraphPattern::Extend { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::ExtendInner),
                pushdown_settings,
            );
        }
        GraphPattern::Minus { left, right } => {
            find_groupby_pushdowns_in_graph_pattern(
                left,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::MinusLeftSide),
                pushdown_settings,
            );
            find_groupby_pushdowns_in_graph_pattern(
                right,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::MinusRightSide),
                pushdown_settings,
            );
        }
        GraphPattern::OrderBy { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::OrderByInner),
                pushdown_settings,
            );
        }
        GraphPattern::Project { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::ProjectInner),
                pushdown_settings,
            );
        }
        GraphPattern::Distinct { inner } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::DistinctInner),
                pushdown_settings,
            );
        }
        GraphPattern::Reduced { inner } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::ReducedInner),
                pushdown_settings,
            );
        }
        GraphPattern::Slice { inner, .. } => {
            find_groupby_pushdowns_in_graph_pattern(
                inner,
                static_query_df,
                time_series_queries,
                variable_constraints,
                &context.extension_with(PathEntry::SliceInner),
                pushdown_settings,
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
                .filter(|v| variable_constraints.contains(v, context))
                .map(|v| {
                    (
                        v.clone(),
                        variable_constraints
                            .get_constraint(v, context)
                            .unwrap()
                            .clone(),
                    )
                })
                .collect();
            let mut to_replace = vec![];
            let mut new_tsqs = vec![];
            'outer: for i in 0..time_series_queries.len() {
                let tsq = time_series_queries.get(i).unwrap();
                if !tsq.dropped_value_expression() {
                    for (v, c) in &vs_and_cs {
                        let in_tsq = match c {
                            Constraint::ExternalTimeseries => {
                                tsq.has_equivalent_timeseries_variable(v, context)
                            }
                            Constraint::ExternalDataPoint => {
                                tsq.has_equivalent_data_point_variable(v, context)
                            }
                            Constraint::ExternalDataValue => {
                                tsq.has_equivalent_value_variable(v, context)
                            }
                            Constraint::ExternalTimestamp => {
                                tsq.has_equivalent_timestamp_variable(v, context)
                            }
                            Constraint::ExternallyDerived => {
                                true //true since we do not want to disqualify our timeseries query.. TODO figure out
                            }
                        };
                        if !in_tsq {
                            continue 'outer;
                        }
                    }
                    let mut timeseries_funcs = vec![];
                    find_all_timeseries_funcs_in_graph_pattern(
                        inner,
                        &mut timeseries_funcs,
                        tsq,
                        &context.extension_with(PathEntry::GroupInner),
                    );
                    let mut static_grouping_variables = vec![];
                    let mut dynamic_grouping_variables = vec![];
                    'forvar: for v in variables {
                        for tsv in tsq.get_timestamp_variables() {
                            if tsv.equivalent(v, context) {
                                dynamic_grouping_variables.push(tsv.variable.clone());
                                continue 'forvar;
                            }
                        }
                        for vv in tsq.get_value_variables() {
                            if vv.equivalent(v, context) {
                                dynamic_grouping_variables.push(vv.variable.clone());
                                continue 'forvar;
                            }
                        }
                        for (fv, _) in &timeseries_funcs {
                            if fv == v {
                                dynamic_grouping_variables.push(fv.clone());
                                continue 'forvar;
                            }
                        }
                        static_grouping_variables.push(v.clone())
                    }
                    let mut all_identifiers_isomorphic = true;
                    for id_var in tsq.get_identifier_variables() {
                        if !variables_isomorphic_to_time_series_id(
                            &static_grouping_variables,
                            id_var.as_str(),
                            static_query_df,
                        ) {
                            all_identifiers_isomorphic = false;
                            break;
                        }
                    }

                    if (static_grouping_variables.is_empty()
                        && !dynamic_grouping_variables.is_empty())
                        || all_identifiers_isomorphic
                    {
                        let mut by = dynamic_grouping_variables;
                        if !static_grouping_variables.is_empty() {
                            for v in tsq.get_identifier_variables() {
                                by.push(v.clone());
                            }
                        }
                        if let Some(updated_tsq) = tsq.try_pushdown_aggregates(
                            aggregates,
                            graph_pattern,
                            timeseries_funcs,
                            by,
                            context,
                            pushdown_settings,
                        ) {
                            to_replace.push(i);
                            new_tsqs.push(updated_tsq);
                        }
                    }
                }
            }
            for i in to_replace {
                let replace_with = new_tsqs.remove(0);
                time_series_queries.remove(i);
                time_series_queries.insert(i, replace_with);
            }
        }
        _ => {}
    }
}

fn find_all_timeseries_funcs_in_graph_pattern(
    graph_pattern: &GraphPattern,
    timeseries_funcs: &mut Vec<(Variable, ExpressionInContext)>,
    tsq: &TimeSeriesQuery,
    context: &Context,
) {
    match graph_pattern {
        GraphPattern::Join { left, right } => {
            find_all_timeseries_funcs_in_graph_pattern(
                left,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::JoinLeftSide),
            );
            find_all_timeseries_funcs_in_graph_pattern(
                right,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::JoinRightSide),
            );
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression: _,
        } => {
            find_all_timeseries_funcs_in_graph_pattern(
                left,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::LeftJoinLeftSide),
            );
            find_all_timeseries_funcs_in_graph_pattern(
                right,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::LeftJoinRightSide),
            );
        }
        GraphPattern::Filter { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(
                inner,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::FilterInner),
            );
        }
        GraphPattern::Union { left, right } => {
            find_all_timeseries_funcs_in_graph_pattern(
                left,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::UnionLeftSide),
            );
            find_all_timeseries_funcs_in_graph_pattern(
                right,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::UnionRightSide),
            );
        }
        GraphPattern::Graph { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(
                inner,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::GraphInner),
            );
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            //Very important to process inner first here to detect nested functions.
            find_all_timeseries_funcs_in_graph_pattern(
                inner,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::ExtendInner),
            );
            let mut function_vars = HashSet::new();
            find_all_used_variables_in_expression(expression, &mut function_vars);
            if !function_vars.is_empty() {
                let mut exists_var_in_timeseries = false;
                'outer: for v in &function_vars {
                    if tsq.has_equivalent_value_variable(v, context) {
                        exists_var_in_timeseries = true;
                        break 'outer;
                    }
                    if tsq.has_equivalent_timestamp_variable(v, context) {
                        exists_var_in_timeseries = true;
                        break 'outer;
                    }
                    for (outvar, _) in timeseries_funcs.iter() {
                        if outvar == v {
                            exists_var_in_timeseries = true;
                            break 'outer;
                        }
                    }
                }
                if exists_var_in_timeseries {
                    timeseries_funcs.push((
                        variable.clone(),
                        ExpressionInContext::new(expression.clone(), context.clone()),
                    ))
                }
            }
        }
        GraphPattern::Minus { left, right } => {
            find_all_timeseries_funcs_in_graph_pattern(
                left,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::MinusLeftSide),
            );
            find_all_timeseries_funcs_in_graph_pattern(
                right,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::MinusRightSide),
            );
        }
        GraphPattern::OrderBy {
            inner,
            expression: _,
        } => {
            //No ordering expressions should be pushed down, not supported
            find_all_timeseries_funcs_in_graph_pattern(
                inner,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::OrderByInner),
            );
        }
        GraphPattern::Project { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(
                inner,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::ProjectInner),
            );
        }
        GraphPattern::Distinct { inner } => {
            find_all_timeseries_funcs_in_graph_pattern(
                inner,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::DistinctInner),
            );
        }
        GraphPattern::Reduced { inner } => {
            find_all_timeseries_funcs_in_graph_pattern(
                inner,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::ReducedInner),
            );
        }
        GraphPattern::Slice { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(
                inner,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::SliceInner),
            );
        }
        GraphPattern::Group { inner, .. } => {
            find_all_timeseries_funcs_in_graph_pattern(
                inner,
                timeseries_funcs,
                tsq,
                &context.extension_with(PathEntry::GroupInner),
            );
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

#[cfg(test)]
mod tests {
    use crate::engine::complete_time_series_queries;
    use crate::groupby_pushdown::find_all_groupby_pushdowns;
    use crate::preprocessing::Preprocessor;
    use crate::pushdown_setting::all_pushdowns;
    use crate::rewriting::StaticQueryRewriter;
    use crate::sparql_result_to_polars::create_static_query_result_df;
    use crate::splitter::parse_sparql_select_query;
    use oxrdf::vocab::xsd;
    use oxrdf::{Literal, Term, Variable};
    use sparesults::QuerySolution;
    use std::rc::Rc;

    #[test]
    fn test_missing_grouping() {
        let sparql = r#"#Four turbines, timestamp constraint
PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX otit:<https://github.com/magbak/otit_swt#>
PREFIX wp:<https://github.com/magbak/otit_swt/windpower_example#>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rds:<https://github.com/magbak/otit_swt/rds_power#>
SELECT ?wtur_label ?year ?month ?day ?hour ?minute_10 (AVG(?val_prod) as ?val_prod_avg) (AVG(?val_dir) as ?val_dir_avg) (AVG(?val_speed) as ?val_speed_avg) WHERE {
    ?site a rds:Site .
    ?site rdfs:label "Wind Mountain" .
    ?site rds:hasFunctionalAspect ?wtur_asp .
    ?wtur_asp rdfs:label ?wtur_label .
    ?wtur rds:hasFunctionalAspectNode ?wtur_asp .
    ?wtur a rds:A .
    ?wtur otit:hasTimeseries ?ts_oper .
    ?ts_oper otit:hasDataPoint ?dp_oper .
    ?dp_oper otit:hasValue ?val_oper .
    ?dp_oper otit:hasTimestamp ?t .
    ?ts_oper rdfs:label "Operating" .
    ?wtur rds:hasFunctionalAspect ?gensys_asp .
    ?gensys rds:hasFunctionalAspectNode ?gensys_asp .
    ?gensys a rds:RA .
    ?gensys rds:hasFunctionalAspect ?generator_asp .
    ?generator rds:hasFunctionalAspectNode ?generator_asp .
    ?generator a rds:GAA .
    ?wtur rds:hasFunctionalAspect ?weather_asp .
    ?weather rds:hasFunctionalAspectNode ?weather_asp .
    ?weather a rds:LE .
    ?weather otit:hasTimeseries ?ts_speed .
    ?ts_speed otit:hasDataPoint ?dp_speed .
    ?dp_speed otit:hasValue ?val_speed .
    ?dp_speed otit:hasTimestamp ?t .
    ?ts_speed rdfs:label "Windspeed" .
    ?weather otit:hasTimeseries ?ts_dir .
    ?ts_dir otit:hasDataPoint ?dp_dir .
    ?dp_dir otit:hasValue ?val_dir .
    ?dp_dir otit:hasTimestamp ?t .
    ?ts_dir rdfs:label "WindDirection" .
    ?generator otit:hasTimeseries ?ts_prod .
    ?ts_prod rdfs:label "Production" .
    ?ts_prod otit:hasDataPoint ?dp_prod .
    ?dp_prod otit:hasValue ?val_prod .
    ?dp_prod otit:hasTimestamp ?t .
    BIND(xsd:integer(FLOOR(minutes(?t) / 10.0)) as ?minute_10)
    BIND(hours(?t) AS ?hour)
    BIND(day(?t) AS ?day)
    BIND(month(?t) AS ?month)
    BIND(year(?t) AS ?year)
    FILTER(?t >= "2022-08-30T08:46:53"^^xsd:dateTime && ?t <= "2022-08-30T21:46:53"^^xsd:dateTime) .
}
GROUP BY ?wtur_label ?year ?month ?day ?hour ?minute_10
"#;

        let parsed = parse_sparql_select_query(sparql).unwrap();
        let mut preprocessor = Preprocessor::new();
        let (preprocessed_query, variable_constraints) = preprocessor.preprocess(&parsed);
        let mut rewriter = StaticQueryRewriter::new(all_pushdowns(), &variable_constraints, true);
        let (static_rewrite, mut tsqs) = rewriter.rewrite_query(preprocessed_query).unwrap();

        let solutions = vec![QuerySolution::from((
            Rc::new(vec![
                Variable::new_unchecked("wtur_label"),
                Variable::new_unchecked("ts_datatype_0"),
                Variable::new_unchecked("ts_external_id_0"),
                Variable::new_unchecked("ts_datatype_1"),
                Variable::new_unchecked("ts_external_id_1"),
                Variable::new_unchecked("ts_datatype_2"),
                Variable::new_unchecked("ts_external_id_2"),
                Variable::new_unchecked("ts_datatype_3"),
                Variable::new_unchecked("ts_external_id_3"),
            ]),
            vec![
                Some(Term::Literal(Literal::new_simple_literal("wt"))),
                Some(Term::Literal(Literal::new_simple_literal(
                    xsd::DOUBLE.as_str(),
                ))),
                Some(Term::Literal(Literal::new_simple_literal("id0"))),
                Some(Term::Literal(Literal::new_simple_literal(
                    xsd::DOUBLE.as_str(),
                ))),
                Some(Term::Literal(Literal::new_simple_literal("id1"))),
                Some(Term::Literal(Literal::new_simple_literal(
                    xsd::DOUBLE.as_str(),
                ))),
                Some(Term::Literal(Literal::new_simple_literal("id2"))),
                Some(Term::Literal(Literal::new_simple_literal(
                    xsd::DOUBLE.as_str(),
                ))),
                Some(Term::Literal(Literal::new_simple_literal("id3"))),
            ],
        ))];
        complete_time_series_queries(&solutions, &mut tsqs).unwrap();
        let static_result_df = create_static_query_result_df(&static_rewrite, solutions);
        find_all_groupby_pushdowns(
            &parsed,
            &static_result_df,
            &mut tsqs,
            &variable_constraints,
            &all_pushdowns(),
        );
        //let static_result_df = DataFrame::new();
        println!("TSQS: {:?}", tsqs);
    }
}

use crate::constants::HAS_VALUE;
use crate::exists_helper::rewrite_exists_graph_pattern;
use crate::rewriting::hash_graph_pattern;
use crate::sparql_result_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use crate::timeseries_query::TimeSeriesQuery;
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNodeRef, Variable};
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::prelude::DataType::Utf8;
use polars::prelude::{
    col, concat, concat_str, Expr, GetOutput, IntoLazy, IntoSeries, JoinType, LazyFrame,
    LiteralValue, Operator, Series, UniqueKeepStrategy,
};
use spargebra::algebra::{
    AggregateExpression, Expression, Function, GraphPattern, OrderExpression,
};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashSet;
use std::ops::Not;
use crate::query_context::{Context, PathEntry};

pub struct Combiner {
    counter: u16,
}

impl Combiner {
    pub fn new() -> Combiner {
        Combiner { counter: 0 }
    }

    pub fn combine_static_and_time_series_results(
        &mut self,
        query: Query,
        static_result_df: DataFrame,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
    ) -> LazyFrame {
        let project_variables;
        let inner_graph_pattern;
        let mut distinct= false;
        let mut context = Context::new();
        if let Query::Select {
            dataset: _,
            pattern,
            base_iri: _,
        } = &query
        {
            if let GraphPattern::Project { inner, variables } = pattern {
                project_variables = variables.clone();
                inner_graph_pattern = inner;
                context = context.extension_with(PathEntry::ProjectInner);
            } else if let GraphPattern::Distinct{ inner } = pattern {
                context = context.extension_with(PathEntry::DistinctInner);
                if let GraphPattern::Project {inner, variables} = inner.as_ref() {
                    distinct = true;
                    project_variables = variables.clone();
                    inner_graph_pattern = inner;
                    context = context.extension_with(PathEntry::ProjectInner);
                } else {
                    panic!("Wrong!");
                }
            } else {
                panic!("Also wrong!");
            }
        } else {
            panic!("Wrong!!!");
        }
        let mut columns = static_result_df
            .get_column_names()
            .iter()
            .map(|c| c.to_string())
            .collect();

        let mut lf = static_result_df.lazy();
        lf = self.lazy_graph_pattern(&mut columns, lf, inner_graph_pattern, time_series, &context);

        let projections = project_variables
            .iter()
            .map(|c| col(c.as_str()))
            .collect::<Vec<Expr>>();
        lf = lf.select(projections.as_slice());
        if distinct {
            lf = lf.unique_stable(None, UniqueKeepStrategy::First);
        }
        lf
    }

    fn lazy_graph_pattern(
        &mut self,
        columns: &mut HashSet<String>,
        input_lf: LazyFrame,
        graph_pattern: &GraphPattern,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
        context: &Context
    ) -> LazyFrame {
        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                //No action, handled statically
                let mut output_lf = input_lf;
                let bgp_context = context.extension_with(PathEntry::BGP);
                for p in patterns {
                    output_lf = Combiner::lazy_triple_pattern(columns, output_lf, p, time_series,                 &bgp_context);
                }
                output_lf
            }
            GraphPattern::Path { .. } => {
                //No action, handled statically
                input_lf
            }
            GraphPattern::Join { left, right } => {
                let left_lf = self.lazy_graph_pattern(columns, input_lf, left, time_series, &context.extension_with(PathEntry::JoinLeftSide));
                let right_lf = self.lazy_graph_pattern(columns, left_lf, right, time_series, &context.extension_with(PathEntry::JoinRightSide));
                right_lf
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let left_join_distinct_column =
                    "left_join_distinct_column_".to_string() + &self.counter.to_string();
                self.counter += 1;
                let mut left_df = self
                    .lazy_graph_pattern(columns, input_lf, left, time_series, &context.extension_with(PathEntry::LeftJoinLeftSide))
                    .with_column(
                        Expr::Literal(LiteralValue::Int64(1)).alias(&left_join_distinct_column),
                    )
                    .with_column(col(&left_join_distinct_column).cumsum(false).keep_name())
                    .collect()
                    .expect("Left join collect left problem");

                let ts_identifiers: Vec<String> = time_series
                    .iter()
                    .map(|(tsq, _)| {
                        tsq.identifier_variable
                            .as_ref()
                            .unwrap()
                            .as_str()
                            .to_string()
                    })
                    .collect();

                let mut right_lf =
                    self.lazy_graph_pattern(columns, left_df.clone().lazy(), right, time_series, &context.extension_with(PathEntry::LeftJoinRightSide));

                if let Some(expr) = expression {
                    let expression_context = context.extension_with(PathEntry::LeftJoinExpression);
                    let expression_context_column_name = expression_context.to_string();
                    right_lf = Combiner::lazy_expression(
                        expr,
                        right_lf,
                        columns,
                        &expression_context_column_name,
                        time_series,
                        &expression_context
                    );
                    right_lf = right_lf
                        .filter(col(&expression_context_column_name))
                        .drop_columns([&expression_context_column_name]);
                }

                let right_df = right_lf.collect().expect("Collect right problem");

                for id in ts_identifiers {
                    if !columns.contains(&id) {
                        left_df.drop(&id).expect("Drop problem");
                    }
                }
                left_df = left_df
                    .filter(
                        &left_df
                            .column(&left_join_distinct_column)
                            .expect("Did not find left helper")
                            .is_in(
                                right_df
                                    .column(&left_join_distinct_column)
                                    .expect("Did not find right helper"),
                            )
                            .expect("Is in problem")
                            .not(),
                    )
                    .expect("Filter problem");

                for c in right_df.get_column_names_owned().iter() {
                    if !left_df.get_column_names().contains(&c.as_str()) {
                        left_df = left_df
                            .lazy()
                            .with_column(Expr::Literal(LiteralValue::Null).alias(c))
                            .collect()
                            .expect("Not ok");
                        left_df
                            .with_column(
                                left_df
                                    .column(c)
                                    .expect("Col c prob")
                                    .cast(right_df.column(c).unwrap().dtype())
                                    .expect("Cast error"),
                            )
                            .expect("TODO: panic message");
                    }
                }

                let mut output_lf =
                    concat(vec![left_df.lazy(), right_df.lazy()], false).expect("Concat error");
                output_lf = output_lf.drop_columns(&[&left_join_distinct_column]);

                output_lf
            }
            GraphPattern::Filter { expr, inner } => {
                let mut inner_lf = self.lazy_graph_pattern(columns, input_lf, inner, time_series, &context.extension_with(PathEntry::FilterInner));
                let expression_context = context.extension_with(PathEntry::FilterExpression);
                let expression_context_column_name = expression_context.to_string();
                inner_lf =
                    Combiner::lazy_expression(expr, inner_lf, columns, &expression_context_column_name, time_series, &expression_context);
                inner_lf = inner_lf
                    .filter(col(&expression_context_column_name))
                    .drop_columns([&expression_context_column_name]);
                inner_lf
            }
            GraphPattern::Union { left, right } => {
                let mut left_columns = columns.clone();
                let original_timeseries_columns: Vec<String> = time_series.iter().map(|(tsq, _)|tsq.identifier_variable.as_ref().unwrap().as_str().to_string()).collect();
                let mut left_lf =
                    self.lazy_graph_pattern(&mut left_columns, input_lf.clone(), left, time_series, &context.extension_with(PathEntry::UnionLeftSide));
                let mut right_columns = columns.clone();
                let mut right_input_lf = input_lf;
                for t in &original_timeseries_columns {
                    if !left_columns.contains(t) {
                        right_columns.remove(t);
                        right_input_lf = right_input_lf.drop_columns([t]);
                    }
                }
                let right_lf = self.lazy_graph_pattern(&mut right_columns, right_input_lf, right, time_series, &context.extension_with(PathEntry::UnionRightSide));

                for t in &original_timeseries_columns {
                    if !right_columns.contains(t) {
                        left_columns.remove(t);
                        left_lf = left_lf.drop_columns([t]);
                    }
                }
                left_columns.extend(right_columns.drain());
                let original_columns:Vec<String> = columns.iter().cloned().collect();
                for o in original_columns {
                    if !left_columns.contains(&o) {
                        columns.remove(&o);
                    }
                }
                columns.extend(left_columns.drain());

                let output_lf = concat(vec![left_lf, right_lf], false).expect("Concat problem");
                output_lf.unique(None, UniqueKeepStrategy::First).collect().expect("Union error").lazy()
            }
            GraphPattern::Graph { name: _, inner } => {
                self.lazy_graph_pattern(columns, input_lf, inner, time_series, &context.extension_with(PathEntry::GraphInner))
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                let mut inner_lf = self.lazy_graph_pattern(columns, input_lf, inner, time_series, &context.extension_with(PathEntry::ExtendInner));
                inner_lf = Combiner::lazy_expression(
                    expression,
                    inner_lf,
                    columns,
                    variable.as_str(),
                    time_series,
                    &context.extension_with(PathEntry::ExtendExpression)
                );
                columns.insert(variable.as_str().to_string());
                inner_lf
            }
            GraphPattern::Minus { left, right } => {
                let minus_column = "minus_column".to_string() + &self.counter.to_string();
                self.counter += 1;
                debug!("Left graph pattern {}", left);
                let mut left_df = self
                    .lazy_graph_pattern(columns, input_lf, left, time_series, &context.extension_with(PathEntry::MinusLeftSide))
                    .with_column(Expr::Literal(LiteralValue::Int64(1)).alias(&minus_column))
                    .with_column(col(&minus_column).cumsum(false).keep_name())
                    .collect()
                    .expect("Minus collect left problem");

                debug!("Minus left hand side: {:?}", left_df);
                //TODO: determine only variables actually used before copy
                let right_df = self
                    .lazy_graph_pattern(columns, left_df.clone().lazy(), right, time_series, &context.extension_with(PathEntry::MinusRightSide))
                    .select([col(&minus_column)])
                    .collect()
                    .expect("Minus right df collect problem");
                left_df = left_df
                    .filter(
                        &left_df
                            .column(&minus_column)
                            .unwrap()
                            .is_in(right_df.column(&minus_column).unwrap())
                            .unwrap()
                            .not(),
                    )
                    .expect("Filter minus left hand side problem");
                left_df.drop(&minus_column).unwrap().lazy()
            }
            GraphPattern::Values {
                variables: _,
                bindings: _,
            } => {
                //These are handled by the static query.
                input_lf
            }
            GraphPattern::OrderBy { inner, expression } => {
                let mut inner_lf = self.lazy_graph_pattern(columns, input_lf, inner, time_series, &context.extension_with(PathEntry::OrderByInner));
                let order_expression_colnames: Vec<String> = (0..expression.len())
                    .map(|i| "ordering_column_".to_string() + &i.to_string())
                    .collect();
                let mut asc_ordering = vec![];
                for i in 0..expression.len() {
                    let (lf, reverse) = Combiner::lazy_order_expression(
                        expression.get(i).unwrap(),
                        inner_lf,
                        columns,
                        order_expression_colnames.get(0).unwrap(),
                        time_series,
                        &context.extension_with(PathEntry::OrderByExpression)
                    );
                    inner_lf = lf;
                    asc_ordering.push(reverse);
                }
                inner_lf = inner_lf.sort_by_exprs(
                    order_expression_colnames
                        .iter()
                        .map(|c| col(c))
                        .collect::<Vec<Expr>>(),
                    asc_ordering.iter().map(|asc| !asc).collect(),
                );
                inner_lf = inner_lf
                    .drop_columns(order_expression_colnames.iter().collect::<Vec<&String>>());
                inner_lf
            }
            GraphPattern::Project { inner, variables } => {
                let inner_lf = self.lazy_graph_pattern(columns, input_lf, inner, time_series, &context.extension_with(PathEntry::ProjectInner));
                let mut cols: Vec<Expr> = variables.iter().map(|c| col(c.as_str())).collect();
                for (tsq, _) in time_series {
                    cols.push(col(tsq.identifier_variable.as_ref().unwrap().as_str()));
                }
                inner_lf.select(cols.as_slice())
            }
            GraphPattern::Distinct { inner } => self
                .lazy_graph_pattern(columns, input_lf, inner, time_series, &context.extension_with(PathEntry::DistinctInner))
                .unique_stable(None, UniqueKeepStrategy::First),
            GraphPattern::Reduced { .. } => {
                todo!()
            }
            GraphPattern::Slice { .. } => {
                todo!()
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                let graph_pattern_hash = hash_graph_pattern(graph_pattern);
                let mut found_index = None;
                for i in 0..time_series.len() {
                    let (tsq, _) = time_series.get(i).as_ref().unwrap();
                    if let Some(grouping) = &tsq.grouping {
                        if graph_pattern_hash == grouping.graph_pattern_hash {
                            found_index = Some(i);
                        }
                    }
                }
                if let Some(index) = found_index {
                    let (tsq, df) = time_series.remove(index);
                    Combiner::join_tsq(columns, input_lf, tsq, df)
                } else {
                    self.lazy_group_without_pushdown(
                        columns,
                        input_lf,
                        inner,
                        variables,
                        aggregates,
                        time_series,
                        context
                    )
                }
            }
            GraphPattern::Service { .. } => {
                todo!()
            }
        }
    }

    fn lazy_group_without_pushdown(
        &mut self,
        columns: &mut HashSet<String>,
        input_lf: LazyFrame,
        inner: &Box<GraphPattern>,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
        context: &Context,
    ) -> LazyFrame {
        let mut lazy_inner = self.lazy_graph_pattern(columns, input_lf, inner, time_series, &context.extension_with(PathEntry::GroupInner));
        let by: Vec<Expr> = variables.iter().map(|v| col(v.as_str())).collect();

        let mut column_variables = vec![];
        'outer: for v in columns.iter() {
            for (tsq, _) in time_series.iter() {
                if tsq.identifier_variable.as_ref().unwrap().as_str() == v {
                    continue 'outer;
                }
            }
            column_variables.push(v.clone());
        }

        let mut aggregate_expressions = vec![];
        let mut aggregate_columns = vec![];
        for i in 0..aggregates.len() {
            let (v, a) = aggregates.get(i).unwrap();
            let column_name = context.to_string() + &i.to_string();
            let (lf, expr, used_col) = sparql_aggregate_expression_as_lazy_column_and_expression(
                v,
                a,
                &column_variables,
                columns,
                &column_name,
                lazy_inner,
                time_series,
                &context.extension_with(PathEntry::GroupAggregation)
            );
            lazy_inner = lf;
            aggregate_expressions.push(expr);
            if used_col {
                aggregate_columns.push(column_name.to_string());
            }
        }

        let lazy_group_by = lazy_inner.groupby(by.as_slice());

        let aggregated_lf = lazy_group_by
            .agg(aggregate_expressions.as_slice())
            .drop_columns(aggregate_columns.as_slice());
        columns.clear();
        for v in variables {
            columns.insert(v.as_str().to_string());
        }
        for (v, _) in aggregates {
            columns.insert(v.as_str().to_string());
        }
        aggregated_lf
    }

    fn join_tsq(
        columns: &mut HashSet<String>,
        input_lf: LazyFrame,
        tsq: TimeSeriesQuery,
        df: DataFrame,
    ) -> LazyFrame {
        let mut join_on = vec![];
        for c in df.get_column_names() {
            if columns.contains(c) {
                join_on.push(col(c));
            } else {
                columns.insert(c.to_string());
            }
        }
        assert!(columns.contains(tsq.identifier_variable.as_ref().unwrap().as_str()));
        let mut output_lf = input_lf.join(
            df.lazy(),
            join_on.as_slice(),
            join_on.as_slice(),
            JoinType::Inner,
        );

        output_lf = output_lf.drop_columns([tsq.identifier_variable.as_ref().unwrap().as_str()]);
        columns.remove(tsq.identifier_variable.as_ref().unwrap().as_str());
        output_lf
    }

    fn lazy_triple_pattern(
        columns: &mut HashSet<String>,
        input_lf: LazyFrame,
        triple_pattern: &TriplePattern,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
        context: &Context
    ) -> LazyFrame {
        let mut found_index = None;
        if let NamedNodePattern::NamedNode(pn) = &triple_pattern.predicate {
            if pn.as_str() == HAS_VALUE {
                if let TermPattern::Variable(obj_var) = &triple_pattern.object {
                    if !columns.contains(obj_var.as_str()) {
                        for i in 0..time_series.len() {
                            let (tsq, _) = time_series.get(i).unwrap();
                            if tsq.value_variable.as_ref().is_some() && tsq.value_variable.as_ref().unwrap().equivalent(obj_var, context) {
                                found_index = Some(i);
                                break;
                            }
                        }
                    }
                }
            }
        }

        if let Some(i) = found_index {
            let (tsq, df) = time_series.remove(i);
            return Combiner::join_tsq(columns, input_lf, tsq, df);
        }
        input_lf
    }

    fn lazy_order_expression(
        oexpr: &OrderExpression,
        lazy_frame: LazyFrame,
        columns: &HashSet<String>,
        column_name: &str,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
        context: &Context
    ) -> (LazyFrame, bool) {
        match oexpr {
            OrderExpression::Asc(expr) => (
                Combiner::lazy_expression(expr, lazy_frame, columns, column_name, time_series, &context.extension_with(PathEntry::OrderingOperation)),
                true,
            ),
            OrderExpression::Desc(expr) => (
                Combiner::lazy_expression(expr, lazy_frame, columns, column_name, time_series, &context.extension_with(PathEntry::OrderingOperation)),
                false,
            ),
        }
    }

    pub fn lazy_expression(
        expr: &Expression,
        inner_lf: LazyFrame,
        columns: &HashSet<String>,
        column_name: &str,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
        context:&Context
    ) -> LazyFrame {
        match expr {
            Expression::NamedNode(nn) => {
                let inner_lf = inner_lf.with_column(
                    Expr::Literal(sparql_named_node_to_polars_literal_value(nn)).alias(column_name),
                );
                inner_lf
            }
            Expression::Literal(lit) => {
                let inner_lf = inner_lf.with_column(
                    Expr::Literal(sparql_literal_to_polars_literal_value(lit)).alias(column_name),
                );
                inner_lf
            }
            Expression::Variable(v) => {
                let inner_lf = inner_lf.with_column(col(v.as_str()).alias(column_name));
                inner_lf
            }
            Expression::Or(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::OrLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::OrRight)
                );
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::Or,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::And(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::AndLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::AndRight)
                );
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::And,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::Equal(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::EqualLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::EqualRight)
                );
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::Eq,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::SameTerm(_, _) => {
                todo!("Not implemented")
            }
            Expression::Greater(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::GreaterLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::GreaterRight)
                );
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::Gt,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name)
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::GreaterOrEqualLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::GreaterOrEqualRight)
                );

                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::GtEq,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::Less(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::LessLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::LessRight)
                );

                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::Lt,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::LessOrEqual(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::LessOrEqualLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::LessOrEqualRight)
                );

                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::LtEq,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::In(left, right) => {
                let left_colname = column_name.to_string() + "_left";
                let right_colnames: Vec<String> = (0..right.len())
                    .map(|i| column_name.to_string() + "right_" + &i.to_string())
                    .collect();
                let mut inner_lf =
                    Combiner::lazy_expression(left, inner_lf, columns, &left_colname, time_series, &context.extension_with(PathEntry::InLeft));
                for i in 0..right.len() {
                    let expr = right.get(i).unwrap();
                    inner_lf = Combiner::lazy_expression(
                        expr,
                        inner_lf,
                        columns,
                        right_colnames.get(i).unwrap(),
                        time_series,
                        &context.extension_with(PathEntry::InRight)
                    );
                }
                let mut expr = Expr::Literal(LiteralValue::Boolean(false));

                for right_colname in &right_colnames {
                    expr = Expr::BinaryExpr {
                        left: Box::new(expr),
                        op: Operator::Or,
                        right: Box::new(Expr::BinaryExpr {
                            left: Box::new(col(&left_colname)),
                            op: Operator::Eq,
                            right: Box::new(col(right_colname)),
                        }),
                    }
                }
                inner_lf = inner_lf
                    .with_column(expr.alias(column_name))
                    .drop_columns([&left_colname])
                    .drop_columns(right_colnames.iter().collect::<Vec<&String>>());
                inner_lf
            }
            Expression::Add(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::AddLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::AddRight)
                );
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::Plus,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::Subtract(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::SubtractLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::SubtractRight)
                );
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::Minus,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::Multiply(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::MultiplyLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::MultiplyRight)
                );

                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::Multiply,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::Divide(left, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::DivideLeft)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::DivideRight)
                );

                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(&left_column_name)),
                            op: Operator::Divide,
                            right: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &right_column_name]);
                inner_lf
            }
            Expression::UnaryPlus(inner) => {
                let plus_column_name = column_name.to_string() + "_plus";
                let mut inner_lf = Combiner::lazy_expression(
                    inner,
                    inner_lf,
                    columns,
                    &plus_column_name,
                    time_series,
                    &context.extension_with(PathEntry::UnaryPlus)
                );
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                            op: Operator::Plus,
                            right: Box::new(col(&plus_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&plus_column_name]);
                inner_lf
            }
            Expression::UnaryMinus(inner) => {
                let minus_column_name = column_name.to_string() + "_minus";
                let mut inner_lf = Combiner::lazy_expression(
                    inner,
                    inner_lf,
                    columns,
                    &minus_column_name,
                    time_series,
                    &context.extension_with(PathEntry::UnaryMinus)
                );
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                            op: Operator::Minus,
                            right: Box::new(col(&minus_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&minus_column_name]);
                inner_lf
            }
            Expression::Not(inner) => {
                let not_column_name = column_name.to_string() + "_not";
                let mut inner_lf = Combiner::lazy_expression(
                    inner,
                    inner_lf,
                    columns,
                    &not_column_name,
                    time_series,
                    &context.extension_with(PathEntry::Not)
                );
                inner_lf = inner_lf
                    .with_column(col(&not_column_name).not().alias(column_name))
                    .drop_columns([&not_column_name]);
                inner_lf
            }
            Expression::Exists(inner) => {
                let exists_helper_column = column_name.to_string() + "_exists_helper";
                let lf = inner_lf.with_column(
                    Expr::Literal(LiteralValue::Int64(1)).alias(&exists_helper_column),
                );
                let mut df = lf
                    .with_column(col(&exists_helper_column).cumsum(false).keep_name())
                    .collect()
                    .expect("Collect lazy error");
                let mut combiner = Combiner::new();
                let new_inner = rewrite_exists_graph_pattern(inner, &exists_helper_column);
                let exists_df = combiner
                    .lazy_graph_pattern(
                        &mut columns.clone(),
                        df.clone().lazy(),
                        &new_inner,
                        time_series,
                        &context.extension_with(PathEntry::Exists)
                    )
                    .select([col(&exists_helper_column)])
                    .unique(None, UniqueKeepStrategy::First)
                    .collect()
                    .expect("Collect lazy exists error");
                debug!("Exists dataframe: {}", exists_df);
                debug!("Exists original dataframe: {}", df);
                let mut ser = Series::from(
                    df.column(&exists_helper_column)
                        .unwrap()
                        .is_in(exists_df.column(&exists_helper_column).unwrap())
                        .unwrap(),
                );
                ser.rename(&column_name);
                df.with_column(ser).unwrap();
                df = df.drop(&exists_helper_column).unwrap();
                debug!("Dataframe after {}", df);
                df.lazy()
            }
            Expression::Bound(v) => {
                inner_lf.with_column(col(v.as_str()).is_null().alias(column_name))
            }
            Expression::If(left, middle, right) => {
                let left_column_name = column_name.to_string() + "_left";
                let mut inner_lf = Combiner::lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    &left_column_name,
                    time_series,
                    &context.extension_with(PathEntry::IfLeft)
                );
                let middle_column_name = column_name.to_string() + "_middle";
                inner_lf = Combiner::lazy_expression(
                    middle,
                    inner_lf,
                    columns,
                    &middle_column_name,
                    time_series,
                    &context.extension_with(PathEntry::IfMiddle)
                );
                let right_column_name = column_name.to_string() + "_right";
                inner_lf = Combiner::lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    &right_column_name,
                    time_series,
                    &context.extension_with(PathEntry::IfRight)
                );

                inner_lf = inner_lf
                    .with_column(
                        (Expr::Ternary {
                            predicate: Box::new(col(&left_column_name)),
                            truthy: Box::new(col(&middle_column_name)),
                            falsy: Box::new(col(&right_column_name)),
                        })
                        .alias(column_name),
                    )
                    .drop_columns([&left_column_name, &middle_column_name, &right_column_name]);
                inner_lf
            }
            Expression::Coalesce(inner) => {
                let mut inner_columns: Vec<String> = (0..inner.len())
                    .map(|i| column_name.to_string() + "_coalesce_arg_" + &i.to_string())
                    .collect();
                let mut inner_lf = inner_lf;
                for i in 0..inner.len() {
                    inner_lf = Combiner::lazy_expression(
                        inner.get(i).unwrap(),
                        inner_lf,
                        columns,
                        inner_columns.get(i).unwrap(),
                        time_series,
                        &context.extension_with(PathEntry::Coalesce)
                    );
                }

                let mut coalesced = col(&inner_columns.remove(0));
                for c in &inner_columns {
                    coalesced = Expr::Ternary {
                        predicate: Box::new(Expr::IsNotNull(Box::new(coalesced.clone()))),
                        truthy: Box::new(coalesced.clone()),
                        falsy: Box::new(col(c)),
                    }
                }
                inner_lf = inner_lf
                    .with_column(coalesced.alias(column_name))
                    .drop_columns(inner_columns.iter().collect::<Vec<&String>>());
                inner_lf
            }
            Expression::FunctionCall(func, args) => {
                let args_cols: Vec<String> = (0..args.len())
                    .map(|i| column_name.to_string() + "_function_arg_" + &i.to_string())
                    .collect();
                let mut inner_lf = inner_lf;
                for i in 0..args.len() {
                    inner_lf = Combiner::lazy_expression(
                        args.get(i).unwrap(),
                        inner_lf,
                        columns,
                        args_cols.get(i).unwrap(),
                        time_series,
                        &context.extension_with(PathEntry::FunctionCall)
                    );
                }
                match func {
                    Function::Year => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf =
                            inner_lf.with_column(col(first_col).dt().year().alias(column_name));
                    }
                    Function::Month => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf =
                            inner_lf.with_column(col(first_col).dt().month().alias(column_name));
                    }
                    Function::Day => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf =
                            inner_lf.with_column(col(first_col).dt().day().alias(column_name));
                    }
                    Function::Hours => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf =
                            inner_lf.with_column(col(first_col).dt().hour().alias(column_name));
                    }
                    Function::Minutes => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf =
                            inner_lf.with_column(col(first_col).dt().minute().alias(column_name));
                    }
                    Function::Seconds => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf =
                            inner_lf.with_column(col(first_col).dt().second().alias(column_name));
                    }
                    Function::Abs => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf = inner_lf.with_column(col(first_col).abs().alias(column_name));
                    }
                    Function::Ceil => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf = inner_lf.with_column(col(first_col).ceil().alias(column_name));
                    }
                    Function::Floor => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf = inner_lf.with_column(col(first_col).floor().alias(column_name));
                    }
                    Function::Concat => {
                        assert!(args.len() > 1);
                        inner_lf = inner_lf.with_column(
                            concat_str(args_cols.iter().map(|c| col(c)).collect(), "")
                                .alias(column_name),
                        );
                    }
                    Function::Round => {
                        assert_eq!(args.len(), 1);
                        let first_col = args_cols.get(0).unwrap();
                        inner_lf = inner_lf.with_column(col(first_col).round(0).alias(column_name));
                    }
                    Function::Custom(nn) => {
                        let nn_ref = NamedNodeRef::from(nn);
                        match nn_ref {
                            xsd::INTEGER => {
                                assert_eq!(args.len(), 1);
                                let first_col = args_cols.get(0).unwrap();
                                inner_lf = inner_lf.with_column(
                                    col(first_col).cast(DataType::Int64).alias(column_name),
                                );
                            }
                            xsd::STRING => {
                                assert_eq!(args.len(), 1);
                                let first_col = args_cols.get(0).unwrap();
                                inner_lf = inner_lf.with_column(
                                    col(first_col).cast(DataType::Utf8).alias(column_name),
                                );
                            }
                            _ => {
                                todo!("{:?}", nn)
                            }
                        }
                    }
                    _ => {
                        todo!("{:?}", func)
                    }
                }
                inner_lf.drop_columns(args_cols.iter().collect::<Vec<&String>>())
            }
        }
    }
}

pub fn sparql_aggregate_expression_as_lazy_column_and_expression(
    variable: &Variable,
    aggregate_expression: &AggregateExpression,
    all_proper_column_names: &Vec<String>,
    columns: &HashSet<String>,
    column_name: &str,
    lf: LazyFrame,
    time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
    context: &Context
) -> (LazyFrame, Expr, bool) {
    let out_lf;
    let mut out_expr;
    let created_col;
    match aggregate_expression {
        AggregateExpression::Count { expr, distinct } => {
            if let Some(some_expr) = expr {
                out_lf =
                    Combiner::lazy_expression(some_expr, lf, columns, column_name, time_series, &context.extension_with(PathEntry::AggregationOperation));
                created_col = true;
                if *distinct {
                    out_expr = col(column_name).n_unique();
                } else {
                    out_expr = col(column_name).count();
                }
            } else {
                out_lf = lf;
                created_col = false;

                let columns_expr = Expr::Columns(all_proper_column_names.clone());
                if *distinct {
                    out_expr = columns_expr.n_unique();
                } else {
                    out_expr = columns_expr.unique();
                }
            }
        }
        AggregateExpression::Sum { expr, distinct } => {
            out_lf = Combiner::lazy_expression(expr, lf, columns, column_name, time_series, &context.extension_with(PathEntry::AggregationOperation));
            created_col = true;

            if *distinct {
                out_expr = col(column_name).unique().sum();
            } else {
                out_expr = col(column_name).sum();
            }
        }
        AggregateExpression::Avg { expr, distinct } => {
            out_lf = Combiner::lazy_expression(expr, lf, columns, column_name, time_series, &context.extension_with(PathEntry::AggregationOperation));
            created_col = true;

            if *distinct {
                out_expr = col(column_name).unique().mean();
            } else {
                out_expr = col(column_name).mean();
            }
        }
        AggregateExpression::Min { expr, distinct: _ } => {
            out_lf = Combiner::lazy_expression(expr, lf, columns, column_name, time_series, &context.extension_with(PathEntry::AggregationOperation));
            created_col = true;

            out_expr = col(column_name).min();
        }
        AggregateExpression::Max { expr, distinct: _ } => {
            out_lf = Combiner::lazy_expression(expr, lf, columns, column_name, time_series, &context.extension_with(PathEntry::AggregationOperation));
            created_col = true;

            out_expr = col(column_name).max();
        }
        AggregateExpression::GroupConcat {
            expr,
            distinct,
            separator,
        } => {
            out_lf = Combiner::lazy_expression(expr, lf, columns, column_name, time_series, &context.extension_with(PathEntry::AggregationOperation));
            created_col = true;

            let use_sep = if let Some(sep) = separator {
                sep.to_string()
            } else {
                "".to_string()
            };
            if *distinct {
                out_expr = col(column_name)
                    .cast(Utf8)
                    .list()
                    .apply(
                        move |s| {
                            Ok(s.unique_stable()
                                .expect("Unique stable error")
                                .str_concat(use_sep.as_str())
                                .into_series())
                        },
                        GetOutput::from_type(Utf8),
                    )
                    .first();
            } else {
                out_expr = col(column_name)
                    .cast(Utf8)
                    .list()
                    .apply(
                        move |s| Ok(s.str_concat(use_sep.as_str()).into_series()),
                        GetOutput::from_type(Utf8),
                    )
                    .first();
            }
        }
        AggregateExpression::Sample { expr, .. } => {
            out_lf = Combiner::lazy_expression(expr, lf, columns, column_name, time_series, &context.extension_with(PathEntry::AggregationOperation));
            created_col = true;

            out_expr = col(column_name).first();
        }
        AggregateExpression::Custom { .. } => {
            out_lf = todo!();
            out_expr = todo!();
            created_col = todo!();
        }
    }
    out_expr = out_expr.alias(variable.as_str());
    (out_lf, out_expr, created_col)
}

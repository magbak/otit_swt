use crate::constants::HAS_VALUE;
use crate::rewriting::hash_graph_pattern;
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::{Variable};
use polars::frame::DataFrame;
use polars::prelude::{col, concat, Expr, IntoLazy, JoinType, LazyFrame, LiteralValue, Operator, UniqueKeepStrategy};
use polars::series::Series;
use sparesults::QuerySolution;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashSet;
use std::sync::Arc;
use crate::sparql_result_to_polars::{sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value};

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
        if let Query::Select {
            dataset: _,
            pattern,
            base_iri: _,
        } = &query
        {
            if let GraphPattern::Project { inner, variables } = pattern {
                project_variables = variables.clone();
                inner_graph_pattern = inner;
            } else {
                panic!("Wrong!!!");
            }
        } else {
            panic!("Wrong!!!");
        }
        let mut columns = static_result_df.get_column_names()
            .iter()
            .map(|c| c.to_string())
            .collect();

        let mut lf = static_result_df.lazy();
        lf = self.lazy_graph_pattern(&mut columns, lf, inner_graph_pattern, time_series);
        let projections = project_variables
            .iter()
            .map(|c| col(c.as_str()))
            .collect::<Vec<Expr>>();
        lf = lf.select(projections.as_slice());
        lf
    }

    fn lazy_graph_pattern(
        &mut self,
        columns: &mut HashSet<String>,
        input_lf: LazyFrame,
        graph_pattern: &GraphPattern,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
    ) -> LazyFrame {
        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                //No action, handled statically
                let mut output_lf = input_lf;
                for p in patterns {
                    output_lf = Combiner::lazy_triple_pattern(columns, output_lf, p, time_series);
                }
                output_lf
            }
            GraphPattern::Path { .. } => {
                //No action, handled statically
                input_lf
            }
            GraphPattern::Join { left, right } => {
                let left_lf = self.lazy_graph_pattern(columns, input_lf, left, time_series);
                let right_lf = self.lazy_graph_pattern(columns, left_lf, right, time_series);
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
                let input_lf = input_lf.with_column(
                    col(columns.iter().next().unwrap())
                        .cumcount(false)
                        .alias(&left_join_distinct_column),
                );
                let left_lf = self.lazy_graph_pattern(columns, input_lf, left, time_series);
                let mut right_lf =
                    self.lazy_graph_pattern(columns, left_lf.clone(), right, time_series);
                if let Some(expr) = expression {
                    right_lf = right_lf.filter(Combiner::lazy_expression(expr))
                }
                let mut output_lf = concat(vec![left_lf, right_lf], false).expect("Concat error");
                output_lf = output_lf
                    .unique(
                        Some(vec![left_join_distinct_column.clone()]),
                        UniqueKeepStrategy::Last,
                    )
                    .drop_columns(&[&left_join_distinct_column]);

                output_lf
            }
            GraphPattern::Filter { expr, inner } => {
                let inner_lf = self.lazy_graph_pattern(columns, input_lf, inner, time_series);
                Combiner::lazy_filter(inner_lf, expr)
            }
            GraphPattern::Union { left, right } => {
                let union_distinct_column =
                    "union_distinct_column".to_string() + &self.counter.to_string();
                self.counter += 1;
                let new_input_df = input_lf.with_column(
                    col(columns.iter().next().unwrap())
                        .cumcount(false)
                        .alias(&union_distinct_column),
                );
                let left_lf =
                    self.lazy_graph_pattern(columns, new_input_df.clone(), left, time_series);
                let right_lf = self.lazy_graph_pattern(columns, new_input_df, right, time_series);
                let output_lf = concat(vec![left_lf, right_lf], false).expect("Concat problem");
                output_lf
                    .unique(None, UniqueKeepStrategy::First)
                    .drop_columns(&[&union_distinct_column])
            }
            GraphPattern::Graph { name, inner } => {
                todo!()
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                let mut inner_lf = self.lazy_graph_pattern(columns, input_lf, inner, time_series);
                let lazy_expr = Combiner::lazy_expression(expression);
                inner_lf = inner_lf.with_column(lazy_expr.alias(variable.as_str()));
                columns.insert(variable.as_str().to_string());
                inner_lf
            }
            GraphPattern::Minus { left, right } => {
                let minus_column = "minus_column".to_string() + &self.counter.to_string();
                let left_lf = self.lazy_graph_pattern(columns, input_lf, left, time_series);
                let right_lf =
                    self.lazy_graph_pattern(columns, left_lf.clone(), right, time_series);
                let mut output_lf = concat(vec![left_lf, right_lf], false).expect("Noprob");
                output_lf = output_lf
                    .filter(col(&minus_column).is_duplicated().not())
                    .drop_columns(&[&minus_column]);
                output_lf
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => {
                todo!()
            }
            GraphPattern::OrderBy { inner, expression } => {
                let inner_lf = self.lazy_graph_pattern(columns, input_lf, inner, time_series);
                let lazy_exprs = expression
                    .iter()
                    .map(|o| Combiner::lazy_order_expression(o))
                    .collect::<Vec<(Expr, bool)>>();
                inner_lf.sort_by_exprs(
                    lazy_exprs
                        .iter()
                        .map(|(e, _)| e.clone())
                        .collect::<Vec<Expr>>(),
                    lazy_exprs.iter().map(|(_, asc)| asc.clone()).collect(),
                )
            }
            GraphPattern::Project { inner, variables } => {
                todo!()
            }
            GraphPattern::Distinct { inner } => self
                .lazy_graph_pattern(columns, input_lf, inner, time_series)
                .unique(None, UniqueKeepStrategy::First),
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
    ) -> LazyFrame {
        let lazy_inner = self.lazy_graph_pattern(columns,input_lf, inner, time_series);
        let by:Vec<Expr> = variables.iter().map(|v|col(v.as_str())).collect();
        let lazy_group_by = lazy_inner.groupby(by.as_slice());
        let mut column_variables = vec![];
        'outer: for v in columns.iter() {
            for (tsq, _) in time_series.iter() {
                if tsq.identifier_variable.as_ref().unwrap().as_str() == v {
                    continue 'outer;
                }
            }
            column_variables.push(v.clone());
        }

        let mut grouped_concats = vec![];
        let mut aggregate_expressions = vec![];
        for (v, a) in aggregates {
            let (agg, is_grouped_concat) = sparql_aggregate_expression_as_agg_expr(v,a, &column_variables);
            aggregate_expressions.push(agg);
            if is_grouped_concat {
                grouped_concats.push(v);
            }
        }
        let aggregated_lf = lazy_group_by.agg(aggregate_expressions.as_slice());
        columns.clear();
        for v in variables {
            columns.insert(v.as_str().to_string());
        }
        for (v,_) in aggregates {
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
    ) -> LazyFrame {
        let mut found_index = None;
        if let NamedNodePattern::NamedNode(pn) = &triple_pattern.predicate {
            if pn.as_str() == HAS_VALUE {
                if let TermPattern::Variable(obj_var) = &triple_pattern.object {
                    if !columns.contains(obj_var.as_str()) {
                        for i in 0..time_series.len() {
                            let (tsq, _) = time_series.get(i).unwrap();
                            if tsq.value_variable.as_ref() == Some(obj_var) {
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

    fn lazy_filter(input_lf: LazyFrame, expression: &Expression) -> LazyFrame {
        input_lf.filter(Combiner::lazy_expression(expression))
    }

    fn lazy_order_expression(oexpr: &OrderExpression) -> (Expr, bool) {
        match oexpr {
            OrderExpression::Asc(expr) => (Combiner::lazy_expression(expr), true),
            OrderExpression::Desc(expr) => (Combiner::lazy_expression(expr), false),
        }
    }

    pub fn lazy_expression(expr: &Expression) -> Expr {
        match expr {
            Expression::NamedNode(nn) => {
                Expr::Literal(sparql_named_node_to_polars_literal_value(nn))
            }
            Expression::Literal(lit) => Expr::Literal(sparql_literal_to_polars_literal_value(lit)),
            Expression::Variable(v) => Expr::Column(Arc::from(v.as_str())),
            Expression::Or(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::Or,
                    right: Box::new(right_expr),
                }
            }
            Expression::And(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::And,
                    right: Box::new(right_expr),
                }
            }
            Expression::Equal(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::Eq,
                    right: Box::new(right_expr),
                }
            }
            Expression::SameTerm(_, _) => {
                todo!("Not implemented")
            }
            Expression::Greater(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::Gt,
                    right: Box::new(right_expr),
                }
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::GtEq,
                    right: Box::new(right_expr),
                }
            }
            Expression::Less(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::Lt,
                    right: Box::new(right_expr),
                }
            }
            Expression::LessOrEqual(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::LtEq,
                    right: Box::new(right_expr),
                }
            }
            Expression::In(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_exprs = right.iter().map(|r| Combiner::lazy_expression(r));
                let mut expr = Expr::Literal(LiteralValue::Boolean(false));
                for r in right_exprs {
                    expr = Expr::BinaryExpr {
                        left: Box::new(expr),
                        op: Operator::Or,
                        right: Box::new(Expr::BinaryExpr {
                            left: Box::new(left_expr.clone()),
                            op: Operator::Eq,
                            right: Box::new(r),
                        }),
                    }
                }
                expr
            }
            Expression::Add(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::Plus,
                    right: Box::new(right_expr),
                }
            }
            Expression::Subtract(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::Minus,
                    right: Box::new(right_expr),
                }
            }
            Expression::Multiply(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::Multiply,
                    right: Box::new(right_expr),
                }
            }
            Expression::Divide(left, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let right_expr = Combiner::lazy_expression(right);
                Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: Operator::Divide,
                    right: Box::new(right_expr),
                }
            }
            Expression::UnaryPlus(inner) => {
                let inner_expr = Combiner::lazy_expression(inner);
                inner_expr
            }
            Expression::UnaryMinus(inner) => {
                let inner_expr = Combiner::lazy_expression(inner);
                Expr::BinaryExpr {
                    left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                    op: Operator::Minus,
                    right: Box::new(inner_expr),
                }
            }
            Expression::Not(inner) => {
                let inner_expr = Combiner::lazy_expression(inner);
                Expr::Not(Box::new(inner_expr))
            }
            Expression::Exists(_) => {
                todo!()
            }
            Expression::Bound(v) => Expr::IsNotNull(Box::new(Expr::Column(Arc::from(v.as_str())))),
            Expression::If(left, middle, right) => {
                let left_expr = Combiner::lazy_expression(left);
                let middle_expr = Combiner::lazy_expression(middle);
                let right_expr = Combiner::lazy_expression(right);
                Expr::Ternary {
                    predicate: Box::new(left_expr),
                    truthy: Box::new(middle_expr),
                    falsy: Box::new(right_expr),
                }
            }
            Expression::Coalesce(inner) => {
                let mut inner_exprs = inner
                    .iter()
                    .map(|e| Combiner::lazy_expression(e))
                    .collect::<Vec<Expr>>();
                let mut coalesced = inner_exprs.remove(0);
                for c in inner_exprs {
                    coalesced = Expr::Ternary {
                        predicate: Box::new(Expr::IsNotNull(Box::new(coalesced.clone()))),
                        truthy: Box::new(coalesced.clone()),
                        falsy: Box::new(c),
                    }
                }
                coalesced
            }
            Expression::FunctionCall(_, _) => {
                todo!()
            }
        }
    }
}



pub fn sparql_aggregate_expression_as_agg_expr(
        variable: &Variable,
        aggregate_expression: &AggregateExpression,
        all_proper_column_names: &Vec<String>,
    ) -> (Expr,bool) {
        let new_col;
        let mut is_group_concat = false;
        match aggregate_expression {
            AggregateExpression::Count { expr, distinct } => {
                if let Some(some_expr) = expr {
                    let lazy_expr = Combiner::lazy_expression(some_expr);
                    if *distinct {
                        new_col = lazy_expr.n_unique();
                    } else {
                        new_col = lazy_expr.count();
                    }
                } else {
                    let columns_expr = Expr::Columns(all_proper_column_names.clone()
                    );
                    if *distinct {
                        new_col = columns_expr.n_unique();
                    } else {
                        new_col = columns_expr.unique();
                    }
                }
            }
            AggregateExpression::Sum { expr, distinct } => {
                let lazy_expr = Combiner::lazy_expression(expr);
                if *distinct {
                    new_col = lazy_expr.unique().sum();
                } else {
                    new_col = lazy_expr.sum();
                }
            }
            AggregateExpression::Avg { expr, distinct } => {
                let lazy_expr = Combiner::lazy_expression(expr);
                if *distinct {
                    new_col = lazy_expr.unique().mean();
                } else {
                    new_col = lazy_expr.mean();
                }
            }
            AggregateExpression::Min { expr, distinct: _ } => {
                let lazy_expr = Combiner::lazy_expression(expr);
                    new_col = lazy_expr.min();
            }
            AggregateExpression::Max { expr, distinct: _ } => {
                let lazy_expr = Combiner::lazy_expression(expr);
                    new_col = lazy_expr.max();
            }
            AggregateExpression::GroupConcat { expr, distinct, separator } => {
                let lazy_expr = Combiner::lazy_expression(expr);
                let use_sep = if let Some(sep) = separator { sep.to_string() } else { "".to_string()};
                    new_col = lazy_expr.list();
                is_group_concat = true;
            }
            AggregateExpression::Sample { expr,.. } => {
                let lazy_expr = Combiner::lazy_expression(expr);
                new_col = lazy_expr.first();
            }
            AggregateExpression::Custom { .. } => {new_col = todo!();},
        }
        (new_col.alias(variable.as_str()), is_group_concat)
}

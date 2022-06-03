use crate::constants::HAS_VALUE;
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use polars::datatypes::TimeUnit;
use polars::export::chrono::NaiveDateTime;
use polars::frame::DataFrame;
use polars::prelude::{
    col, concat, Expr, IntoLazy, JoinType, LazyFrame, LiteralValue, NamedFrom, Operator,
    UniqueKeepStrategy,
};
use polars::series::Series;
use sparesults::QuerySolution;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::{Literal, NamedNodePattern, Term, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use crate::rewriting::hash_graph_pattern;

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
        static_query: Query,
        sparql_result: Vec<QuerySolution>,
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
        let column_variables;
        if let Query::Select {
            dataset: _,
            pattern,
            base_iri: _,
        } = static_query
        {
            if let GraphPattern::Project { inner, variables } = pattern {
                column_variables = variables.clone();
            } else {
                panic!("");
            }
        } else {
            panic!("");
        }

        let mut series_vec = vec![];
        for c in &column_variables {
            let literal_values = sparql_result
                .iter()
                .map(|x| {
                    if let Some(term) = x.get(c) {
                        sparql_term_to_polars_literal_value(term)
                    } else {
                        LiteralValue::Null
                    }
                })
                .collect();
            let series = polars_literal_values_to_series(literal_values, c.as_str());
            series_vec.push(series);
        }
        let lf = DataFrame::new(series_vec)
            .expect("Create df problem")
            .lazy();
        let mut columns = column_variables
            .iter()
            .map(|v| v.as_str().to_string())
            .collect();
        let mut result_lf =
            self.lazy_graph_pattern(&mut columns, lf, inner_graph_pattern, time_series);
        let projections = project_variables
            .iter()
            .map(|c| col(c.as_str()))
            .collect::<Vec<Expr>>();
        result_lf = result_lf.select(projections.as_slice());
        result_lf
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
                todo!()
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
                    self.lazy_group_without_pushdown(columns, input_lf, inner, variables, aggregates, time_series)
                }
            }
            GraphPattern::Service { .. } => {
                todo!()
            }
        }
    }

    fn lazy_group_without_pushdown(&self, columns: &mut HashSet<String>, input_lf: LazyFrame, inner: &Box<GraphPattern>, variables: &Vec<Variable>, aggregates: &Vec<(Variable, AggregateExpression)>, time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>) -> LazyFrame {
        todo!()
    }

    fn join_tsq(columns: &mut HashSet<String>, input_lf:LazyFrame, tsq:TimeSeriesQuery, df:DataFrame) -> LazyFrame {
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

            output_lf =
                output_lf.drop_columns([tsq.identifier_variable.as_ref().unwrap().as_str()]);
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

fn sparql_term_to_polars_literal_value(term: &Term) -> LiteralValue {
    match term {
        Term::NamedNode(named_node) => sparql_named_node_to_polars_literal_value(named_node),
        Term::Literal(lit) => sparql_literal_to_polars_literal_value(lit),
        _ => {
            panic!("Not supported")
        }
    }
}

fn sparql_named_node_to_polars_literal_value(named_node: &NamedNode) -> LiteralValue {
    LiteralValue::Utf8(named_node.as_str().to_string())
}

fn sparql_literal_to_polars_literal_value(lit: &Literal) -> LiteralValue {
    let datatype = lit.datatype();
    let value = lit.value();
    let literal_value = if datatype == xsd::STRING {
        LiteralValue::Utf8(value.to_string())
    } else if datatype == xsd::INTEGER {
        let i = i32::from_str(value).expect("Integer parsing error");
        LiteralValue::Int32(i)
    } else if datatype == xsd::BOOLEAN {
        let b = bool::from_str(value).expect("Boolean parsing error");
        LiteralValue::Boolean(b)
    } else if datatype == xsd::DATE_TIME {
        let dt = value
            .parse::<NaiveDateTime>()
            .expect("Datetime parsing error");
        LiteralValue::DateTime(dt, TimeUnit::Nanoseconds)
    } else {
        println!("{}", datatype.as_str());
        todo!("Not implemented!")
    };
    literal_value
}

fn polars_literal_values_to_series(literal_values: Vec<LiteralValue>, name: &str) -> Series {
    let first_non_null_opt = literal_values
        .iter()
        .find(|x| &&LiteralValue::Null != x)
        .cloned();
    let first_null_opt = literal_values
        .iter()
        .find(|x| &&LiteralValue::Null == x)
        .cloned();
    if let (Some(first_non_null), None) = (&first_non_null_opt, &first_null_opt) {
        match first_non_null {
            LiteralValue::Boolean(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Boolean(b) = x {
                            b
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<bool>>(),
            ),
            LiteralValue::Utf8(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Utf8(u) = x {
                            u
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<String>>(),
            ),
            LiteralValue::UInt32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt32(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<u32>>(),
            ),
            LiteralValue::UInt64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt64(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<u64>>(),
            ),
            LiteralValue::Int32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int32(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<i32>>(),
            ),
            LiteralValue::Int64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int64(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<i64>>(),
            ),
            LiteralValue::Float32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float32(f) = x {
                            f
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<f32>>(),
            ),
            LiteralValue::Float64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float64(f) = x {
                            Some(f)
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<Option<f64>>>(),
            ),
            LiteralValue::Range { .. } => {
                todo!()
            }
            LiteralValue::DateTime(_, t) =>
            //TODO: Assert time unit lik??
            {
                let s =
                Series::new(
                    name,
                    literal_values
                        .into_iter()
                        .map(|x| {
                            if let LiteralValue::DateTime(n, t_prime) = x {
                                assert_eq!(t, &t_prime);
                                n
                            } else {
                                panic!("Not possible")
                            }
                        })
                        .collect::<Vec<NaiveDateTime>>(),
                );
                println!("series: {}", s);
                s
            }
            LiteralValue::Duration(_, _) => {
                todo!()
            }
            LiteralValue::Series(_) => {
                todo!()
            }
            _ => {
                todo!()
            }
        }
    } else if let (Some(first_non_null), Some(_)) = (&first_non_null_opt, &first_null_opt) {
        match first_non_null {
            LiteralValue::Boolean(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Boolean(b) = x {
                            Some(b)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<bool>>>(),
            ),
            LiteralValue::Utf8(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Utf8(u) = x {
                            Some(u)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<String>>>(),
            ),
            LiteralValue::UInt32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt32(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<u32>>>(),
            ),
            LiteralValue::UInt64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt64(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<u64>>>(),
            ),
            LiteralValue::Int32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int32(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i32>>>(),
            ),
            LiteralValue::Int64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int64(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i64>>>(),
            ),
            LiteralValue::Float32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float32(f) = x {
                            Some(f)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<f32>>>(),
            ),
            LiteralValue::Float64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float64(f) = x {
                            Some(f)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<f64>>>(),
            ),
            LiteralValue::Range { .. } => {
                todo!()
            }
            LiteralValue::DateTime(_, t) =>
            //TODO: Assert time unit lik??
            {
                Series::new(
                    name,
                    literal_values
                        .into_iter()
                        .map(|x| {
                            if let LiteralValue::DateTime(n, t_prime) = x {
                                assert_eq!(t, &t_prime);
                                Some(n)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<NaiveDateTime>>>(),
                )
            }
            LiteralValue::Duration(_, _) => {
                todo!()
            }
            LiteralValue::Series(_) => {
                todo!()
            }
            _ => {
                todo!()
            }
        }
    } else {
        Series::new(
            name,
            literal_values
                .iter()
                .map(|_| None)
                .collect::<Vec<Option<bool>>>(),
        )
    }
}

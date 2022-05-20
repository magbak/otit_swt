use std::lazy::Lazy;
use std::str::FromStr;
use std::sync::Arc;
use oxrdf::vocab::xsd;
use polars::datatypes::BooleanChunked;
use polars::frame::DataFrame;
use polars::prelude::{BooleanChunkedBuilder, Expr, IntoLazy, LazyFrame, LiteralValue, NamedFrom, Operator, when};
use polars::series::{ChunkCompare, Series};
use sparesults::QuerySolution;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::Query;
use spargebra::term::{Literal, Term, TriplePattern, Variable};
use crate::timeseries_query::TimeSeriesQuery;


pub fn combine_static_and_time_series_results(query: Query, static_query:Query, time_series_queries: Vec<TimeSeriesQuery>, sparql_result:Vec<QuerySolution>, time_series_results:Vec<DataFrame>) {
    let column_variables ;
    let inner_graph_pattern;
    if let Some(Query::Select { dataset:_, pattern, base_iri:_ }) = &static_query {
        if let Some(GraphPattern::Project { inner, variables }) = pattern{
            column_variables = variables.clone();
            inner_graph_pattern = inner;
        } else {
            panic!("Wrong!!!");
        }
    } else {
        panic!("Wrong!!!");
    }

    let mut series = vec![];
    let first_soln = sparql_result.get(0).unwrap();
    for c in &column_variables {
        let data_type = match first_soln.get(c).expect("Variable exists in soln") {
            Term::NamedNode(_) => {}
            Term::BlankNode(_) => {}
            Term::Literal(lit) => {
                match lit { &_ => {} }
            }
            Term::Triple(_) => {}
        };
        series.push()
    }

    evaluate_graph_pattern(&mut result_df, inner_graph_pattern, time_series_queries, time_series_results);
}

struct Evaluator {
    result_df: DataFrame,
    time_series_queries: Vec<TimeSeriesQuery>
}

fn evaluate_graph_pattern(input_df: LazyFrame, graph_pattern: &GraphPattern, time_series_queries: Vec<TimeSeriesQuery>, time_series_results: Vec<DataFrame>) -> LazyFrame {
    match graph_pattern {
        GraphPattern::Bgp { patterns } => {
            //No action, handled statically
            let mut output_df = input_df;
            for p in patterns {
                output_df = evaluate_triple_pattern(output_df, p, time_series_queries, time_series_results)
            }
            output_df
        }
        GraphPattern::Path { .. } => {
            //No action, handled statically
            input_df
        }
        GraphPattern::Join {  } => {}
        GraphPattern::LeftJoin {  } => {}
        GraphPattern::Filter { expr, inner } => {
            let lf = evaluate_graph_pattern(result_df, inner, time_series_queries, time_series_results);
            evaluate_filter(lf, expr)
        }
        GraphPattern::Union {  } => {}
        GraphPattern::Graph {  } => {}
        GraphPattern::Extend {  } => {}
        GraphPattern::Minus { left, right } => {}
        GraphPattern::Values { variables, bindings } => {}
        GraphPattern::OrderBy { .. } => {}
        GraphPattern::Project { .. } => {}
        GraphPattern::Distinct { inner } => {}
        GraphPattern::Reduced { .. } => {}
        GraphPattern::Slice { .. } => {}
        GraphPattern::Group { .. } => {}
        GraphPattern::Service { .. } => {}
    }
}

fn evaluate_triple_pattern(input_lf: LazyFrame, triple_pattern: &TriplePattern, time_series_query: Vec<TimeSeriesQuery>, time_series_results: Vec<DataFrame>) -> LazyFrame {
    if triple_pattern.object.
}

fn evaluate_filter(result_df: LazyFrame, expression: &Expression) -> LazyFrame {
    result_df.lazy().filter(compute_expression(expression))
}

fn compute_expression(expr: &Expression) -> Expr {
    match expr {
        Expression::NamedNode(nn) => {
            Expr::Literal(LiteralValue::Utf8(nn.to_string()))
        }
        Expression::Literal(lit) => {
            let datatype = lit.datatype();
            if datatype == xsd::STRING {
                let s = lit.value();
                Expr::Literal(LiteralValue::Utf8(s.to_string()))
            } else if datatype == xsd::INTEGER {
                let i = i32::from_str(lit.value()).expect("Integer parsing error");
                Expr::Literal(LiteralValue::Int32(i))
            } else if datatype == xsd::BOOLEAN {
                let b = bool::from_str(lit.value()).expect("Boolean parsing error");
                Expr::Literal(LiteralValue::Boolean(b))
            }
        }
        Expression::Variable(v) => {
            Expr::Column(Arc::from(v.as_str()))
        }
        Expression::Or(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::Or,
                right: Box::new(right_expr)
            }
        }
        Expression::And(left, right)=> {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::And,
                right: Box::new(right_expr)
            }
        }
        Expression::Equal(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::Eq,
                right: Box::new(right_expr)
            }
        }
        Expression::SameTerm(_, _) => {
            todo!("Not implemented")
        }
        Expression::Greater(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::Gt,
                right: Box::new(right_expr)
            }
        }
        Expression::GreaterOrEqual(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::GtEq,
                right: Box::new(right_expr)
            }
        }
        Expression::Less(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::Lt,
                right: Box::new(right_expr)
            }
        }
        Expression::LessOrEqual(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::LtEq,
                right: Box::new(right_expr)
            }
        }
        Expression::In(left, right) => {
            let left_expr = compute_expression( left);
            let right_exprs = right.iter().map(|r| compute_expression( r));
            let mut expr = Expr::Literal(LiteralValue::Boolean(false));
            for r in right_exprs {
                expr = Expr::BinaryExpr {
                    left: Box::new(expr),
                    op: Operator::Or,
                    right: Box::new(Expr::BinaryExpr {
                        left: Box::new(left_expr.clone()),
                        op: Operator::Eq,
                        right: Box::new(r)
                    })
                }
            }
            expr
        }
        Expression::Add(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::Plus,
                right: Box::new(right_expr)
            }
        }
        Expression::Subtract(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::Minus,
                right: Box::new(right_expr)
            }
        }
        Expression::Multiply(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::Multiply,
                right: Box::new(right_expr)
            }
        }
        Expression::Divide(left, right) => {
            let left_expr = compute_expression( left);
            let right_expr = compute_expression( right);
            Expr::BinaryExpr {
                left: Box::new(left_expr),
                op: Operator::Divide,
                right: Box::new(right_expr)
            }
        }
        Expression::UnaryPlus(inner) => {
            let inner_expr = compute_expression( inner);
            inner_expr
        }
        Expression::UnaryMinus(inner) => {
            let inner_expr = compute_expression( inner);
            Expr::BinaryExpr{
                left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                op: Operator::Minus,
                right: Box::new(inner_expr)
            }
        }
        Expression::Not(inner) => {
            let inner_expr = compute_expression( inner);
            Expr::Not(Box::new(inner_expr))
        }
        Expression::Exists(_) => {
            todo!()
        }
        Expression::Bound(v) => {
            Expr::IsNotNull(Box::new(Expr::Column(Arc::from(v.as_str()))))
        }
        Expression::If(left, middle, right) => {
            let left_expr = compute_expression( left);
            let middle_expr = compute_expression( middle);
            let right_expr = compute_expression( right);
            Expr::Ternary{
                predicate: Box::new(left_expr),
                truthy: Box::new(middle_expr),
                falsy: Box::new(right_expr)}
        }
        Expression::Coalesce(inner) => {
            let mut inner_exprs = inner.iter().map(|e| compute_expression(e)).collect::<Vec<Expr>>();
            let mut coalesced = inner_exprs.remove(0);
            for c in inner_exprs {
                Expr::Ternary {
                    predicate: Box::new(Expr::IsNotNull(Box::new(coalesced.clone()))),
                    truthy: Box::new(coalesced.clone()),
                    falsy: Box::new(c)
                }
            }
            coalesced
        }
        Expression::FunctionCall(_, _) => {
            todo!()
        }
    }
}
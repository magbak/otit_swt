use std::collections::HashMap;
use std::error::Error;
use oxrdf::Variable;
use polars::frame::DataFrame;
use polars::prelude::{col, concat, Expr, IntoLazy, LazyFrame, LazyGroupBy, lit, LiteralValue, Operator};
use spargebra::algebra::{AggregateExpression, Expression};
use hybrid::combiner::{Combiner, sparql_aggregate_expression_as_agg_expr};
use hybrid::timeseries_database::TimeSeriesQueryable;
use hybrid::timeseries_query::TimeSeriesQuery;

pub struct InMemoryTimeseriesDatabase {
    pub frames: HashMap<String,DataFrame>
}

impl TimeSeriesQueryable for InMemoryTimeseriesDatabase {
    fn execute(&self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        assert!(tsq.ids.is_some() && !tsq.ids.as_ref().unwrap().is_empty());
        let mut lfs = vec![];
        for id in tsq.ids.as_ref().unwrap() {
            if let Some(df) = self.frames.get(id) {
                assert!(tsq.identifier_variable.is_some());
                let mut df = df.clone();

                if let Some(value_variable) = &tsq.value_variable {
                    df.rename("value", value_variable.as_str()).expect("Rename problem");
                }
                if let Some(timestamp_variable) = &tsq.timestamp_variable {
                    df.rename("timestamp", timestamp_variable.as_str()).expect("Rename problem");
                }
                let mut lf = df.lazy();
                lf = lf.with_column(lit(id.to_string()).alias(tsq.identifier_variable.as_ref().unwrap().as_str()));

                if tsq.conditions.len() > 0 {
                    let exprs: Vec<Expr> = tsq.conditions.iter().map(|c| Combiner::lazy_expression(c)).collect();
                    let expr = exprs.into_iter().fold(Expr::Literal(LiteralValue::Boolean(true)), |left, right| {
                        Expr::BinaryExpr {
                            left: Box::new(left),
                            op: Operator::And,
                            right: Box::new(right)
                        }
                    });
                    lf = lf.filter(expr);
                }


                lfs.push(lf);
            } else {
                panic!("Missing frame");
            }
        }
        let mut out_lf = concat(lfs, false)?;
        if let Some(grouping) = &tsq.grouping {
            //Important to do iteration in reversed direction for nested functions
            for (v, expression) in grouping.timeseries_funcs.iter().rev() {
                let lazy_expr = Combiner::lazy_expression(expression);
                out_lf = out_lf.with_column(lazy_expr.alias(v.as_str()));
            }
            let by_cols :Vec<Expr> = grouping.by.iter().map(|v|col(v.as_str())).collect();
            let grouped_lf = out_lf.groupby(by_cols.as_slice());
            let mut aggregation_exprs = vec![];
            let timestamp_name = if let Some(ts_var) = &tsq.timestamp_variable {ts_var.as_str().to_string()} else {"timestamp".to_string()};
            let timestamp_names = vec![timestamp_name];
            for (v,agg) in &grouping.aggregations {
                let (agg_expr,_) = sparql_aggregate_expression_as_agg_expr(v,agg, &timestamp_names);
                aggregation_exprs.push(agg_expr);
            }
            out_lf = grouped_lf.agg(aggregation_exprs.as_slice());
        }

        let collected = out_lf.collect()?;
        Ok(collected)
    }
}

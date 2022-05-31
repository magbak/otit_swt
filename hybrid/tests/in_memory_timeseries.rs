use std::error::Error;
use polars::frame::DataFrame;
use polars::prelude::{col, Expr, IntoLazy, LiteralValue, Operator};
use hybrid::combiner::Combiner;
use hybrid::timeseries_database::TimeSeriesQueryable;
use hybrid::timeseries_query::TimeSeriesQuery;

pub struct InMemoryTimeseriesDatabase {
    df: DataFrame
}

impl TimeSeriesQueryable for InMemoryTimeseriesDatabase {
    fn execute(&self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        let mut lf = self.df.clone().lazy(); //Should find workaround for clone
        assert!(tsq.identifier_variable.is_some());
        lf = lf.rename(["id"], [tsq.identifier_variable.as_ref().unwrap().as_str()]);
        if let Some(value_variable) = &tsq.value_variable {
            lf = lf.rename(["value"],[value_variable.as_str()])
        }
        if let Some(timestamp_variable) = &tsq.timestamp_variable {
            lf = lf.rename(["timestamp"], [timestamp_variable.as_str()])
        }
        if tsq.conditions.len() > 0 {
            let exprs = tsq.conditions.iter().map(|c| Combiner::lazy_expression(c));
            let expr = exprs.fold(Expr::Literal(LiteralValue::Boolean(true)), |left, right| {
                Expr::BinaryExpr {
                    left: Box::new(left),
                    op: Operator::And,
                    right: Box::new(right)
                }
            });
            lf = lf.filter(expr);
        }
        Ok(lf.collect()?)
    }
}
use std::collections::HashMap;
use std::error::Error;
use polars::frame::DataFrame;
use polars::prelude::{concat, Expr, IntoLazy, lit, LiteralValue, Operator};
use hybrid::combiner::Combiner;
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
        let collected = concat(lfs, false)?.collect()?;
        Ok(collected)
    }
}
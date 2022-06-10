use std::collections::{HashMap, HashSet};
use std::error::Error;
use polars::frame::DataFrame;
use polars::prelude::{col, concat, Expr, IntoLazy, lit, LiteralValue, Operator};
use hybrid::combiner::{Combiner, sparql_aggregate_expression_as_lazy_column_and_expression};
use hybrid::timeseries_database::TimeSeriesQueryable;
use hybrid::timeseries_query::TimeSeriesQuery;

pub struct InMemoryTimeseriesDatabase {
    pub frames: HashMap<String,DataFrame>
}

impl TimeSeriesQueryable for InMemoryTimeseriesDatabase {
    fn execute(&self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        assert!(tsq.ids.is_some() && !tsq.ids.as_ref().unwrap().is_empty());
        let mut lfs = vec![];
        let mut columns:HashSet<String> = HashSet::new();
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
                columns = HashSet::from_iter(df.get_column_names_owned().into_iter());
                let mut lf = df.lazy();
                lf = lf.with_column(lit(id.to_string()).alias(tsq.identifier_variable.as_ref().unwrap().as_str()));

                if tsq.conditions.len() > 0 {
                    let column_name = "tsq_condition_expression_column_name";
                    for expr in &tsq.conditions {
                        lf = Combiner::lazy_expression(expr, lf, &columns, column_name, &mut vec![]);
                        lf = lf.filter(col(column_name)).drop_columns([column_name]);
                    }
                }


                lfs.push(lf);
            } else {
                panic!("Missing frame");
            }
        }
        let mut out_lf = concat(lfs, false)?;
        if let Some(grouping) = &tsq.grouping {
            let grouping_expr_columns:Vec<String> = (grouping.timeseries_funcs.len()..0).map(|i|"grouping_column_name_".to_string() + &i.to_string()).collect();
            //Important to do iteration in reversed direction for nested functions
            for i in grouping.timeseries_funcs.len()..0 {
                let (v, expression) = grouping.timeseries_funcs.get(i).unwrap();
                let column_name = grouping_expr_columns.get(i).unwrap();
                out_lf = Combiner::lazy_expression(expression, out_lf, &columns, column_name,&mut vec![]);
            }
            let mut aggregation_exprs = vec![];
            let timestamp_name = if let Some(ts_var) = &tsq.timestamp_variable {ts_var.as_str().to_string()} else {"timestamp".to_string()};
            let timestamp_names = vec![timestamp_name];
            let mut aggregate_helper_columns = vec![];
            for i in 0..grouping.aggregations.len() {
                let (v, agg) = grouping.aggregations.get(i).unwrap();
                let aggregation_helper_column_name = "aggregation_helper_column_".to_string() + &i.to_string();
                let (lf, agg_expr, used_column) = sparql_aggregate_expression_as_lazy_column_and_expression(v, agg, &timestamp_names, &columns, &aggregation_helper_column_name, out_lf, &mut vec![]);
                out_lf = lf;
                aggregation_exprs.push(agg_expr);
                aggregate_helper_columns.push(aggregation_helper_column_name);
            }
            let by:Vec<Expr> = grouping_expr_columns.iter().map(|c|col(c)).collect();
            let grouped_lf = out_lf.groupby(by);
            out_lf = grouped_lf.agg(aggregation_exprs.as_slice()).drop_columns(aggregate_helper_columns.iter().collect::<Vec<&String>>());
        }

        let collected = out_lf.collect()?;
        Ok(collected)
    }
}

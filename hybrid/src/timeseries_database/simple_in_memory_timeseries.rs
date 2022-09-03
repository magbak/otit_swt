use crate::combiner::lazy_aggregate::sparql_aggregate_expression_as_lazy_column_and_expression;
use crate::combiner::lazy_expressions::lazy_expression;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::{
    BasicTimeSeriesQuery, GroupedTimeSeriesQuery, Synchronizer, TimeSeriesQuery,
};
use async_trait::async_trait;
use polars::frame::DataFrame;
use polars::prelude::{col, concat, lit, Expr, IntoLazy};
use polars_core::prelude::JoinType;
use spargebra::algebra::Expression;
use std::collections::HashMap;
use std::error::Error;

pub struct InMemoryTimeseriesDatabase {
    pub frames: HashMap<String, DataFrame>,
}

#[async_trait]
impl TimeSeriesQueryable for InMemoryTimeseriesDatabase {
    async fn execute(&mut self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        self.execute_query(tsq)
    }

    fn allow_compound_timeseries_queries(&self) -> bool {
        true
    }
}

impl InMemoryTimeseriesDatabase {
    fn execute_query(&self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        match tsq {
            TimeSeriesQuery::Basic(b) => self.execute_basic(b),
            TimeSeriesQuery::Filtered(inner, filter, _) => self.execute_filtered(inner, filter),
            TimeSeriesQuery::InnerSynchronized(inners, synchronizers) => {
                self.execute_inner_synchronized(inners, synchronizers)
            }
            TimeSeriesQuery::LeftSynchronized(_, _, _, _, _) => {
                unimplemented!()
            }
            TimeSeriesQuery::Grouped(grouped) => self.execute_grouped(grouped),
        }
    }

    fn execute_basic(&self, btsq: &BasicTimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        let mut lfs = vec![];
        for id in btsq.ids.as_ref().unwrap() {
            if let Some(df) = self.frames.get(id) {
                assert!(btsq.identifier_variable.is_some());
                let mut df = df.clone();

                if let Some(value_variable) = &btsq.value_variable {
                    df.rename("value", value_variable.variable.as_str())
                        .expect("Rename problem");
                } else {
                    df = df.drop("value").expect("Drop value problem");
                }
                if let Some(timestamp_variable) = &btsq.timestamp_variable {
                    df.rename("timestamp", timestamp_variable.variable.as_str())
                        .expect("Rename problem");
                } else {
                    df = df.drop("timestamp").expect("Drop timestamp problem");
                }
                let mut lf = df.lazy();
                lf = lf.with_column(
                    lit(id.to_string()).alias(btsq.identifier_variable.as_ref().unwrap().as_str()),
                );

                lfs.push(lf);
            } else {
                panic!("Missing frame");
            }
        }
        let out_lf = concat(lfs, true)?;
        Ok(out_lf.collect().unwrap())
    }

    fn execute_filtered(
        &self,
        tsq: &TimeSeriesQuery,
        filter: &Option<Expression>,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let df = self.execute_query(tsq)?;
        let columns = df
            .get_column_names()
            .into_iter()
            .map(|x| x.to_string())
            .collect();
        if let Some(filter) = filter {
            let tmp_context = Context::from_path(vec![PathEntry::Coalesce(12)]);
            let mut lf = lazy_expression(filter, df.lazy(), &columns, &mut vec![], &tmp_context);
            lf = lf
                .filter(col(tmp_context.as_str()))
                .drop_columns([tmp_context.as_str()]);
            Ok(lf.collect().unwrap())
        } else {
            Ok(df)
        }
    }

    fn execute_grouped(
        &self,
        grouped: &GroupedTimeSeriesQuery,
    ) -> Result<DataFrame, Box<dyn Error>> {
        //Important to do iteration in reversed direction for nested functions
        let df = self.execute_query(&grouped.tsq)?;
        let mut columns = df
            .get_column_names()
            .into_iter()
            .map(|x| x.to_string())
            .collect();
        let mut out_lf = df.lazy();
        for (v, expression) in grouped.timeseries_funcs.iter().rev() {
            out_lf = lazy_expression(
                &expression.expression,
                out_lf,
                &columns,
                &mut vec![],
                &expression.context,
            )
            .rename([expression.context.as_str()], [v.as_str()]);
            columns.insert(v.as_str().to_string());
        }
        let mut aggregation_exprs = vec![];
        let timestamp_name = if let Some(ts_var) = grouped.tsq.get_timestamp_variables().get(0) {
            ts_var.variable.as_str().to_string()
        } else {
            "timestamp".to_string()
        };
        let timestamp_names = vec![timestamp_name];
        let mut aggregate_inner_contexts = vec![];
        for i in 0..grouped.aggregations.len() {
            let (v, agg) = grouped.aggregations.get(i).unwrap();
            let (lf, agg_expr, used_context) =
                sparql_aggregate_expression_as_lazy_column_and_expression(
                    v,
                    &agg.aggregate_expression,
                    &timestamp_names,
                    &columns,
                    out_lf,
                    &mut vec![],
                    &agg.context,
                );
            out_lf = lf;
            aggregation_exprs.push(agg_expr);
            if let Some(inner_context) = used_context {
                aggregate_inner_contexts.push(inner_context);
            }
        }
        let by: Vec<Expr> = grouped.by.iter().map(|c| col(c.as_str())).collect();
        let grouped_lf = out_lf.groupby(by);
        out_lf = grouped_lf.agg(aggregation_exprs.as_slice()).drop_columns(
            aggregate_inner_contexts
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<&str>>(),
        );

        let collected = out_lf.collect()?;
        Ok(collected)
    }

    fn execute_inner_synchronized(
        &self,
        inners: &Vec<Box<TimeSeriesQuery>>,
        synchronizers: &Vec<Synchronizer>,
    ) -> Result<DataFrame, Box<dyn Error>> {
        assert_eq!(synchronizers.len(), 1);
        if let Synchronizer::Identity(timestamp_col) = synchronizers.get(0).unwrap() {
            let mut dfs = vec![];
            for q in inners {
                let df = self.execute_query(q)?;
                dfs.push(df);
            }
            let mut first_df = dfs.remove(0);
            for df in dfs.into_iter() {
                first_df = first_df.join(
                    &df,
                    [&timestamp_col],
                    [&timestamp_col],
                    JoinType::Inner,
                    None,
                )?;
            }
            Ok(first_df)
        } else {
            todo!()
        }
    }
}

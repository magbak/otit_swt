use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{col, JoinType};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use std::collections::HashSet;

pub fn join_tsq(
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
    let id_vars = tsq.get_identifier_variables();
    for id_var in &id_vars {
        assert!(columns.contains(id_var.as_str()));
    }
    let mut output_lf = input_lf.join(
        df.lazy(),
        join_on.as_slice(),
        join_on.as_slice(),
        JoinType::Inner,
    );

    let id_vars_names: Vec<&str> = id_vars.iter().map(|x| x.as_str()).collect();
    output_lf = output_lf.drop_columns(id_vars_names.as_slice());
    for var_name in id_vars_names {
        columns.remove(var_name);
    }
    output_lf
}

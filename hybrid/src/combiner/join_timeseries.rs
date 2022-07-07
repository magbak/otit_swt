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

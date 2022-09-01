use crate::timeseries_query::{Synchronizer, TimeSeriesQuery};

pub fn create_identity_synchronized_queries(
    mut tsqs: Vec<TimeSeriesQuery>,
) -> Vec<TimeSeriesQuery> {
    let mut out_queries = vec![];
    while tsqs.len() > 1 {
        let mut queries_to_synchronize = vec![];
        let first_query = tsqs.remove(0);
        let first_query_timestamp_variables = first_query.get_timestamp_variables();
        let mut keep_tsqs = vec![];
        for other in tsqs.into_iter() {
            if other.overlaps_timestamp_variables(first_query_timestamp_variables) {
                queries_to_synchronize.push(other)
            } else {
                keep_tsqs.push(other);
            }
        }
        tsqs = keep_tsqs;
        if !queries_to_synchronize.is_empty() {
            queries_to_synchronize.push(first_query);
            out_queries.push(TimeSeriesQuery::InnerSynchronized(
                queries_to_synchronize,
                vec![Synchronizer::Identity],
            ));
        } else {
            out_queries.push(first_query);
        }
    }
    out_queries
}

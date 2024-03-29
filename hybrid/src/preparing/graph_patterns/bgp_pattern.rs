use super::TimeSeriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::synchronization::create_identity_synchronized_queries;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;

impl TimeSeriesQueryPrepper {
    pub(crate) fn prepare_bgp(
        &mut self,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let context = context.extension_with(PathEntry::BGP);
        let mut local_tsqs = vec![];
        for tsq in &self.basic_time_series_queries {
            if let Some(dp_ctx) = &tsq.data_point_variable {
                if &dp_ctx.context == &context {
                    local_tsqs.push(TimeSeriesQuery::Basic(tsq.clone()));
                }
            }
        }
        if try_groupby_complex_query {
            local_tsqs = create_identity_synchronized_queries(local_tsqs);
        }
        GPPrepReturn::new(local_tsqs)
    }
}

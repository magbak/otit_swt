use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::constants::{HAS_DATATYPE, HAS_DATA_POINT, HAS_EXTERNAL_ID, HAS_TIMESTAMP, HAS_VALUE};
use crate::constraints::{Constraint, VariableConstraints};
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry, VariableInContext};
use crate::timeseries_query::synchronization::create_identity_synchronized_queries;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use oxrdf::{NamedNode, Variable};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use crate::preparing::synchronization::{create_identity_synchronized_queries};

impl TimeSeriesQueryPrepper {
    pub(crate) fn prepare_bgp(
        &mut self,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let context = context.extension_with(PathEntry::BGP);
        let mut local_tsqs = vec![];
        for tsq in &self.basic_time_series_queries {
            if let Some(id_ctx) = &tsq.timestamp_variable {
                if &id_ctx.context == context {
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
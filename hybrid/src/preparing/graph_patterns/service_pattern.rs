use super::TimeSeriesQueryPrepper;

use crate::query_context::{Context};
use spargebra::algebra::GraphPattern;
use spargebra::term::NamedNodePattern;
use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeSeriesQueryPrepper<'_> {
    pub fn prepare_service(
        &mut self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        silent: &bool,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        //Service pattern should not contain anything dynamic
        GPPrepReturn::new(vec![])
    }
}

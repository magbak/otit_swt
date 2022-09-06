use super::TimeSeriesQueryPrepper;

use oxrdf::Variable;
use spargebra::term::GroundTerm;
use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeSeriesQueryPrepper<'_> {
    pub fn prepare_values(
        &mut self,
        _variables: &Vec<Variable>,
        _bindings: &Vec<Vec<Option<GroundTerm>>>,
    ) -> GPPrepReturn {
        GPPrepReturn::new(vec![])
    }
}

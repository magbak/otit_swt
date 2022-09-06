use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use spargebra::term::GroundTerm;
use std::collections::HashMap;
use crate::preparing::graph_patterns::GPPrepReturn;

impl TimeSeriesQueryPrepper {
    pub fn prepare_values(
        &mut self,
        variables: &Vec<Variable>,
        bindings: &Vec<Vec<Option<GroundTerm>>>,
    ) -> GPPrepReturn {

    }
}

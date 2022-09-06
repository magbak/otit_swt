use super::TimeSeriesQueryPrepper;
use crate::change_types::ChangeType;
use crate::preparing::graph_patterns::GPPrepReturn;
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::TermPattern;
use std::collections::HashSet;

impl TimeSeriesQueryPrepper {
    //We assume that all paths have been prepared so as to not contain any datapoint, timestamp, or data value.
    //These should have been split into ordinary triples.
    pub fn prepare_path(
        &mut self,
        subject: &TermPattern,
        path: &PropertyPathExpression,
        object: &TermPattern,
    ) -> GPPrepReturn {
        let mut variables_in_scope = HashSet::new();
        if let TermPattern::Variable(s) = subject {
            variables_in_scope.insert(s.clone());
        }
        if let TermPattern::Variable(o) = object {
            variables_in_scope.insert(o.clone());
        }

        let gpr = GPPrepReturn::new();
        return gpr;
    }
}

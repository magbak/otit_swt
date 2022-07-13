mod to_python;

use std::collections::HashMap;
use pyo3::prelude::*;
use hybrid::orchestrator::execute_hybrid_query;
use hybrid::timeseries_database::TimeSeriesQueryable;

#[pyclass]
pub struct Engine {
    endpoint: String,
    tsdb: dyn TimeSeriesQueryable
}

#[pymethods]
impl Engine {
    pub fn new(endpoint:&str, time_series_parameters:HashMap<String, String>) -> Box<Engine> {

        Box::new(Engine {endpoint:endpoint.to_string(), tsdb: () })
    }

    pub fn execute_hybrid_query(&self, py: Python<'_>, sparql:&str) -> PyResult<PyObject> {
        let df = execute_hybrid_query(sparql, &self.endpoint, tsdb).await;
        to_python::to_py_array()
    }
}   
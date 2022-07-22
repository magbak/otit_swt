mod to_python;
mod errors;

use oxrdf::{IriParseError, NamedNode};
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use hybrid::orchestrator::execute_hybrid_query;
use hybrid::timeseries_database::arrow_flight_sql_database::ArrowFlightSQLDatabase as RustArrowFlightSQLDatabase;
use hybrid::timeseries_database::timeseries_sql_rewrite::TimeSeriesTable as RustTimeSeriesTable;
use crate::errors::PyQueryError;

#[pyclass]
pub struct Engine {
    endpoint: String,
    arrow_flight_sql_db: Option<RustArrowFlightSQLDatabase>,
}

#[pymethods]
impl Engine {
    #[new]
    pub fn new(endpoint:&str) -> Box<Engine> {
        Box::new(Engine {endpoint:endpoint.to_string(), arrow_flight_sql_db:None })
    }

    pub fn arrow_flight_sql(&mut self, db:&ArrowFlightSQLDatabase) -> PyResult<()>{
        let endpoint = format!("grpc+tcp:://{}:{}", &db.host, &db.port);
        let mut new_tables = vec![];
        for t in &db.tables {
            new_tables.push(t.to_rust_table().map_err(PyQueryError::from)?);
        }
        let afsqldb_result = Runtime::new().unwrap().block_on(
            RustArrowFlightSQLDatabase::new(&endpoint, new_tables)
        );
        let afsqldb = afsqldb_result.map_err(PyQueryError::from)?;
        self.arrow_flight_sql_db = Some(afsqldb);
        Ok(())
    }

    pub fn execute_hybrid_query(&mut self, py: Python<'_>, sparql:&str) -> PyResult<PyObject> {
        let df_result = Runtime::new().unwrap().block_on(
            execute_hybrid_query(sparql, &self.endpoint, Box::new(&mut self.arrow_flight_sql_db.unwrap()))
        );
        match df_result {
            Ok(mut df) => {
                let chunk = df.as_single_chunk().iter_chunks().next().unwrap();
                to_python::to_py_array(chunk.into_arrays().remove(0), py, &())
            }
            Err(err) => {
                Err(PyErr::from(PyQueryError::QueryExecutionError(err)))
            }
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ArrowFlightSQLDatabase {
    host: String,
    port: u16,
    tables: Vec<TimeSeriesTable>,
}

#[pymethods]
impl ArrowFlightSQLDatabase {
    #[new]
    pub fn new(host:String, port:u16, tables:Vec<TimeSeriesTable>) -> ArrowFlightSQLDatabase {
        ArrowFlightSQLDatabase {
            host,
            port,
            tables
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct TimeSeriesTable {
    #[pyo3(get, set)]
    pub schema: Option<String>,
    #[pyo3(get, set)]
    pub time_series_table: String,
    #[pyo3(get, set)]
    pub value_column: String,
    #[pyo3(get, set)]
    pub timestamp_column: String,
    #[pyo3(get, set)]
    pub identifier_column: String,
    #[pyo3(get, set)]
    pub value_datatype: String,
}

impl TimeSeriesTable {
    fn to_rust_table(&self) -> Result<RustTimeSeriesTable, IriParseError> {
        Ok(RustTimeSeriesTable {
            schema: self.schema.clone(),
            time_series_table: self.time_series_table.clone(),
            value_column: self.value_column.clone(),
            timestamp_column: self.timestamp_column.clone(),
            identifier_column: self.identifier_column.clone(),
            value_datatype: NamedNode::new(&self.value_datatype)?
        })
    }
}


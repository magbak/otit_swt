mod errors;
mod to_python;

use crate::errors::PyQueryError;
use hybrid::orchestrator::execute_hybrid_query;
use hybrid::timeseries_database::arrow_flight_sql_database::ArrowFlightSQLDatabase as RustArrowFlightSQLDatabase;
use hybrid::timeseries_database::timeseries_sql_rewrite::TimeSeriesTable as RustTimeSeriesTable;
use oxrdf::{IriParseError, NamedNode};
use pyo3::prelude::*;
use tokio::runtime::{Runtime, Builder};
use log::debug;

#[pyclass]
pub struct Engine {
    endpoint: String,
    arrow_flight_sql_db: Option<RustArrowFlightSQLDatabase>,
}

#[pymethods]
impl Engine {
    #[new]
    pub fn new(endpoint: &str) -> Box<Engine> {
        Box::new(Engine {
            endpoint: endpoint.to_string(),
            arrow_flight_sql_db: None,
        })
    }

    pub fn arrow_flight_sql(&mut self, db: &ArrowFlightSQLDatabase) -> PyResult<()> {
        let endpoint = format!("http://{}:{}", &db.host, &db.port);
        let mut new_tables = vec![];
        for t in &db.tables {
            new_tables.push(t.to_rust_table().map_err(PyQueryError::from)?);
        }

        let afsqldb_result = Runtime::new()
            .unwrap()
            .block_on(RustArrowFlightSQLDatabase::new(
                &endpoint,
                &db.username,
                &db.password,
                new_tables,
            ));
        let afsqldb = afsqldb_result.map_err(PyQueryError::from)?;
        self.arrow_flight_sql_db = Some(afsqldb);
        Ok(())
    }

    pub fn execute_hybrid_query(&mut self, py: Python<'_>, sparql: &str) -> PyResult<PyObject> {
        let res = env_logger::try_init();
        match res {
            Ok(_) => {}
            Err(_) => {
                debug!("Tried to initialize logger which is already initialize")
            }
        }
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let df_result = builder.build().unwrap().block_on(execute_hybrid_query(
            sparql,
            &self.endpoint,
            Box::new(self.arrow_flight_sql_db.as_mut().unwrap()),
        ));
        match df_result {
            Ok(mut df) => {
                let names_vec: Vec<String> = df
                    .get_column_names()
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect();
                let names: Vec<&str> = names_vec.iter().map(|x| x.as_str()).collect();
                let chunk = df.as_single_chunk().iter_chunks().next().unwrap();
                let pyarrow = PyModule::import(py, "pyarrow")?;
                to_python::to_py_rb(&chunk, names.as_slice(), py, pyarrow)
            }
            Err(err) => Err(PyErr::from(PyQueryError::QueryExecutionError(err))),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ArrowFlightSQLDatabase {
    host: String,
    port: u16,
    username: String,
    password: String,
    tables: Vec<TimeSeriesTable>,
}

#[pymethods]
impl ArrowFlightSQLDatabase {
    #[new]
    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        tables: Vec<TimeSeriesTable>,
    ) -> ArrowFlightSQLDatabase {
        ArrowFlightSQLDatabase {
            username,
            password,
            host,
            port,
            tables,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct TimeSeriesTable {
    pub schema: Option<String>,
    pub time_series_table: String,
    pub value_column: String,
    pub timestamp_column: String,
    pub identifier_column: String,
    pub value_datatype: String,
}

#[pymethods]
impl TimeSeriesTable {
    #[new]
    pub fn new(
        time_series_table: String,
        value_column: String,
        timestamp_column: String,
        identifier_column: String,
        value_datatype: String,
        schema: Option<String>,
    ) -> TimeSeriesTable {
        TimeSeriesTable {
            schema,
            time_series_table,
            value_column,
            timestamp_column,
            identifier_column,
            value_datatype,
        }
    }
}

impl TimeSeriesTable {
    fn to_rust_table(&self) -> Result<RustTimeSeriesTable, IriParseError> {
        Ok(RustTimeSeriesTable {
            schema: self.schema.clone(),
            time_series_table: self.time_series_table.clone(),
            value_column: self.value_column.clone(),
            timestamp_column: self.timestamp_column.clone(),
            identifier_column: self.identifier_column.clone(),
            value_datatype: NamedNode::new(&self.value_datatype)?,
        })
    }
}

#[pymodule]
fn otit_swt_query(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<TimeSeriesTable>()?;
    m.add_class::<ArrowFlightSQLDatabase>()?;
    Ok(())
}

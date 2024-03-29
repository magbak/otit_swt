pub mod errors;

use crate::errors::PyQueryError;
use arrow_python_utils::to_python::to_py_df;
use dsl::connective_mapping::ConnectiveMapping;
use dsl::costants::{REPLACE_STR_LITERAL, REPLACE_VARIABLE_NAME};
use dsl::parser::ts_query;
use dsl::translator::Translator;
use hybrid::timeseries_database::arrow_flight_sql_database::ArrowFlightSQLDatabase as RustArrowFlightSQLDatabase;
use hybrid::timeseries_database::opcua_history_read::OPCUAHistoryRead as RustOPCUAHistoryRead;
use hybrid::timeseries_database::timeseries_sql_rewrite::TimeSeriesTable as RustTimeSeriesTable;
use hybrid::engine::Engine as RustEngine;
use hybrid::pushdown_setting::{PushdownSetting, all_pushdowns};
use log::debug;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{IriParseError, Literal, NamedNode, Variable};
use pyo3::prelude::*;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::HashMap;
use tokio::runtime::{Builder, Runtime};

#[pyclass(unsendable)]
pub struct Engine {
    engine: Option<RustEngine>,
    endpoint: String,
    connective_mapping: Option<ConnectiveMapping>,
    name_predicate: Option<String>,
}

#[pymethods]
impl Engine {
    #[new]
    pub fn new(endpoint: &str) -> Box<Engine> {
        Box::new(Engine {
            engine: None,
            endpoint: endpoint.to_string(),
            connective_mapping: None,
            name_predicate: None,
        })
    }

    pub fn set_arrow_flight_sql(&mut self, db: &ArrowFlightSQLDatabase) -> PyResult<()> {
        if self.engine.is_some() {
            return Err(PyQueryError::TimeSeriesDatabaseAlreadyDefined.into());
        }
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
        let db = afsqldb_result.map_err(PyQueryError::from)?;
        self.engine = Some(RustEngine::new(
                    all_pushdowns(), Box::new(db)
                ));
        Ok(())
    }

    pub fn set_opcua_history_read(&mut self, db: &OPCUAHistoryRead) -> PyResult<()> {
        if self.engine.is_some() {
            return Err(PyQueryError::TimeSeriesDatabaseAlreadyDefined.into());
        }
        let actual_db = RustOPCUAHistoryRead::new(&db.endpoint, db.namespace);
        self.engine = Some(RustEngine::new(
                    [PushdownSetting::GroupBy].into(), Box::new(actual_db)
                ));
        Ok(())
    }

    pub fn execute_hybrid_query(&mut self, py: Python<'_>, sparql: &str) -> PyResult<PyObject> {
        if self.engine.is_none() {
            return Err(PyQueryError::MissingTimeSeriesDatabaseError.into());
        }
        let res = env_logger::try_init();
        match res {
            Ok(_) => {}
            Err(_) => {
                debug!("Tried to initialize logger which is already initialize")
            }
        }
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        let df_result = builder.build().unwrap().block_on(self.engine.as_mut().unwrap().execute_hybrid_query(
                sparql,
                &self.endpoint
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
                let polars = PyModule::import(py, "polars")?;
                to_py_df(&chunk, names.as_slice(), py, pyarrow, polars)
            }
            Err(err) => Err(PyErr::from(PyQueryError::QueryExecutionError(err))),
        }
    }

    pub fn name_predicate(&mut self, name_predicate: &str) -> PyResult<()> {
        self.name_predicate = Some(name_predicate.into());
        Ok(())
    }

    pub fn connective_mapping(&mut self, map: HashMap<String, String>) -> PyResult<()> {
        self.connective_mapping = Some(ConnectiveMapping { map });
        Ok(())
    }

    pub fn execute_dsl_query(&mut self, py: Python<'_>, query: &str) -> PyResult<PyObject> {
        let (_, parsed) = ts_query(query).map_err(|_| PyQueryError::DSLParsingError)?;
        let use_name_template = name_template(self.name_predicate.as_ref().unwrap());
        let use_type_name_template = type_name_template(self.name_predicate.as_ref().unwrap());
        let mut translator = Translator::new(
            use_name_template,
            use_type_name_template,
            self.connective_mapping.as_ref().unwrap().clone(),
        );
        let sparql = translator.translate(&parsed).to_string();
        println!("Q: {}", sparql);
        self.execute_hybrid_query(py, &sparql)
    }
}

fn type_name_template(predicate: &str) -> Vec<TriplePattern> {
    let type_variable = Variable::new_unchecked("type_var");
    let type_triple = TriplePattern {
        subject: TermPattern::Variable(Variable::new_unchecked(REPLACE_VARIABLE_NAME)),
        predicate: NamedNodePattern::NamedNode(NamedNode::from(rdf::TYPE)),
        object: TermPattern::Variable(type_variable.clone()),
    };
    let type_name_triple = TriplePattern {
        subject: TermPattern::Variable(type_variable),
        predicate: NamedNodePattern::NamedNode(NamedNode::new(predicate).unwrap()),
        object: TermPattern::Literal(Literal::new_typed_literal(REPLACE_STR_LITERAL, xsd::STRING)),
    };
    vec![type_triple, type_name_triple]
}

fn name_template(predicate: &str) -> Vec<TriplePattern> {
    let name_triple = TriplePattern {
        subject: TermPattern::Variable(Variable::new_unchecked(REPLACE_VARIABLE_NAME)),
        predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(predicate)),
        object: TermPattern::Literal(Literal::new_typed_literal(REPLACE_STR_LITERAL, xsd::STRING)),
    };
    vec![name_triple]
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
pub struct OPCUAHistoryRead {
    namespace: u16,
    endpoint: String,
}

#[pymethods]
impl OPCUAHistoryRead {
    #[new]
    pub fn new(endpoint: String, namespace: u16) -> OPCUAHistoryRead {
        OPCUAHistoryRead {
            namespace,
            endpoint,
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
    pub year_column: Option<String>,
    pub month_column: Option<String>,
    pub day_column: Option<String>,
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
        year_column: Option<String>,
        month_column: Option<String>,
        day_column: Option<String>,
    ) -> TimeSeriesTable {
        TimeSeriesTable {
            schema,
            time_series_table,
            value_column,
            timestamp_column,
            identifier_column,
            value_datatype,
            year_column,
            month_column,
            day_column
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
            year_column: self.year_column.clone(),
            month_column: self.month_column.clone(),
            day_column: self.day_column.clone()
        })
    }
}

#[pymodule]
fn otit_swt_query(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<TimeSeriesTable>()?;
    m.add_class::<ArrowFlightSQLDatabase>()?;
    m.add_class::<OPCUAHistoryRead>()?;
    Ok(())
}

use oxrdf::IriParseError;
use thiserror::Error;

use pyo3::{
    create_exception,
    exceptions::{PyException},
    prelude::*,
};
use hybrid::timeseries_database::arrow_flight_sql_database::ArrowFlightSQLError as RustArrowFlightSQLError;

#[derive(Error, Debug)]
pub enum PyQueryError {
    #[error(transparent)]
    ArrowFlightSQLError(#[from] RustArrowFlightSQLError),
    #[error(transparent)]
    DatatypeIRIParseError(#[from] IriParseError),
    #[error(transparent)]
    QueryExecutionError(Box<dyn std::error::Error>),
}

impl std::convert::From<PyQueryError> for PyErr {
    fn from(pqe: PyQueryError) -> Self {
        match pqe {
            PyQueryError::ArrowFlightSQLError(err) => {ArrowFlightSQLError::new_err(format!("{}", err))}
            PyQueryError::DatatypeIRIParseError(err) => {DatatypeIRIParseError::new_err(format!("{}", err))}
            PyQueryError::QueryExecutionError(err) => {QueryExecutionError::new_err(format!("{}", err))}
        }
    }
}

create_exception!(exceptions, ArrowFlightSQLError, PyException);
create_exception!(exceptions, DatatypeIRIParseError, PyException);
create_exception!(exceptions, QueryExecutionError, PyException);



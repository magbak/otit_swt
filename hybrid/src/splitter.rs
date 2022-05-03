use std::fmt;
use std::fmt::{Debug, Display, Formatter, Pointer};
use spargebra::{ParseError, Query};
use spargebra::algebra::GraphPattern;
use crate::splitter::SelectQueryErrorKind::Unsupported;

pub enum SelectQueryErrorKind {
    Parse(ParseError),
    NotSelectQuery,
    Unsupported(String)
}

struct SelectQueryError {
    kind: SelectQueryErrorKind
}

impl Display for SelectQueryError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match &self.kind {
            SelectQueryErrorKind::Parse(pe) => {pe.fmt(f)}
            SelectQueryErrorKind::NotSelectQuery => {
                write!(f, "Not a select query")
            }
            SelectQueryErrorKind::Unsupported(s) => {
                write!(f, "Unsupported construct: {}", s)
            }
        }
    }
}

impl Debug for SelectQueryError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match &self.kind {
            SelectQueryErrorKind::Parse(pe) => {pe.fmt(f)}
            SelectQueryErrorKind::NotSelectQuery => {
                write!(f, "Not a select query")
            }
            SelectQueryErrorKind::Unsupported(s) => {
                write!(f, "Unsupported construct: {}", s)
            }
        }
    }
}

pub fn parse_sparql_select_query(query_str:&str) -> Result<GraphPattern, SelectQueryError> {
    let q = Query::parse(query_str, None)?;
    match q {
        Query::Select { dataset, pattern, base_iri } => {
            let mut unsupported_constructs = vec![];
            if dataset.is_some() {
                unsupported_constructs.push("Dataset")
            }
            if base_iri.is_some() {
                unsupported_constructs.push("BaseIri")
            }
            if unsupported_constructs.len() > 0 {
                Err(SelectQueryError { kind: SelectQueryErrorKind::Unsupported(unsupported_constructs.join(",")) })
            } else {
                Ok(pattern)
            }
        }
        _ => {Err(SelectQueryError{kind: SelectQueryErrorKind::NotSelectQuery}) }
    }
}
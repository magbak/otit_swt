use crate::ast::{ConstantTerm, PType};
use polars_core::prelude::{DataType, Series};
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum MappingErrorType {
    TemplateNotFound(String),
    MissingKeyColumn,
    KeyColumnContainsDuplicates(Series),
    KeyColumnOverlapsExisting(Series),
    NonOptionalColumnHasNull(String, Series),
    InvalidKeyColumnDataType(DataType),
    NonBlankColumnHasBlankNode(String, Series),
    MissingParameterColumn(String),
    ContainsIrrelevantColumns(Vec<String>),
    CouldNotInferStottrDatatypeForColumn(String, DataType),
    ColumnDataTypeMismatch(String, DataType, PType),
    PTypeNotSupported(String, PType),
    UnknownTimeZoneError(String),
    UnknownVariableError(String),
    ConstantDoesNotMatchDataType(ConstantTerm, PType, PType),
    ConstantListHasInconsistentPType(ConstantTerm, PType, PType),
}

#[derive(Debug)]
pub struct MappingError {
    pub kind: MappingErrorType,
}

impl Display for MappingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            MappingErrorType::TemplateNotFound(t) => {
                write!(f, "Could not find template: {}", t)
            }
            MappingErrorType::MissingKeyColumn => {
                write!(f, "Could not find Key column")
            }
            MappingErrorType::KeyColumnContainsDuplicates(dupes) => {
                write!(f, "Key column has duplicate entries: {}", dupes)
            }
            MappingErrorType::KeyColumnOverlapsExisting(overlapping) => {
                write!(f, "Key column overlaps existing keys: {}", overlapping)
            }
            MappingErrorType::NonOptionalColumnHasNull(col, nullkey) => {
                write!(
                    f,
                    "Column {} which is non-optional has null values for keys: {}",
                    col, nullkey
                )
            }
            MappingErrorType::InvalidKeyColumnDataType(dt) => {
                write!(
                    f,
                    "Key column has invalid data type: {}, should be Utf8",
                    dt
                )
            }
            MappingErrorType::NonBlankColumnHasBlankNode(col, blanks) => {
                write!(f, "Non-blank column {} has blanks {}", col, blanks)
            }
            MappingErrorType::MissingParameterColumn(c) => {
                write!(f, "Expected column {} is missing", c)
            }
            MappingErrorType::ContainsIrrelevantColumns(irr) => {
                write!(f, "Unexpected columns: {}", irr.join(","))
            }
            MappingErrorType::CouldNotInferStottrDatatypeForColumn(col, dt) => {
                write!(
                    f,
                    "Could not infer stottr type for column {} with polars datatype {}",
                    col, dt
                )
            }
            MappingErrorType::ColumnDataTypeMismatch(col, dt, ptype) => {
                write!(
                    f,
                    "Column {} had datatype {} which was incompatible with the stottr datatype {}",
                    col, dt, ptype
                )
            }
            MappingErrorType::PTypeNotSupported(name, ptype) => {
                write!(
                    f,
                    "Found value {} with unsupported stottr datatype {}",
                    name, ptype
                )
            }
            MappingErrorType::UnknownTimeZoneError(tz) => {
                write!(f, "Unknown time zone {}", tz)
            }
            MappingErrorType::UnknownVariableError(v) => {
                write!(
                    f,
                    "Could not find variable {}, is the stottr template invalid?",
                    v
                )
            }
            MappingErrorType::ConstantDoesNotMatchDataType(constant_term, expected, actual) => {
                write!(
                    f,
                    "Expected constant term {:?} to have data type {} but was {}",
                    constant_term, expected, actual
                )
            }
            MappingErrorType::ConstantListHasInconsistentPType(constant_term, prev, next) => {
                write!(
                    f,
                    "Constant term {:?} has inconsistent data types {} and {}",
                    constant_term, prev, next
                )
            }
        }
    }
}

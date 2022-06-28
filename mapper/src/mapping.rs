use std::collections::{HashMap, HashSet};
use nom::Parser;
use oxrdf::{NamedNode, Variable};
use polars::prelude::{BooleanChunked, col, DataFrame, DataType, GetOutput, IntoLazy, lit, Series};
use polars::toggle_string_cache;
use crate::ast::{PType, Signature};
use crate::templates::Templates;
use polars::export::rayon::iter::ParallelIterator;

pub struct Mapping {
    templates: Templates,
    triples: DataFrame
}

pub enum MappingErrorType {
    TemplateNotFound,
    MissingKeyColumn,
    KeyColumnDatatypeMismatch(DataType, DataType),
    KeyColumnContainsDuplicates(Series),
    KeyColumnOverlapsExisting(Series),
    NonOptionalColumnHasNull(String, Series),
    InvalidKeyColumnDataType(DataType),
    NonBlankColumnHasBlankNode(String, Series)
}

pub struct MappingError {
    kind: MappingErrorType
}

pub struct MappingReport {

}

pub enum ColumnKind {
    LiteralColumn,
    PathColumn
}

pub struct MappedColumn {
    kind: ColumnKind,
    variable: Variable,
    column_name: String,
}

impl Mapping {
    pub fn new(templates:&Templates) {
        toggle_string_cache(true);
    }

    pub fn instantiate(&mut self, name:&NamedNode, df:&DataFrame) -> Result<MappingReport, MappingError> {
        self.validate_dataframe(df)?;
        if let Some(template) = self.templates.get(name) {
            let column_mapping = find_valid_column_mapping(&template.signature, df)?;

        } else {
            return Err(MappingError{kind:MappingErrorType::TemplateNotFound})
        }
        Ok(MappingReport{})

    }


    fn validate_dataframe(&self, df:&DataFrame) -> Result<(), MappingError>{
        if !df.get_column_names().contains(&"Key") {
            return Err(MappingError{kind:MappingErrorType::MissingKeyColumn});
        }
        let existing_key_datatype= self.triples.column("Key").unwrap().dtype();
        if !(existing_key_datatype == &DataType::Utf8 || existing_key_datatype == &DataType::Categorical(None) || existing_key_datatype == &DataType::UInt32 || existing_key_datatype == &DataType::UInt64) {
            return Err(MappingError{kind:MappingErrorType::InvalidKeyColumnDataType(existing_key_datatype.clone())})
        }

        if self.triples.get_column_names().contains(&"Key") {
            let new_key_datatype = df.column("Key").unwrap().dtype();
            if !(new_key_datatype == &DataType::Utf8 && existing_key_datatype == &DataType::Categorical(None)) && new_key_datatype != existing_key_datatype {
                return Err(MappingError{kind:MappingErrorType::KeyColumnDatatypeMismatch(new_key_datatype.clone(), existing_key_datatype.clone())})
            }
            if df.column("Key").unwrap().is_duplicated().unwrap().any() {
                let is_duplicated = df.column("Key").unwrap().is_duplicated().unwrap();
                let dupes = df.filter(&is_duplicated).unwrap().clone();
                return Err(MappingError{kind:MappingErrorType::KeyColumnContainsDuplicates(dupes.column("Key").unwrap().clone())});
            }
            let existing_keys = self.triples.column("Key").unwrap();
            let overlapping_keys = df.column("Key").unwrap().is_in(existing_keys).unwrap();
            if overlapping_keys.any() {
                return Err(MappingError{kind:MappingErrorType::KeyColumnOverlapsExisting(df.column("Key").unwrap().filter(&overlapping_keys).unwrap().clone())});
            }
        }
        Ok(())
}
}


fn find_valid_column_mapping(signature:&Signature, df:&DataFrame) -> Result<HashMap<String, MappedColumn>, MappingError> {
    let mut df_columns = HashSet::new();
    df_columns.extend(df.get_column_names().into_iter());
    let removed = df_columns.remove("Key");
    assert!(removed);

    for parameter in &signature.parameter_list {
        let variable_name = &parameter.stottr_variable.name;
        if df_columns.contains(variable_name.as_str()) {
            if !parameter.optional {
                validate_non_optional_parameter(df, variable_name)?;
            }
            if parameter.non_blank { //TODO handle blanks;
                validate_non_blank_parameter(df, variable_name)?;
            }
            if let Some(t) = &parameter.ptype {
                validate_data_type(df, t, variable_name)?;
            }
        }
    }

    let map = HashMap::new();
    Ok(map)
}

fn validate_data_type(df: &DataFrame, ptype: &PType, column_name: &str) -> Result<(),MappingError> {
    let mut current_ptype = ptype;
    let mut current_column_data_type = df.column(column_name).unwrap().dtype();
    let mut validated = false;
    while !validated {
        let current_ptype = match current_ptype {
            PType::BasicType(tn) => {

            }
            PType::LUBType(_) => {}
            PType::ListType(_) => {}
            PType::NEListType(_) => {}
        };
    }
    Ok(())
}

fn validate_non_optional_parameter(df: &DataFrame, column_name:&str) -> Result<(), MappingError> {
    if df.column(column_name).unwrap().is_null().any() {
        let is_null = df.column(column_name).unwrap().is_null();
        Err(MappingError{kind:MappingErrorType::NonOptionalColumnHasNull(column_name.to_string(), df.column("Key").unwrap().filter(&is_null).unwrap())})
    } else {
        Ok(())
    }
}

fn validate_non_blank_parameter(df:&DataFrame, column_name:&str) -> Result<(), MappingError> {
    let is_blank_node_mask: BooleanChunked = df.column(column_name).unwrap().utf8().map(move |x|x.par_iter().map(move |x|x.unwrap_or("").starts_with("_:")).collect()).unwrap();
    if is_blank_node_mask.any() {
        return Err(MappingError{kind:MappingErrorType::NonBlankColumnHasBlankNode(column_name.to_string(), df.column(column_name).unwrap().filter(&is_blank_node_mask).unwrap())})
    }
    Ok(())
}
use crate::ast::StottrDocument;
use crate::errors::MapperError;
use crate::parsing::whole_stottr_doc;
use crate::resolver::resolve_document;
use std::fs::read_to_string;
use std::path::Path;

pub fn document_from_str(s: &str) -> Result<StottrDocument, MapperError> {
    let unresolved = whole_stottr_doc(s).map_err(MapperError::from)?;
    resolve_document(unresolved).map_err(MapperError::from)
}

pub fn document_from_file<P: AsRef<Path>>(p: P) -> Result<StottrDocument, MapperError> {
    let s = read_to_string(p)?;
    document_from_str(&s)
}

use crate::ast::StottrDocument;
use crate::parser::whole_stottr_doc;
use crate::resolver::resolve_document;
use std::error::Error;
use std::fs::read_to_string;
use std::path::Path;

pub fn document_from_str(s: &str) -> Result<StottrDocument, Box<dyn Error>> {
    let unresolved = whole_stottr_doc(s)?;
    let resolved = resolve_document(unresolved)?;
    Ok(resolved)
}

pub fn document_from_file<P: AsRef<Path>>(p: P) -> Result<StottrDocument, Box<dyn Error>> {
    let s = read_to_string(p)?;
    document_from_str(&s)
}

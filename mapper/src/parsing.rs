use std::error::Error;
use nom::Finish;
use crate::parsing::errors::{ParsingError, ParsingErrorKind};
use crate::parsing::nom_parsing::stottr_doc;
use crate::parsing::parsing_ast::UnresolvedStottrDocument;

mod nom_parsing;
mod errors;
pub mod parsing_ast;

pub fn whole_stottr_doc(s: &str) -> Result<UnresolvedStottrDocument, Box<dyn Error>> {
    let result = stottr_doc(s).finish();
    match result {
        Ok((rest, doc)) => {
            if rest != "" {
                Err(Box::new(ParsingError {
                    kind: ParsingErrorKind::CouldNotParseEverything(rest.to_string()),
                }))
            } else {
                Ok(doc)
            }
        }
        Err(e) => Err(Box::new(ParsingError {
            kind: ParsingErrorKind::NomParserError(format!("{:?}", e.code)),
        })),
    }
}
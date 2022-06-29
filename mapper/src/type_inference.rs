use crate::ast::{Statement, StottrDocument};

fn infer_document_types(stottr_document:&mut StottrDocument) {
    for statement in &mut stottr_document.statements {
        //infer_statement_types(statement);
    }
}
//
// fn infer_statement_types(statement: &mut Statement) {
//     if let Some(instance)
//
//     match statement {
//         Statement::Signature(signature) => {
//         }
//         Statement::Template(_) => {}
//         Statement::BaseTemplate(_) => {}
//         Statement::Instance(_) => {}
//     }
// }
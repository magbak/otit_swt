use crate::parser::parse_sparql_query;

mod parser;
mod splitter;

pub fn process_query(sparql_query:String) {
    parse_sparql_query(sparql_query)
}
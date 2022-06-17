use std::collections::HashMap;
use crate::ast::ConnectiveType;
use serde::{Serialize, Deserialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectiveMapping {
    #[serde(flatten)]
    pub(crate) map:HashMap<String, String>
}
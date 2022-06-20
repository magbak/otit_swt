use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectiveMapping {
    #[serde(flatten)]
    pub(crate) map:HashMap<String, String>
}
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectiveMapping {
    #[serde(flatten)]
    pub map: HashMap<String, String>,
}

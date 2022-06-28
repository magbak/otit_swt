use std::collections::HashMap;
use oxrdf::NamedNode;
use crate::ast::Template;

pub struct Templates {
    pub map: HashMap<NamedNode, Template>
}

impl Templates {
    pub fn get(&self, named_node:&NamedNode) -> Option<&Template> {
        self.map.get(named_node)
    }
}
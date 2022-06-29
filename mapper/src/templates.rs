use oxrdf::NamedNode;
use crate::ast::Template;

pub struct TemplateLibrary {
    pub templates: Template
}

impl TemplateLibrary {
    pub fn get(&self, named_node:&NamedNode) -> Option<&Template> {
        //self.map.get(named_node)
        None
    }
}

pub struct TemplateDataset {

}
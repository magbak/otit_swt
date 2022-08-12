use oxrdf::{BlankNode, NamedNode};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[derive(PartialEq, Debug, Clone)]
pub struct Prefix {
    pub name: String,
    pub iri: NamedNode,
}

#[derive(PartialEq, Debug, Clone)]
pub enum Directive {
    Prefix(Prefix),
    Base(NamedNode),
}

#[derive(PartialEq, Debug, Clone)]
pub enum Statement {
    Template(Template),
    Instance(Instance),
}

#[derive(PartialEq, Debug, Clone)]
pub struct Template {
    pub signature: Signature,
    pub pattern_list: Vec<Instance>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Signature {
    pub template_name: NamedNode,
    pub parameter_list: Vec<Parameter>,
    pub annotation_list: Option<Vec<Annotation>>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Parameter {
    pub optional: bool,
    pub non_blank: bool,
    pub ptype: Option<PType>,
    pub stottr_variable: StottrVariable,
    pub default_value: Option<DefaultValue>,
}

#[derive(PartialEq, Debug, Clone)]
pub enum PType {
    BasicType(NamedNode),
    LUBType(Box<PType>),
    ListType(Box<PType>),
    NEListType(Box<PType>),
}

impl Display for PType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PType::BasicType(nn) => {
                write!(f, "BasicType({})", nn)
            }
            PType::LUBType(lt) => {
                let s = lt.to_string();
                write!(f, "LUBType({})", s)
            }
            PType::ListType(lt) => {
                let s = lt.to_string();
                write!(f, "ListType({})", s)
            }
            PType::NEListType(lt) => {
                let s = lt.to_string();
                write!(f, "NEListType({})", s)
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct StottrVariable {
    pub name: String,
}

#[derive(PartialEq, Debug, Clone)]
pub struct DefaultValue {
    pub constant_term: ConstantTerm,
}

#[derive(PartialEq, Debug, Clone)]
pub enum ConstantTerm {
    Constant(ConstantLiteral),
    ConstantList(Vec<ConstantTerm>),
}

#[derive(PartialEq, Debug, Clone)]
pub enum ConstantLiteral {
    IRI(NamedNode),
    BlankNode(BlankNode),
    Literal(StottrLiteral),
    None,
}

#[derive(PartialEq, Debug, Clone)]
pub struct StottrLiteral {
    pub value: String,
    pub language: Option<String>,
    pub data_type_iri: Option<NamedNode>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Instance {
    pub list_expander: Option<ListExpanderType>,
    pub template_name: NamedNode,
    pub argument_list: Vec<Argument>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Annotation {
    pub instance: Instance,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Argument {
    pub list_expand: bool,
    pub term: StottrTerm,
}

#[derive(PartialEq, Debug, Clone)]
pub enum ListExpanderType {
    Cross,
    ZipMin,
    ZipMax,
}

impl ListExpanderType {
    pub fn from(l: &str) -> ListExpanderType {
        match l {
            "cross" => ListExpanderType::Cross,
            "zipMin" => ListExpanderType::ZipMin,
            "zipMax" => ListExpanderType::ZipMax,
            _ => panic!("Did not recognize list expander type"),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum StottrTerm {
    Variable(StottrVariable),
    ConstantTerm(ConstantTerm),
    List(Vec<StottrTerm>),
}

#[derive(PartialEq, Debug, Clone)]
pub struct StottrDocument {
    pub directives: Vec<Directive>,
    pub statements: Vec<Statement>,
    pub prefix_map: HashMap<String, NamedNode>,
}

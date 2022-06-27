use oxrdf::{BlankNode, NamedNode};

#[derive(PartialEq, Debug)]
pub struct Prefix {
    pub name: String,
    pub iri: NamedNode,
}

#[derive(PartialEq, Debug)]
pub enum Directive {
    Prefix(Prefix),
    Base(NamedNode),
    SparqlBase(NamedNode),
    SparqlPrefix(Prefix),
}
#[derive(PartialEq, Debug)]
pub enum Statement {
    Signature(Signature),
    Template(Template),
    BaseTemplate(BaseTemplate),
    Instance(Instance),
}

#[derive(PartialEq, Debug)]
pub struct Template {
    pub signature: Signature,
    pub pattern_list: Vec<Instance>,
}

#[derive(PartialEq, Debug)]
pub struct BaseTemplate {
    pub signature: Signature,
}

#[derive(PartialEq, Debug)]
pub struct Signature {
    pub template_name: ResolvesToNamedNode,
    pub parameter_list: Vec<Parameter>,
    pub annotation_list: Option<Vec<Annotation>>,
}

#[derive(PartialEq, Debug)]
pub struct Parameter {
    pub optional: bool,
    pub non_blank: bool,
    pub ptype: Option<PType>,
    pub stottr_variable: StottrVariable,
    pub default_value: Option<DefaultValue>,
}

#[derive(PartialEq, Debug)]
pub enum PType {
    BasicType(PrefixedName),
    LUBType(Box<PType>),
    ListType(Box<PType>),
    NEListType(Box<PType>),
}

#[derive(PartialEq, Debug)]
pub struct StottrVariable {
    pub name: String,
}

#[derive(PartialEq, Debug)]
pub struct DefaultValue {
    pub constant_term: ConstantTerm,
}

#[derive(PartialEq, Debug)]
pub enum ConstantTerm {
    Constant(ConstantLiteral),
    ConstantList(Vec<ConstantTerm>),
}

#[derive(PartialEq, Debug)]
pub enum ConstantLiteral {
    IRI(ResolvesToNamedNode),
    BlankNode(BlankNode),
    Literal(StottrLiteral),
    None,
}

#[derive(PartialEq, Debug)]
pub struct StottrLiteral {
    pub value: String,
    pub language: Option<String>,
    pub data_type_iri: Option<ResolvesToNamedNode>,
}

#[derive(PartialEq, Debug)]
pub struct PrefixedName {
    pub prefix: String,
    pub name: String,
}

#[derive(PartialEq, Debug)]
pub enum ResolvesToNamedNode {
    PrefixedName(PrefixedName),
    NamedNode(NamedNode),
}

#[derive(PartialEq, Debug)]
pub struct Instance {
    pub list_expander: Option<ListExpanderType>,
    pub template_name: ResolvesToNamedNode,
    pub argument_list: Vec<Argument>,
}

#[derive(PartialEq, Debug)]
pub struct Annotation {
    pub instance: Instance,
}

#[derive(PartialEq, Debug)]
pub struct Argument {
    pub list_expand: bool,
    pub term: StottrTerm,
}

#[derive(PartialEq, Debug)]
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

#[derive(PartialEq, Debug)]
pub enum StottrTerm {
    Variable(StottrVariable),
    ConstantTerm(ConstantTerm),
    List(Vec<StottrTerm>),
}

#[derive(PartialEq, Debug)]
pub struct StottrDocument {
    pub directives: Vec<Directive>,
    pub statements: Vec<Statement>,
}

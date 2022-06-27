use oxrdf::{BlankNode, NamedNode};

pub struct Prefix {
    pub name: String,
    pub iri: NamedNode,
}

pub enum Directive {
    Prefix(Prefix),
    Base(NamedNode),
    SparqlBase(NamedNode),
    SparqlPrefix(Prefix),
}

pub enum Statement {
    Signature(Signature),
    Template(Template),
    BaseTemplate(BaseTemplate),
    Instance(Instance)
}

pub struct Template {
    pub signature:Signature,
    pub pattern_list: Vec<Instance>
}

pub struct BaseTemplate {
    pub signature:Signature,
}

pub struct Signature {
    pub template_name: ResolvesToNamedNode,
    pub parameter_list: Vec<Parameter>,
    pub annotation_list: Option<Vec<Annotation>>
}

pub struct Parameter {
    pub optional: bool,
    pub non_blank: bool,
    pub ptype: Option<PType>,
    pub stottr_variable: StottrVariable,
    pub default_value: Option<DefaultValue>
}

pub enum PType {
    BasicType(PrefixedName),
    LUBType(Box<PType>),
    ListType(Box<PType>),
    NEListType(Box<PType>),
}

pub struct StottrVariable {
    pub name: String
}

pub struct DefaultValue {
    pub constant_term: ConstantTerm,
}

pub enum ConstantTerm {
    Constant(ConstantLiteral),
    ConstantList(Vec<ConstantTerm>)
}

pub enum ConstantLiteral {
    IRI(ResolvesToNamedNode),
    BlankNode(BlankNode),
    Literal(StottrLiteral),
    None,
}

pub struct StottrLiteral {
    pub value:String,
    pub language: Option<String>,
    pub data_type_iri: Option<ResolvesToNamedNode>
}

pub struct PrefixedName {
    pub prefix: String,
    pub name: String,
}

pub enum ResolvesToNamedNode {
    PrefixedName(PrefixedName),
    NamedNode(NamedNode)
}

pub struct Instance {
    pub list_expander: Option<ListExpanderType>,
    pub template_name: ResolvesToNamedNode,
    pub argument_list: Vec<Argument>
}

pub struct Annotation {
    pub instance: Instance
}

pub struct Argument {
    pub list_expand: bool,
    pub term: StottrTerm
}

pub enum ListExpanderType {
    Cross,
    ZipMin,
    ZipMax
}

impl ListExpanderType {
    pub fn from(l:&str) -> ListExpanderType {
        match l {
            "cross" => {ListExpanderType::Cross},
            "zipMin" => {ListExpanderType::ZipMin},
            "zipMax" => {ListExpanderType::ZipMax},
            _ => panic!("Did not recognize list expander type")
        }
    }
}

pub enum StottrTerm {
    Variable(StottrVariable),
    ConstantTerm(ConstantTerm),
    List(Vec<StottrTerm>)
}

pub struct StottrDocument {
    pub directives: Vec<Directive>,
    pub statements: Vec<Statement>
}
use oxrdf::NamedNode;

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
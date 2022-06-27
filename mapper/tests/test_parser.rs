use mapper::ast::{
    Argument, ConstantLiteral, ConstantTerm, Directive, Instance, PType, Parameter, Prefix,
    PrefixedName, ResolvesToNamedNode, Signature, Statement, StottrDocument, StottrTerm,
    StottrVariable, Template,
};
use mapper::parser::stottr_doc;
use nom::Finish;
use oxrdf::{BlankNode, NamedNode};

#[test]
fn test_easy_template() {
    //This test case is taken from:
    // https://github.com/Callidon/pyOTTR/blob/master/tests/stottr_test.py
    let stottr = r#"
        @prefix ex: <http://example.org#>.
        ex:Person[ ?firstName, ?lastName, ?email ] :: {
            o-rdf:Type (_:person, foaf:Person )
        } .
    "#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.org#"),
        })],
        statements: vec![Statement::Template(Template {
            signature: Signature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "Person".to_string(),
                }),
                parameter_list: vec![
                    Parameter {
                        optional: false,
                        non_blank: false,
                        ptype: None,
                        stottr_variable: StottrVariable {
                            name: "firstName".to_string(),
                        },
                        default_value: None,
                    },
                    Parameter {
                        optional: false,
                        non_blank: false,
                        ptype: None,
                        stottr_variable: StottrVariable {
                            name: "lastName".to_string(),
                        },
                        default_value: None,
                    },
                    Parameter {
                        optional: false,
                        non_blank: false,
                        ptype: None,
                        stottr_variable: StottrVariable {
                            name: "email".to_string(),
                        },
                        default_value: None,
                    },
                ],
                annotation_list: None,
            },
            pattern_list: vec![Instance {
                list_expander: None,
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "o-rdf".to_string(),
                    name: "Type".to_string(),
                }),
                argument_list: vec![
                    Argument {
                        list_expand: false,
                        term: StottrTerm::ConstantTerm(ConstantTerm::Constant(
                            ConstantLiteral::BlankNode(BlankNode::new_unchecked("person")),
                        )),
                    },
                    Argument {
                        list_expand: false,
                        term: StottrTerm::ConstantTerm(ConstantTerm::Constant(
                            ConstantLiteral::IRI(ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "foaf".to_string(),
                                name: "Person".to_string(),
                            })),
                        )),
                    },
                ],
            }],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_modifiers_1() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ ??pizza  ] .
    "#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![Statement::Signature(Signature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![Parameter {
                optional: true,
                non_blank: false,
                ptype: None,
                stottr_variable: StottrVariable {
                    name: "pizza".to_string(),
                },
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_modifiers_2() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ !?pizza  ] .
    "#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![Statement::Signature(Signature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![Parameter {
                optional: false,
                non_blank: true,
                ptype: None,
                stottr_variable: StottrVariable {
                    name: "pizza".to_string(),
                },
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_modifiers_3() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ ?!?pizza  ] .
    "#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![Statement::Signature(Signature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![Parameter {
                optional: true,
                non_blank: true,
                ptype: None,
                stottr_variable: StottrVariable {
                    name: "pizza".to_string(),
                },
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_modifiers_4() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ !??pizza  ] .
    "#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![Statement::Signature(Signature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![Parameter {
                optional: true,
                non_blank: true,
                ptype: None,
                stottr_variable: StottrVariable {
                    name: "pizza".to_string(),
                },
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_type_1() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ owl:Class ?pizza ].
    "#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![Directive::Prefix(Prefix {
            name: "ex".to_string(),
            iri: NamedNode::new_unchecked("http://example.net/ns#"),
        })],
        statements: vec![Statement::Signature(Signature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![Parameter {
                optional: false,
                non_blank: false,
                ptype: Some(PType::BasicType(PrefixedName {
                    prefix: "owl".to_string(),
                    name: "Class".to_string(),
                })),
                stottr_variable: StottrVariable {
                    name: "pizza".to_string(),
                },
                default_value: None,
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

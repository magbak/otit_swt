use mapper::ast::{
    Annotation, Argument, ConstantLiteral, ConstantTerm, DefaultValue, Directive, Instance,
    ListExpanderType, PType, Parameter, Prefix, PrefixedName, ResolvesToNamedNode, Signature,
    Statement, StottrDocument, StottrLiteral, StottrTerm, StottrVariable, Template,
};
use mapper::parser::stottr_doc;
use nom::Finish;
use oxrdf::vocab::xsd;
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

#[test]
fn test_spec_type_2() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ ? owl:Class ?pizza ].
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

#[test]
fn test_spec_type_3() {
    let stottr = r#"
        @prefix ex:<http://example.net/ns#>.

    ex:NamedPizza [ ?! owl:Class ?pizza ].
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

#[test]
fn test_spec_default_value_1() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza[ owl:Class ?pizza = p:pizza] ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
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
                default_value: Some(DefaultValue {
                    constant_term: ConstantTerm::Constant(ConstantLiteral::IRI(
                        ResolvesToNamedNode::PrefixedName(PrefixedName {
                            prefix: "p".to_string(),
                            name: "pizza".to_string(),
                        }),
                    )),
                }),
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_default_value_2() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza[ owl:Class ?pizza = 2] ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
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
                default_value: Some(DefaultValue {
                    constant_term: ConstantTerm::Constant(ConstantLiteral::Literal(
                        StottrLiteral {
                            value: "2".to_string(),
                            language: None,
                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                xsd::INTEGER.into_owned(),
                            )),
                        },
                    )),
                }),
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_default_value_3() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza[ owl:Class ?pizza = "asdf"] ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
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
                default_value: Some(DefaultValue {
                    constant_term: ConstantTerm::Constant(ConstantLiteral::Literal(
                        StottrLiteral {
                            value: "asdf".to_string(),
                            language: None,
                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                xsd::STRING.into_owned(),
                            )),
                        },
                    )),
                }),
            }],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_more_parameters() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza [  ?pizza ,  ?country  ,  ?toppings ] ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![Statement::Signature(Signature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![
                Parameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    stottr_variable: StottrVariable {
                        name: "pizza".to_string(),
                    },
                    default_value: None,
                },
                Parameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    stottr_variable: StottrVariable {
                        name: "country".to_string(),
                    },
                    default_value: None,
                },
                Parameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    stottr_variable: StottrVariable {
                        name: "toppings".to_string(),
                    },
                    default_value: None,
                },
            ],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_lists() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza [  ?pizza = "asdf" ,  ?country = ("asdf", "asdf") ,  ?toppings = ((())) ] ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![Statement::Signature(Signature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![
                Parameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    stottr_variable: StottrVariable {
                        name: "pizza".to_string(),
                    },
                    default_value: Some(DefaultValue {
                        constant_term: ConstantTerm::Constant(ConstantLiteral::Literal(
                            StottrLiteral {
                                value: "asdf".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                    NamedNode::new_unchecked(
                                        "http://www.w3.org/2001/XMLSchema#string",
                                    ),
                                )),
                            },
                        )),
                    }),
                },
                Parameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    stottr_variable: StottrVariable {
                        name: "country".to_string(),
                    },
                    default_value: Some(DefaultValue {
                        constant_term: ConstantTerm::ConstantList(vec![
                            ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                value: "asdf".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                    NamedNode::new_unchecked(
                                        "http://www.w3.org/2001/XMLSchema#string",
                                    ),
                                )),
                            })),
                            ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                value: "asdf".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(
                                    NamedNode::new_unchecked(
                                        "http://www.w3.org/2001/XMLSchema#string",
                                    ),
                                )),
                            })),
                        ]),
                    }),
                },
                Parameter {
                    optional: false,
                    non_blank: false,
                    ptype: None,
                    stottr_variable: StottrVariable {
                        name: "toppings".to_string(),
                    },
                    default_value: Some(DefaultValue {
                        constant_term: ConstantTerm::ConstantList(vec![
                            ConstantTerm::ConstantList(vec![ConstantTerm::ConstantList(vec![])]),
                        ]),
                    }),
                },
            ],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_more_complex_types() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza [
      ! owl:Class ?pizza  ,
      ?! owl:NamedIndividual ?country  = ex:Class ,
      NEList<List<List<owl:Class>>> ?toppings
    ] ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![Statement::Signature(Signature {
            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                prefix: "ex".to_string(),
                name: "NamedPizza".to_string(),
            }),
            parameter_list: vec![
                Parameter {
                    optional: false,
                    non_blank: true,
                    ptype: Some(PType::BasicType(PrefixedName {
                        prefix: "owl".to_string(),
                        name: "Class".to_string(),
                    })),
                    stottr_variable: StottrVariable {
                        name: "pizza".to_string(),
                    },
                    default_value: None,
                },
                Parameter {
                    optional: true,
                    non_blank: true,
                    ptype: Some(PType::BasicType(PrefixedName {
                        prefix: "owl".to_string(),
                        name: "NamedIndividual".to_string(),
                    })),
                    stottr_variable: StottrVariable {
                        name: "country".to_string(),
                    },
                    default_value: Some(DefaultValue {
                        constant_term: ConstantTerm::Constant(ConstantLiteral::IRI(
                            ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "ex".to_string(),
                                name: "Class".to_string(),
                            }),
                        )),
                    }),
                },
                Parameter {
                    optional: false,
                    non_blank: false,
                    ptype: Some(PType::NEListType(Box::new(PType::ListType(Box::new(
                        PType::ListType(Box::new(PType::BasicType(PrefixedName {
                            prefix: "owl".to_string(),
                            name: "Class".to_string(),
                        }))),
                    ))))),
                    stottr_variable: StottrVariable {
                        name: "toppings".to_string(),
                    },
                    default_value: None,
                },
            ],
            annotation_list: None,
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_example_1() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:template [ ] :: { ex:template((ex:template)) } ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![Statement::Template(Template {
            signature: Signature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                parameter_list: vec![],
                annotation_list: None,
            },
            pattern_list: vec![Instance {
                list_expander: None,
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                argument_list: vec![Argument {
                    list_expand: false,
                    term: StottrTerm::ConstantTerm(ConstantTerm::ConstantList(vec![
                        ConstantTerm::Constant(ConstantLiteral::IRI(
                            ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "ex".to_string(),
                                name: "template".to_string(),
                            }),
                        )),
                    ])),
                }],
            }],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_example_2() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:template [?!?var ] :: { ex:template((((ex:template)))) } ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![Statement::Template(Template {
            signature: Signature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                parameter_list: vec![Parameter {
                    optional: true,
                    non_blank: true,
                    ptype: None,
                    stottr_variable: StottrVariable {
                        name: "var".to_string(),
                    },
                    default_value: None,
                }],
                annotation_list: None,
            },
            pattern_list: vec![Instance {
                list_expander: None,
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                argument_list: vec![Argument {
                    list_expand: false,
                    term: StottrTerm::ConstantTerm(ConstantTerm::ConstantList(vec![
                        ConstantTerm::ConstantList(vec![ConstantTerm::ConstantList(vec![
                            ConstantTerm::Constant(ConstantLiteral::IRI(
                                ResolvesToNamedNode::PrefixedName(PrefixedName {
                                    prefix: "ex".to_string(),
                                    name: "template".to_string(),
                                }),
                            )),
                        ])]),
                    ])),
                }],
            }],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_example_3() {
    let stottr = r#"@prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:template [ ] :: { ex:template(( ex:template )) } ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),
            }),
        ],
        statements: vec![Statement::Template(Template {
            signature: Signature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                parameter_list: vec![],
                annotation_list: None,
            },
            pattern_list: vec![Instance {
                list_expander: None,
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "template".to_string(),
                }),
                argument_list: vec![Argument {
                    list_expand: false,
                    term: StottrTerm::ConstantTerm(ConstantTerm::ConstantList(vec![
                        ConstantTerm::Constant(ConstantLiteral::IRI(
                            ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "ex".to_string(),
                                name: "template".to_string(),
                            }),
                        )),
                    ])),
                }],
            }],
        })],
    };
    assert_eq!(expected, doc);
}

#[test]
fn test_spec_example_4() {
    let stottr = r#"
    @prefix ex:<http://example.net/ns#>.
    @prefix p:<http://example.net/pizzas#>.
    ex:NamedPizza [
      ! owl:Class ?pizza = p:Grandiosa , ?! LUB<owl:NamedIndividual> ?country  , List<owl:Class> ?toppings
      ]
      @@ cross | ex:SomeAnnotationTemplate("asdf", "asdf", "asdf" ),
      @@<http://asdf>("asdf", "asdf", ++("A", "B", "C") )
      :: {
         cross | ex:Template1 (?pizza, ++?toppings) ,
         ex:Template2 (1, 2,4,   5) ,
         <http://Template2.com> ("asdf"^^xsd:string) ,
         zipMax | ex:Template4 ("asdf"^^xsd:string, ?pizza, ++( "a", "B" )),
         zipMax | ex:Template4 ([], [], [], ++([], []))
      } ."#;

    let (s, doc) = stottr_doc(stottr).finish().expect("Ok");
    assert_eq!("", s);
    let expected = StottrDocument {
        directives: vec![
            Directive::Prefix(Prefix {
                name: "ex".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/ns#"),
            }),
            Directive::Prefix(Prefix {
                name: "p".to_string(),
                iri: NamedNode::new_unchecked("http://example.net/pizzas#"),

            }),
        ],
        statements: vec![Statement::Template(Template {
            signature: Signature {
                template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                    prefix: "ex".to_string(),
                    name: "NamedPizza".to_string(),
                }),
                parameter_list: vec![
                    Parameter {
                        optional: false,
                        non_blank: true,
                        ptype: Some(PType::BasicType(PrefixedName {
                            prefix: "owl".to_string(),
                            name: "Class".to_string(),
                        })),
                        stottr_variable: StottrVariable { name: "pizza".to_string() },
                        default_value: Some(DefaultValue {
                            constant_term: ConstantTerm::Constant(ConstantLiteral::IRI(ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "p".to_string(),
                                name: "Grandiosa".to_string(),
                            }))),
                        }),
                    },
                    Parameter {
                        optional: true,
                        non_blank: true,
                        ptype: Some(PType::LUBType(Box::new(PType::BasicType(PrefixedName {
                            prefix: "owl".to_string(),
                            name: "NamedIndividual".to_string(),
                        })))),
                        stottr_variable: StottrVariable { name: "country".to_string() },
                        default_value: None,
                    },
                    Parameter {
                        optional: false,
                        non_blank: false,
                        ptype: Some(PType::ListType(Box::new(PType::BasicType(PrefixedName {
                            prefix: "owl".to_string(),
                            name: "Class".to_string(),
                        })))),
                        stottr_variable: StottrVariable { name: "toppings".to_string() },
                        default_value: None,
                    },
                ],
                annotation_list: Some(vec![
                    Annotation {
                        instance: Instance {
                            list_expander: Some(ListExpanderType::Cross),
                            template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "ex".to_string(),
                                name: "SomeAnnotationTemplate".to_string(),
                            }),
                            argument_list: vec![
                                Argument {
                                    list_expand: false,
                                    term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),

                                    }))),
                                },
                                Argument {
                                    list_expand: false,
                                    term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),

                                    }))),
                                },
                                Argument {
                                    list_expand: false,
                                    term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                    }))),
                                },
                            ],
                        },
                    },
                    Annotation {
                        instance: Instance {
                            list_expander: None,
                            template_name: ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://asdf" )),
                            argument_list: vec![
                                Argument {
                                    list_expand: false,
                                    term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                    }))),
                                },
                                Argument {
                                    list_expand: false,
                                    term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                        value: "asdf".to_string(),
                                        language: None,
                                        data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                    }))),
                                },
                                Argument {
                                    list_expand: true,
                                    term: StottrTerm::ConstantTerm(ConstantTerm::ConstantList(vec![
                                        ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                            value: "A".to_string(),
                                            language: None,
                                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),

                                        })),
                                        ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                            value: "B".to_string(),
                                            language: None,
                                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                        })),
                                        ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                            value: "C".to_string(),
                                            language: None,
                                            data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"))),
                                        })),
                                    ])),
                                },
                            ],
                        },
                    },
                ]),
            },
            pattern_list: vec![
                Instance {
                    list_expander: Some(ListExpanderType::Cross),
                    template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                        prefix: "ex".to_string(),
                        name: "Template1".to_string(),
                    }),
                    argument_list: vec![
                        Argument {
                            list_expand: false,
                            term: StottrTerm::Variable(StottrVariable { name: "pizza".to_string() }),
                        },
                        Argument {
                            list_expand: true,
                            term: StottrTerm::Variable(StottrVariable { name: "toppings".to_string() }),
                        },
                    ],
                },
                Instance {
                    list_expander: None,
                    template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                        prefix: "ex".to_string(),
                        name: "Template2".to_string(),
                    }),
                    argument_list: vec![
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                value: "1".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"))),
                            }))),
                        },
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                value: "2".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"))),
                            }))),
                        },
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                value: "4".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"))),
                            }))),
                        },
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                value: "5".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"))),
                            }))),
                        },
                    ],
                },
                Instance {
                    list_expander: None,
                    template_name: ResolvesToNamedNode::NamedNode(NamedNode::new_unchecked("http://Template2.com")),
                    argument_list: vec![Argument {
                        list_expand: false,
                        term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                            value: "asdf".to_string(),
                            language: None,
                            data_type_iri: Some(ResolvesToNamedNode::PrefixedName(PrefixedName {
                                prefix: "xsd".to_string(),
                                name: "string".to_string(),
                            })),
                        }))),
                    }],
                },
                Instance {
                    list_expander: Some(ListExpanderType::ZipMax),
                    template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                        prefix: "ex".to_string(),
                        name: "Template4".to_string(),
                    }),
                    argument_list: vec![
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                value: "asdf".to_string(),
                                language: None,
                                data_type_iri: Some(ResolvesToNamedNode::PrefixedName(PrefixedName {
                                    prefix: "xsd".to_string(),
                                    name: "string".to_string(),
                                })),
                            }))),
                        },
                        Argument {
                            list_expand: false,
                            term: StottrTerm::Variable(StottrVariable { name: "pizza".to_string() }),
                        },
                        Argument {
                            list_expand: true,
                            term: StottrTerm::ConstantTerm(ConstantTerm::ConstantList(vec![
                                ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                    value: "a".to_string(),
                                    language: None,
                                    data_type_iri: Some(ResolvesToNamedNode::NamedNode(xsd::STRING.into_owned())),
                                })),
                                ConstantTerm::Constant(ConstantLiteral::Literal(StottrLiteral {
                                    value: "B".to_string(),
                                    language: None,
                                    data_type_iri: Some(ResolvesToNamedNode::NamedNode(xsd::STRING.into_owned())),
                                })),
                            ])),
                        },
                    ],
                },
                Instance {
                    list_expander: Some(ListExpanderType::ZipMax),
                    template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
                        prefix: "ex".to_string(),
                        name: "Template4".to_string(),
                    }),
                    argument_list: vec![
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::BlankNode(BlankNode::new_unchecked(
                                "AnonymousBlankNode",
                            )))),
                        },
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::BlankNode(BlankNode::new_unchecked(
                                "AnonymousBlankNode",
                            )))),
                        },
                        Argument {
                            list_expand: false,
                            term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::BlankNode(BlankNode::new_unchecked(
                                "AnonymousBlankNode",
                            )))),
                        },
                        Argument {
                            list_expand: true,
                            term: StottrTerm::ConstantTerm(ConstantTerm::ConstantList(vec![
                                ConstantTerm::Constant(ConstantLiteral::BlankNode(BlankNode::new_unchecked("AnonymousBlankNode"))),
                                ConstantTerm::Constant(ConstantLiteral::BlankNode(BlankNode::new_unchecked("AnonymousBlankNode"))),
                            ])),
                        },
                    ],
                },
            ],
        })],
    };
    assert_eq!(expected, doc);
}

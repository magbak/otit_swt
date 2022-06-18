use crate::ast::{
    BooleanOperator, Connective, ElementConstraint, Glue, GraphPattern, Literal, PathElement,
    PathElementOrConnective, PathOrLiteral, TsQuery,
};
use crate::connective_mapping::ConnectiveMapping;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use spargebra::term::Literal as SpargebraLiteral;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashMap;

pub struct Translator<'a> {
    variables: Vec<Variable>,
    triples: Vec<TriplePattern>,
    optional_triples: Vec<Vec<TriplePattern>>,
    glue_to_nodes: HashMap<Glue, &'a Variable>,
    counter: u16,
    name_template: Vec<TriplePattern>,
    type_name_template: Vec<TriplePattern>,
}

enum VariableOrLiteral {
    Variable(Variable),
    Literal(SpargebraLiteral),
}

impl Translator<'_> {
    pub fn translate(
        &mut self,
        ts_query: &TsQuery,
        connective_mapping: &ConnectiveMapping,
    ) -> Query {
        let gp_res = self.translate_graph_pattern(&ts_query.graph_pattern, connective_mapping);
        Query::Select {
            dataset: None,
            pattern: Default::default(),
            base_iri: None
        }
    }
    fn translate_graph_pattern(
        &mut self,
        gp: &GraphPattern,
        connective_mapping: &ConnectiveMapping,
    ) -> spargebra::algebra::GraphPattern {
        let mut optional_counter = 0;
        for cp in &gp.conditioned_paths {
            let mut optional_index = None;
            if cp.lhs_path.optional {
                optional_index = Some(optional_counter);
            }
            let translated_lhs_variable = self.translate_path(
                None,
                optional_index,
                cp.lhs_path.path.iter().collect(),
                connective_mapping,
            );
            if let Some(op) = &cp.boolean_operator {
                if let Some(rhs_path_or_literal) = &cp.rhs_path_or_literal {
                    let translated_rhs_variable_or_literal = self.translate_path_or_literal(
                        optional_index,
                        rhs_path_or_literal,
                        connective_mapping,
                    );
                    self.add_condition(
                        optional_index,
                        translated_lhs_variable,
                        op,
                        translated_rhs_variable_or_literal,
                    );
                }
            }
            if cp.lhs_path.optional {
                optional_counter += 1
            }
        }
        self.basic_graph_pattern()
    }
    fn basic_graph_pattern(&self) -> spargebra::algebra::GraphPattern {
        spargebra::algebra::GraphPattern::Bgp {
            patterns: self.triples.clone(),
        }
    }
    fn translate_path(
        &mut self,
        input_first_variable: Option<&Variable>,
        optional_index: Option<usize>,
        path_elements: Vec<&PathElementOrConnective>,
        connective_mapping: &ConnectiveMapping,
    ) -> &Variable {
        let start_index;
        let first_variable;
        if let Some(first) = input_first_variable {
            assert!(path_elements.len() >= 2);
            start_index = 0;
            first_variable = first;
        } else {
            assert!(path_elements.len() >= 3);
            if let PathElementOrConnective::PathElement(pe) = path_elements.get(0).unwrap() {
                first_variable = self.add_path_element(optional_index, pe);
                start_index = 1;
            } else {
                panic!("Found unexpected connective");
            }
        }

        let first_elem = *path_elements.get(start_index).unwrap();
        let second_elem = *path_elements.get(start_index + 1).unwrap();
        if let PathElementOrConnective::Connective(c) = first_elem {
            if let PathElementOrConnective::PathElement(pe) = second_elem {
                let connective_named_node = translate_connective_named_node(c, connective_mapping);
                let last_variable = self.add_path_element(optional_index, pe);
                let triple_pattern = TriplePattern {
                    subject: TermPattern::Variable(first_variable.clone()),
                    predicate: NamedNodePattern::NamedNode(connective_named_node),
                    object: TermPattern::Variable(last_variable.clone()),
                };
                self.add_triple_pattern(triple_pattern, optional_index);

                if path_elements.len() > start_index + 2 {
                    self.translate_path(
                        Some(last_variable),
                        optional_index,
                        path_elements[start_index + 2..path_elements.len()].to_vec(),
                        connective_mapping,
                    )
                } else {
                    last_variable
                }
            } else {
                panic!("Bad path sequence")
            }
        } else {
            panic!("Bad path sequence")
        }
    }
    fn add_condition(
        &mut self,
        optional_index: Option<usize>,
        lhs_variable: &Variable,
        op: &BooleanOperator,
        rhs_variable_or_literal: VariableOrLiteral,
    ) {
    }

    fn add_path_element(
        &mut self,
        optional_index: Option<usize>,
        path_element: &PathElement,
    ) -> &Variable {
        if path_element.element.is_none() && path_element.glue.is_some() {
            let glue = &path_element.glue.unwrap();
            if self.glue_to_nodes.contains_key(glue) {
                return self.glue_to_nodes.get(glue).unwrap();
            } else {
                self.create_and_add_variable(
                    optional_index,
                    &path_element.element,
                    &path_element.glue,
                )
            }
        } else if path_element.element.is_some() && path_element.glue.is_some() {

        }
    }

    fn create_and_add_variable(
        &mut self,
        optional_index: Option<usize>,
        element_constraint: &Option<ElementConstraint>,
        glue_opt: &Option<Glue>,
    ) -> &Variable {
        let variable_name;
        if let Some(glue) = glue_opt {
            variable_name = glue.id.to_string();
        } else {
            variable_name = format!("v_{}", self.counter);
            self.counter += 1;
        }
        let variable = Variable::new(variable_name).expect("Invalid variable name");

        if let Some(ec) = element_constraint {
            self.add_element_constraint_to_variable(optional_index, ec, &variable);
        }
        self.variables.push(variable);
        self.variables.get(self.variables.len() - 1).unwrap()
    }

    fn add_element_constraint_to_variable(
        &mut self,
        optional_index: Option<usize>,
        ec: &ElementConstraint,
        variable: &Variable,
    ) {
        match ec {
            ElementConstraint::Name(n) => {
                let name_triples = self.name_func(n, variable);
                for name_triple in name_triples {
                    self.add_triple_pattern(name_triple, optional_index);
                }
            }
            ElementConstraint::TypeName(tn) => {
                let type_name_triples = self.type_name_func(tn, variable);
                for type_name_triple in type_name_triples {
                    self.add_triple_pattern(type_name_triple, optional_index);
                }
            }
            ElementConstraint::TypeNameAndName(tn, n) => {
                let name_triples = self.name_func(n, variable);
                for name_triple in name_triples {
                    self.add_triple_pattern(name_triple, optional_index);
                }
                let type_name_triples = self.type_name_func(tn, variable);
                for type_name_triple in type_name_triples {
                    self.add_triple_pattern(type_name_triple, optional_index);
                }
            }
        }
    }

    fn add_triple_pattern(&mut self, triple_pattern: TriplePattern, optional_index: Option<usize>) {
        if let Some(i) = optional_index {
            self.optional_triples
                .get_mut(i)
                .unwrap()
                .push(triple_pattern);
        } else {
            self.triples.push(triple_pattern);
        }
    }

    fn translate_path_or_literal(
        &mut self,
        optional_index: Option<usize>,
        path_or_literal: &PathOrLiteral,
        connective_mapping: &ConnectiveMapping,
    ) -> VariableOrLiteral {
        match path_or_literal {
            PathOrLiteral::Path(p) => {
                assert!(!p.optional);
                //optional from lhs of condition always dominates, we do not expect p.optional to be set.
                let variable = self.translate_path(
                    None,
                    optional_index,
                    p.path.iter().collect(),
                    &connective_mapping,
                );
                VariableOrLiteral::Variable(variable.clone())
            }
            PathOrLiteral::Literal(l) => {
                let literal = match l {
                    Literal::Real(r) => {
                        SpargebraLiteral::new_typed_literal(r.to_string(), xsd::DOUBLE)
                    }
                    Literal::Integer(i) => {
                        SpargebraLiteral::new_typed_literal(i.to_string(), xsd::INTEGER)
                    }
                    Literal::String(s) => {
                        SpargebraLiteral::new_typed_literal(s.to_string(), xsd::STRING)
                    }
                    Literal::Boolean(b) => {
                        SpargebraLiteral::new_typed_literal(b.to_string(), xsd::BOOLEAN)
                    }
                };
                VariableOrLiteral::Literal(literal)
            }
        }
    }
    fn name_func(&mut self, name: &String, variable: &Variable) -> Vec<TriplePattern> {
        self.fill_triples_template(&self.name_template, name, variable)
    }

    fn type_name_func(&mut self, type_name: &String, variable: &Variable) -> Vec<TriplePattern> {
        self.fill_triples_template(&self.type_name_template, type_name, variable)
    }

    fn fill_triples_template(
        &mut self,
        name_template: &Vec<TriplePattern>,
        replace_str: &str,
        replace_variable: &Variable,
    ) -> Vec<TriplePattern> {
        let mut map = HashMap::new();
        let mut triples = vec![];
        for t in name_template {
            let subject_term_pattern;
            if let TermPattern::Variable(subject_variable) = &t.subject {
                if !map.contains_key(subject_variable) {
                    let use_subject_variable;
                    if "replace_variable" == subject_variable.as_str() {
                        use_subject_variable = replace_variable.clone();
                    } else {
                        use_subject_variable = Variable::new_unchecked(format!(
                            "{}_{}",
                            subject_variable.as_str().to_string(),
                            self.counter
                        ));
                        self.counter += 1;
                    }
                    subject_term_pattern = TermPattern::Variable(use_subject_variable);
                    map.insert(subject_variable, subject_term_pattern);
                } else {
                    subject_term_pattern = map.get(subject_variable).unwrap().clone();
                }
            } else {
                subject_term_pattern = t.subject.clone();
            }
            let object_term_pattern;
            if let TermPattern::Variable(object_variable) = &t.object {
                if !map.contains_key(object_variable) {
                    let use_object_variable;
                    if "replace_variable" == object_variable.as_str() {
                        use_object_variable = replace_variable.clone();
                    } else {
                        use_object_variable = Variable::new_unchecked(format!(
                            "{}_{}",
                            object_variable.as_str().to_string(),
                            self.counter
                        ));
                        self.counter += 1;
                    }
                    object_term_pattern = TermPattern::Variable(use_object_variable);
                    map.insert(object_variable, object_term_pattern);
                } else {
                    object_term_pattern = map.get(object_variable).unwrap().clone();
                }
            } else if let TermPattern::Literal(lit) = &t.object {
                let use_object_literal;
                if lit.datatype() == xsd::STRING && lit.value() == "replace_str" {
                    use_object_literal =
                        SpargebraLiteral::new_typed_literal(replace_str, xsd::STRING);
                } else {
                    use_object_literal = lit.clone();
                }
                object_term_pattern = TermPattern::Literal(use_object_literal);
            } else {
                object_term_pattern = t.object.clone();
            }
            triples.push(TriplePattern {
                subject: subject_term_pattern,
                predicate: t.predicate.clone(),
                object: object_term_pattern,
            })
        }
        triples
    }
}

fn translate_connective_named_node(
    connective: &Connective,
    connective_mapping: &ConnectiveMapping,
) -> NamedNode {
    let connective_string = connective.to_string();
    let iri = connective_mapping
        .map
        .get(&connective_string)
        .expect(&format!("Connective {} not defined", &connective_string));
    NamedNode::new(iri).expect("Invalid iri")
}

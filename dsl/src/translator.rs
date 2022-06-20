use crate::ast::{
    BooleanOperator, Connective, ElementConstraint, Glue, GraphPattern, Literal, PathElement,
    PathElementOrConnective, PathOrLiteral, TsQuery,
};
use crate::connective_mapping::ConnectiveMapping;
use crate::costants::LIKE_FUNCTION;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use spargebra::algebra::GraphPattern::LeftJoin;
use spargebra::algebra::{Expression, Function, GraphPattern as SpargebraGraphPattern};
use spargebra::term::Literal as SpargebraLiteral;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashMap;

pub struct Translator<'a> {
    variables: Vec<Variable>,
    triples: Vec<TriplePattern>,
    conditions: Vec<Expression>,
    optional_triples: Vec<Vec<TriplePattern>>,
    optional_conditions: Vec<Option<Expression>>,
    glue_to_nodes: HashMap<Glue, &'a Variable>,
    counter: u16,
    name_template: Vec<TriplePattern>,
    type_name_template: Vec<TriplePattern>,
    projections: Vec<Variable>,
}

enum VariableOrLiteral {
    Variable(Variable),
    Literal(SpargebraLiteral),
}

enum TemplateType {
    TypeTemplate,
    NameTemplate,
}

impl Translator<'_> {
    pub fn translate(
        &mut self,
        ts_query: &TsQuery,
        connective_mapping: &ConnectiveMapping,
    ) -> Query {
        self.translate_graph_pattern(&ts_query.graph_pattern, connective_mapping);
        let mut inner_gp = SpargebraGraphPattern::Bgp {
            patterns: self.triples.drain(0..self.triples.len()).collect(),
        };
        for (optional_pattern, expressions_opt) in self
            .optional_triples
            .drain(0..self.optional_triples.len())
            .zip(
                self.optional_conditions
                    .drain(0..self.optional_conditions.len()),
            )
        {
            let mut optional_gp = SpargebraGraphPattern::Bgp {
                patterns: optional_pattern,
            };

            if let Some(expression) = expressions_opt {
                optional_gp = SpargebraGraphPattern::Filter {
                    expr: expression,
                    inner: Box::new(optional_gp),
                }
            }

            inner_gp = LeftJoin {
                left: Box::new(inner_gp),
                right: Box::new(optional_gp),
                expression: None,
            };
        }
        if !self.conditions.is_empty() {
            let mut conjuction = self.conditions.remove(0);
            for c in self.conditions.drain(0..self.conditions.len()) {
                conjuction = Expression::And(Box::new(conjuction), Box::new(c));
            }
            inner_gp = SpargebraGraphPattern::Filter { expr: conjuction, inner: Box::new(inner_gp) };
        }

        let project = SpargebraGraphPattern::Project {
            inner: Box::new(inner_gp),
            variables: self.projections.drain(0..self.projections.len()).collect(),
        };

        Query::Select {
            dataset: None,
            pattern: project,
            base_iri: None,
        }
    }
    fn translate_graph_pattern(
        &mut self,
        gp: &GraphPattern,
        connective_mapping: &ConnectiveMapping,
    ) {
        let mut optional_counter = 0;
        for cp in &gp.conditioned_paths {
            let mut optional_index = None;
            if cp.lhs_path.optional {
                optional_index = Some(optional_counter);
            }
            let translated_lhs_variable = self
                .translate_path(
                    &mut vec![],
                    None,
                    optional_index,
                    cp.lhs_path.path.iter().collect(),
                    connective_mapping,
                )
                .clone();
            self.projections.push(translated_lhs_variable.clone());
            if let Some(op) = &cp.boolean_operator {
                if let Some(rhs_path_or_literal) = &cp.rhs_path_or_literal {
                    let translated_rhs_variable_or_literal = self.translate_path_or_literal(
                        &mut vec![],
                        optional_index,
                        rhs_path_or_literal,
                        connective_mapping,
                    );
                    self.add_condition(
                        optional_index,
                        &translated_lhs_variable,
                        op,
                        translated_rhs_variable_or_literal,
                    );
                }
            }
            if cp.lhs_path.optional {
                optional_counter += 1
            }
        }
    }

    fn translate_path(
        &mut self,
        path_identifier: &mut Vec<String>,
        input_first_variable: Option<Variable>,
        optional_index: Option<usize>,
        path_elements: Vec<&PathElementOrConnective>,
        connective_mapping: &ConnectiveMapping,
    ) -> Variable {
        let start_index;
        let first_variable;
        if let Some(first) = input_first_variable {
            assert!(path_elements.len() >= 2);
            start_index = 0;
            first_variable = first;
        } else {
            assert!(path_elements.len() >= 3);
            if let PathElementOrConnective::PathElement(pe) = path_elements.get(0).unwrap() {
                first_variable = self
                    .add_path_element(path_identifier, optional_index, pe)
                    .clone();
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
                let last_variable = self
                    .add_path_element(path_identifier, optional_index, pe)
                    .clone();
                let triple_pattern = TriplePattern {
                    subject: TermPattern::Variable(first_variable.clone()),
                    predicate: NamedNodePattern::NamedNode(connective_named_node),
                    object: TermPattern::Variable(last_variable.clone()),
                };
                self.add_triple_pattern(triple_pattern, optional_index);
                path_identifier.push(c.to_string());
                if path_elements.len() > start_index + 2 {
                    self.translate_path(
                        path_identifier,
                        Some(last_variable.clone()),
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
        let lhs_expression = Expression::Variable(lhs_variable.clone());
        let rhs_expression = match rhs_variable_or_literal {
            VariableOrLiteral::Variable(v) => Expression::Variable(v),
            VariableOrLiteral::Literal(l) => Expression::Literal(l),
        };
        let mapped_expression = match op {
            BooleanOperator::NEQ => Expression::Not(Box::new(Expression::Equal(
                Box::new(lhs_expression),
                Box::new(rhs_expression),
            ))),
            BooleanOperator::EQ => Expression::Equal(
                Box::new(lhs_expression),
                Box::new(rhs_expression),
            ),
            BooleanOperator::LTEQ => Expression::LessOrEqual(
                Box::new(lhs_expression),
                Box::new(rhs_expression),
            ),
            BooleanOperator::GTEQ =>Expression::GreaterOrEqual(
                Box::new(lhs_expression),
                Box::new(rhs_expression),
            ),
            BooleanOperator::LT =>Expression::Less(
                Box::new(lhs_expression),
                Box::new(rhs_expression),
            ),
            BooleanOperator::GT =>Expression::Greater(
                Box::new(lhs_expression),
                Box::new(rhs_expression),
            ),
            BooleanOperator::LIKE => Expression::FunctionCall(
                Function::Custom(NamedNode::new_unchecked(LIKE_FUNCTION)),
                vec![rhs_expression],
            ),
        };

        if let Some(_) = optional_index {
            self.optional_conditions.push(Some(mapped_expression));
        } else {
            self.optional_conditions.push(None);
            self.conditions.push(mapped_expression);
        }
    }

    fn add_path_element(
        &mut self,
        path_identifier: &mut Vec<String>,
        optional_index: Option<usize>,
        path_element: &PathElement,
    ) -> Variable {
        let variable;
        if let Some(glue) = &path_element.glue {
            path_identifier.clear();
            path_identifier.push(path_element.glue.as_ref().unwrap().id.clone());

            if self.glue_to_nodes.contains_key(glue) {
                variable = (*self.glue_to_nodes.get(glue).unwrap()).clone();
            } else {
                variable = self.create_and_add_variable(&path_identifier.join(""));
            }
        } else if let Some(element) = &path_element.element {
            match element {
                ElementConstraint::Name(n) => {
                    path_identifier.push(format!("\"{}\"", n));
                }
                ElementConstraint::TypeName(tn) => {
                    path_identifier.push(tn.to_string());
                }
                ElementConstraint::TypeNameAndName(tn, n) => {
                    path_identifier.push(tn.to_string());
                    path_identifier.push(format!("\"{}\"", n));
                }
            }
            variable = self.create_and_add_variable(&path_identifier.join(""));
        } else {
            panic!("Either element or glue must be set")
        }

        if let Some(element) = &path_element.element {
            self.add_element_constraint_to_variable(optional_index, element, &variable);
        }
        variable
    }

    fn create_and_add_variable(&mut self, variable_name: &str) -> Variable {
        let variable = Variable::new(variable_name).expect("Invalid variable name");

        self.variables.push(variable);
        self.variables
            .get(self.variables.len() - 1)
            .unwrap()
            .clone()
    }

    fn add_element_constraint_to_variable(
        &mut self,
        optional_index: Option<usize>,
        ec: &ElementConstraint,
        variable: &Variable,
    ) {
        match ec {
            ElementConstraint::Name(n) => {
                let name_triples =
                    self.fill_triples_template(TemplateType::NameTemplate, n, variable);
                for name_triple in name_triples {
                    self.add_triple_pattern(name_triple, optional_index);
                }
            }
            ElementConstraint::TypeName(tn) => {
                let type_name_triples =
                    self.fill_triples_template(TemplateType::TypeTemplate, tn, variable);
                for type_name_triple in type_name_triples {
                    self.add_triple_pattern(type_name_triple, optional_index);
                }
            }
            ElementConstraint::TypeNameAndName(tn, n) => {
                let name_triples =
                    self.fill_triples_template(TemplateType::NameTemplate, n, variable);
                for name_triple in name_triples {
                    self.add_triple_pattern(name_triple, optional_index);
                }
                let type_name_triples =
                    self.fill_triples_template(TemplateType::TypeTemplate, tn, variable);
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
        path_identifier: &mut Vec<String>,
        optional_index: Option<usize>,
        path_or_literal: &PathOrLiteral,
        connective_mapping: &ConnectiveMapping,
    ) -> VariableOrLiteral {
        match path_or_literal {
            PathOrLiteral::Path(p) => {
                assert!(!p.optional);
                //optional from lhs of condition always dominates, we do not expect p.optional to be set.
                let variable = self.translate_path(
                    path_identifier,
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

    fn fill_triples_template(
        &mut self,
        template_type: TemplateType,
        replace_str: &str,
        replace_variable: &Variable,
    ) -> Vec<TriplePattern> {
        let template = match template_type {
            TemplateType::TypeTemplate => &self.type_name_template,
            TemplateType::NameTemplate => &self.name_template,
        };
        let mut map = HashMap::new();
        let mut triples = vec![];
        for t in template {
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
                    map.insert(subject_variable, subject_term_pattern.clone());
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
                    map.insert(object_variable, object_term_pattern.clone());
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

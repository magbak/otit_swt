use crate::ast::{
    BooleanOperator, Connective, ElementConstraint, GraphPathPattern, LiteralData,
    PathElement, PathElementOrConnective, PathOrLiteralData, TsQuery,
};
use crate::connective_mapping::ConnectiveMapping;
use crate::costants::{
    DATETIME_AS_NANOS, HAS_TIMESERIES, HAS_TIMESTAMP, HAS_VALUE, LIKE_FUNCTION,
    REPLACE_STR_LITERAL, REPLACE_VARIABLE_NAME, TIMESTAMP_VARIABLE_NAME, XSD_DATETIME_FORMAT,
};
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, Variable};
use spargebra::algebra::{
    AggregateExpression, Expression, Function, GraphPattern
};
use spargebra::term::Literal;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::iter::zip;

#[derive(Debug)]
pub struct VariablePathExpression {
    pub(crate) variable: Variable,
    pub(crate) path_variable: Variable,
    pub(crate) path_to_variable_expression: Expression,
}

pub struct Translator {
    variables: Vec<Variable>,
    triples: Vec<TriplePattern>,
    conditions: Vec<Expression>,
    path_name_expressions: Vec<VariablePathExpression>,
    group_path_name_expressions: Vec<VariablePathExpression>,
    optional_triples: Vec<Vec<TriplePattern>>,
    optional_conditions: Vec<Option<Expression>>,
    optional_path_name_expressions: Vec<Option<VariablePathExpression>>,
    variable_has_path_name: HashMap<Variable, Variable>,
    variable_has_value: HashMap<Variable, Variable>,
    counter: u16,
    name_template: Vec<TriplePattern>,
    type_name_template: Vec<TriplePattern>,
    has_outgoing: HashSet<Variable>,
    is_lhs_terminal: HashSet<Variable>,
    connective_mapping: ConnectiveMapping,
    group_by: Vec<String>,
    glue_variables: Vec<Variable>,
}

enum VariableOrLiteral {
    Variable(Variable),
    Literal(Literal),
}

enum TemplateType {
    TypeTemplate,
    NameTemplate,
}

impl Translator {
    pub fn new(
        name_template: Vec<TriplePattern>,
        type_name_template: Vec<TriplePattern>,
        connective_mapping: ConnectiveMapping,
    ) -> Translator {
        Translator {
            variables: vec![],
            triples: vec![],
            conditions: vec![],
            path_name_expressions: vec![],
            group_path_name_expressions: vec![],
            optional_triples: vec![],
            optional_conditions: vec![],
            optional_path_name_expressions: vec![],
            variable_has_path_name: Default::default(),
            variable_has_value: Default::default(),
            counter: 0,
            name_template,
            type_name_template,
            has_outgoing: Default::default(),
            is_lhs_terminal: Default::default(),
            connective_mapping,
            group_by: vec![],
            glue_variables: vec![],
        }
    }

    pub fn translate(&mut self, ts_query: &TsQuery) -> Query {
        if let Some(group) = &ts_query.group {
            self.group_by.extend(group.var_names.iter().cloned());
        }
        self.translate_graph_pattern(&ts_query.graph_pattern);
        let mut inner_gp = GraphPattern::Bgp {
            patterns: self.triples.drain(0..self.triples.len()).collect(),
        };
        let optional_triples_drain = self.optional_triples.drain(0..self.optional_triples.len());
        let optional_path_name_expressions_drain = self
            .optional_path_name_expressions
            .drain(0..self.optional_path_name_expressions.len());
        let optional_conditions_drain = self
            .optional_conditions
            .drain(0..self.optional_conditions.len());

        let mut project_values = vec![];
        let mut project_paths = vec![];
        for (optional_pattern, (path_name_expression_opt, conditions_opt)) in zip(
            optional_triples_drain,
            zip(
                optional_path_name_expressions_drain,
                optional_conditions_drain,
            ),
        ) {
            let mut optional_gp = GraphPattern::Bgp {
                patterns: optional_pattern,
            };

            if let Some(condition) = conditions_opt {
                optional_gp = GraphPattern::Filter {
                    expr: condition,
                    inner: Box::new(optional_gp),
                }
            }

            if let Some(variable_path_expression) = path_name_expression_opt {
                if !self
                    .has_outgoing
                    .contains(&variable_path_expression.variable)
                {
                    optional_gp = GraphPattern::Extend {
                        inner: Box::new(optional_gp),
                        variable: variable_path_expression.path_variable.clone(),
                        expression: variable_path_expression.path_to_variable_expression.clone(),
                    };
                    project_paths.push(variable_path_expression.path_variable.clone());
                    let value_var = self
                        .variable_has_value
                        .get(&variable_path_expression.variable)
                        .expect("Optional variable path has value");
                    project_values.push(value_var.clone());
                }
            }

            inner_gp = GraphPattern::LeftJoin {
                left: Box::new(inner_gp),
                right: Box::new(optional_gp),
                expression: None,
            };
        }

        let mut timestamp_conditions = vec![];
        if let Some(from_ts) = ts_query.from_datetime {
            let gteq = Expression::GreaterOrEqual(
                Box::new(Expression::Literal(Literal::new_typed_literal(
                    format!("{}", from_ts.format(XSD_DATETIME_FORMAT)),
                    xsd::DATE_TIME,
                ))),
                Box::new(Expression::Variable(Variable::new_unchecked(
                    TIMESTAMP_VARIABLE_NAME,
                ))),
            );
            timestamp_conditions.push(gteq);
        }

        if let Some(to_ts) = ts_query.to_datetime {
            let gteq = Expression::LessOrEqual(
                Box::new(Expression::Literal(Literal::new_typed_literal(
                    format!("{}", to_ts.format(XSD_DATETIME_FORMAT)),
                    xsd::DATE_TIME,
                ))),
                Box::new(Expression::Variable(Variable::new_unchecked(
                    TIMESTAMP_VARIABLE_NAME,
                ))),
            );
            timestamp_conditions.push(gteq);
        }
        self.conditions.append(&mut timestamp_conditions);

        if !self.conditions.is_empty() {
            let mut conjuction = self.conditions.remove(0);
            for c in self.conditions.drain(0..self.conditions.len()) {
                conjuction = Expression::And(Box::new(conjuction), Box::new(c));
            }
            inner_gp = GraphPattern::Filter {
                expr: conjuction,
                inner: Box::new(inner_gp),
            };
        }

        for variable_path_expression in self
            .path_name_expressions
            .drain(0..self.path_name_expressions.len())
        {
            println!("{:?}", variable_path_expression);
            if !self
                .has_outgoing
                .contains(&variable_path_expression.variable)
            {
                inner_gp = GraphPattern::Extend {
                    inner: Box::new(inner_gp),
                    variable: variable_path_expression.path_variable.clone(),
                    expression: variable_path_expression.path_to_variable_expression.clone(),
                };
                project_paths.push(variable_path_expression.path_variable.clone());
                let value_variable = self
                    .variable_has_value
                    .get(&variable_path_expression.variable)
                    .expect("Value variable associated with path");
                project_values.push(value_variable.clone());
            }
        }

        if let Some(aggregation) = &ts_query.aggregation {
            let timestamp_variable_expression =
                Expression::Variable(Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME));
            //TODO: Is this safe?
            let duration_nanoseconds = aggregation.duration.as_nanos() as u64;

            let grouping_col_expression = Expression::FunctionCall(
                Function::Floor,
                vec![Expression::Divide(
                    Box::new(Expression::FunctionCall(
                        Function::Custom(NamedNode::new_unchecked(DATETIME_AS_NANOS)),
                        vec![timestamp_variable_expression.clone()],
                    )),
                    Box::new(Expression::Literal(Literal::new_typed_literal(
                        duration_nanoseconds.to_string(),
                        xsd::UNSIGNED_LONG,
                    ))),
                )],
            );
            //TODO: Performance - use less precision when grouping is coarse grained
            let timestamp_grouping_variable =
                Variable::new_unchecked(format!("{}_grouping", TIMESTAMP_VARIABLE_NAME));
            inner_gp = GraphPattern::Extend {
                inner: Box::new(inner_gp),
                variable: timestamp_grouping_variable.clone(),
                expression: grouping_col_expression,
            };
            let mut grouping_cols = project_paths.clone();
            grouping_cols.push(timestamp_grouping_variable.clone());

            let mut aggregates = vec![];
            for val_col in &project_values {
                let agg_expr = match aggregation.function_name.as_str() {
                    "mean" | "avg" => AggregateExpression::Avg {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "max" | "maximum" => AggregateExpression::Max {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "min" | "minimum" => AggregateExpression::Min {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "sum" => AggregateExpression::Sum {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "sample" => AggregateExpression::Sample {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "count" => AggregateExpression::Count {
                        expr: Some(Box::new(Expression::Variable(val_col.clone()))),
                        distinct: false,
                    },
                    _ => {panic!("Not found!!!")}
                };
                aggregates.push((val_col.clone(), agg_expr));
            }

            inner_gp = GraphPattern::Group {
                inner: Box::new(inner_gp),
                variables: grouping_cols,
                aggregates,
            };
            inner_gp = GraphPattern::Extend {
                inner: Box::new(inner_gp),
                variable: Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME),
                expression: Expression::FunctionCall(Function::Custom(NamedNode::from(xsd::DATE_TIME)), vec![Expression::Variable(timestamp_grouping_variable)])
            }

        }

        let mut all_projections = project_paths;
        let has_value = !project_values.is_empty();
        all_projections.append(&mut project_values);
        if has_value {
            all_projections.push(Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME));
        }

        if let Some(group) = &ts_query.group {
            for var_name in &group.var_names {
                let var = self
                    .glue_variables
                    .iter()
                    .find(|var| var.as_str() == var_name)
                    .unwrap();
                for vp in &self.group_path_name_expressions {
                    if &vp.variable == var {
                        inner_gp = GraphPattern::Extend {
                            inner: Box::new(inner_gp),
                            variable: vp.path_variable.clone(),
                            expression: vp.path_to_variable_expression.clone(),
                        };
                    }
                }
            }
            inner_gp = GraphPattern::Group {
                inner: Box::new(Default::default()),
                variables: vec![],
                aggregates: vec![],
            };
        }

        let project = GraphPattern::Project {
            inner: Box::new(inner_gp),
            variables: all_projections,
        };

        Query::Select {
            dataset: None,
            pattern: project,
            base_iri: None,
        }
    }
    fn translate_graph_pattern(&mut self, gp: &GraphPathPattern) {
        let mut optional_counter = 0;
        for cp in &gp.conditioned_paths {
            println!("cp: {:?}", cp);
            let mut optional_index = None;
            if cp.lhs_path.optional {
                optional_index = Some(optional_counter);
            }
            let mut translated_lhs_variable_path = vec![];
            self.translate_path(
                &mut vec![],
                &mut translated_lhs_variable_path,
                optional_index,
                cp.lhs_path.path.iter().collect(),
            );
            let translated_lhs_value_variable = self.add_value_and_timeseries_variable(
                optional_index,
                translated_lhs_variable_path.last().unwrap(),
            );

            self.is_lhs_terminal
                .insert(translated_lhs_value_variable.clone());
            let connectives_path = cp
                .lhs_path
                .path
                .iter()
                .map(|p| match p {
                    PathElementOrConnective::PathElement(_) => None,
                    PathElementOrConnective::Connective(c) => Some(c.to_string()),
                })
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect();
            self.create_name_path_variable(
                optional_index,
                translated_lhs_variable_path,
                connectives_path,
            );
            if let Some(op) = &cp.boolean_operator {
                if let Some(rhs_path_or_literal) = &cp.rhs_path_or_literal {
                    let translated_rhs_variable_or_literal = self.translate_path_or_literal(
                        &mut vec![],
                        optional_index,
                        rhs_path_or_literal,
                    );
                    let translated_rhs_value_variable_or_literal =
                        match translated_rhs_variable_or_literal {
                            VariableOrLiteral::Variable(rhs_end) => VariableOrLiteral::Variable(
                                self.add_value_and_timeseries_variable(optional_index, &rhs_end),
                            ),
                            VariableOrLiteral::Literal(l) => VariableOrLiteral::Literal(l),
                        };
                    self.add_condition(
                        optional_index,
                        &translated_lhs_value_variable,
                        op,
                        translated_rhs_value_variable_or_literal,
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
        variable_path_so_far: &mut Vec<Variable>,
        optional_index: Option<usize>,
        path_elements: Vec<&PathElementOrConnective>,
    ) {
        let start_index;
        let first_variable;
        if !variable_path_so_far.is_empty() {
            let first = variable_path_so_far.last().unwrap();
            assert!(path_elements.len() >= 2);
            start_index = 0;
            first_variable = first.clone();
        } else {
            assert!(path_elements.len() >= 3);
            if let PathElementOrConnective::PathElement(pe) = path_elements.get(0).unwrap() {
                first_variable = self
                    .add_path_element(path_identifier, optional_index, pe)
                    .clone();
                start_index = 1;
                variable_path_so_far.push(first_variable.clone());
            } else {
                panic!("Found unexpected connective");
            }
        }

        let first_elem = *path_elements.get(start_index).unwrap();
        let second_elem = *path_elements.get(start_index + 1).unwrap();
        if let PathElementOrConnective::Connective(c) = first_elem {
            if let PathElementOrConnective::PathElement(pe) = second_elem {
                let connective_named_node = self.translate_connective_named_node(c);
                let last_variable = self
                    .add_path_element(path_identifier, optional_index, pe)
                    .clone();
                variable_path_so_far.push(last_variable.clone());
                self.has_outgoing.insert(first_variable.clone());
                let triple_pattern = TriplePattern {
                    subject: TermPattern::Variable(first_variable.clone()),
                    predicate: NamedNodePattern::NamedNode(connective_named_node),
                    object: TermPattern::Variable(last_variable),
                };
                self.add_triple_pattern(triple_pattern, optional_index);
                path_identifier.push(format!("__{}__", c.to_variable_name_part()));
                if path_elements.len() > start_index + 2 {
                    self.translate_path(
                        path_identifier,
                        variable_path_so_far,
                        optional_index,
                        path_elements[start_index + 2..path_elements.len()].to_vec(),
                    )
                } else {
                    //Finished
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
            BooleanOperator::EQ => {
                Expression::Equal(Box::new(lhs_expression), Box::new(rhs_expression))
            }
            BooleanOperator::LTEQ => {
                Expression::LessOrEqual(Box::new(lhs_expression), Box::new(rhs_expression))
            }
            BooleanOperator::GTEQ => {
                Expression::GreaterOrEqual(Box::new(lhs_expression), Box::new(rhs_expression))
            }
            BooleanOperator::LT => {
                Expression::Less(Box::new(lhs_expression), Box::new(rhs_expression))
            }
            BooleanOperator::GT => {
                Expression::Greater(Box::new(lhs_expression), Box::new(rhs_expression))
            }
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

            if let Some(glue_var) = self.glue_variables.iter().find(|x| x.as_str() == &glue.id) {
                variable = glue_var.clone();
            } else {
                variable = self.create_and_add_variable(&path_identifier.join(""));
                self.glue_variables.push(variable.clone())
            }
        } else if let Some(element) = &path_element.element {
            match element {
                ElementConstraint::Name(n) => {
                    path_identifier.push(format!("_{}_", n));
                }
                ElementConstraint::TypeName(tn) => {
                    path_identifier.push(tn.to_string());
                }
                ElementConstraint::TypeNameAndName(tn, n) => {
                    path_identifier.push(tn.to_string());
                    path_identifier.push(format!("_{}_", n));
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
        let variable = Variable::new(variable_name)
            .expect(&format!("Invalid variable name: {}", variable_name));

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
                    self.fill_triples_template(TemplateType::NameTemplate, Some(n), None, variable);
                for name_triple in name_triples {
                    self.add_triple_pattern(name_triple, optional_index);
                }
            }
            ElementConstraint::TypeName(tn) => {
                let type_name_triples = self.fill_triples_template(
                    TemplateType::TypeTemplate,
                    Some(tn),
                    None,
                    variable,
                );
                for type_name_triple in type_name_triples {
                    self.add_triple_pattern(type_name_triple, optional_index);
                }
            }
            ElementConstraint::TypeNameAndName(tn, n) => {
                let name_triples =
                    self.fill_triples_template(TemplateType::NameTemplate, Some(n), None, variable);
                for name_triple in name_triples {
                    self.add_triple_pattern(name_triple, optional_index);
                }
                let type_name_triples = self.fill_triples_template(
                    TemplateType::TypeTemplate,
                    Some(tn),
                    None,
                    variable,
                );
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
        path_or_literal: &PathOrLiteralData,
    ) -> VariableOrLiteral {
        match path_or_literal {
            PathOrLiteralData::Path(p) => {
                assert!(!p.optional);
                //optional from lhs of condition always dominates, we do not expect p.optional to be set.
                let mut translated_path = vec![];
                self.translate_path(
                    path_identifier,
                    &mut translated_path,
                    optional_index,
                    p.path.iter().collect(),
                );
                VariableOrLiteral::Variable(translated_path.last().unwrap().clone())
            }
            PathOrLiteralData::Literal(l) => {
                let literal = match l {
                    LiteralData::Real(r) => {
                        Literal::new_typed_literal(r.to_string(), xsd::DOUBLE)
                    }
                    LiteralData::Integer(i) => {
                        Literal::new_typed_literal(i.to_string(), xsd::INTEGER)
                    }
                    LiteralData::String(s) => {
                        Literal::new_typed_literal(s.to_string(), xsd::STRING)
                    }
                    LiteralData::Boolean(b) => {
                        Literal::new_typed_literal(b.to_string(), xsd::BOOLEAN)
                    }
                };
                VariableOrLiteral::Literal(literal)
            }
        }
    }

    fn fill_triples_template(
        &mut self,
        template_type: TemplateType,
        replace_str: Option<&str>,
        replace_str_variable: Option<&Variable>,
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
                    if REPLACE_VARIABLE_NAME == subject_variable.as_str() {
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
                    if REPLACE_VARIABLE_NAME == object_variable.as_str() {
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
                if lit.datatype() == xsd::STRING && lit.value() == REPLACE_STR_LITERAL {
                    if let Some(replace_str) = replace_str {
                        object_term_pattern = TermPattern::Literal(
                            Literal::new_typed_literal(replace_str, xsd::STRING),
                        );
                    } else if let Some(replace_str_variable) = replace_str_variable {
                        object_term_pattern = TermPattern::Variable(replace_str_variable.clone())
                    } else {
                        panic!("Should never happen");
                    }
                } else {
                    object_term_pattern = TermPattern::Literal(lit.clone());
                }
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
    fn create_name_path_variable(
        &mut self,
        optional_index: Option<usize>,
        variables_path: Vec<Variable>,
        mut connectives_path: Vec<String>,
    ) {
        let mut variable_names_path = vec![];
        let mut glue_names_path = vec![];
        for (i, v) in variables_path.iter().enumerate() {
            let vname = Variable::new_unchecked(format!("{}_name_on_path", v.as_str()));
            variable_names_path.push(vname.clone());
            let triples =
                self.fill_triples_template(TemplateType::NameTemplate, None, Some(&vname), v);
            for t in triples {
                self.add_triple_pattern(t, optional_index);
            }
            if !self.variable_has_path_name.contains_key(v)
                && self.group_by.contains(&v.as_str().to_string())
                && i < variables_path.len() - 1
            {
                glue_names_path.push((v.clone(), variables_path.clone()));
            }
        }
        fn build_concat_path_expression(
            variable_names_path: Vec<Variable>,
            connectives_path: Vec<&String>,
        ) -> Expression {
            println!("{:?}_{:?}", variable_names_path, connectives_path);
            assert_eq!(variable_names_path.len(), connectives_path.len() + 1);
            let mut args_vec = vec![];
            for (vp, cc) in variable_names_path.iter().zip(connectives_path) {
                args_vec.push(Expression::Variable(vp.clone()));
                args_vec.push(Expression::Literal(Literal::new_typed_literal(
                    cc.clone(),
                    xsd::STRING,
                )));
            }
            //We must push the final variable since the connectives path has one less element.
            args_vec.push(Expression::Variable(
                variable_names_path.last().unwrap().clone(),
            ));

            Expression::FunctionCall(Function::Concat, args_vec)
        }

        let path_string_expression =
            build_concat_path_expression(variable_names_path, connectives_path.iter().collect());

        let last_variable = variables_path.last().unwrap().clone();
        let path_variable =
            Variable::new_unchecked(format!("{}_path_name", last_variable.as_str()));
        self.variable_has_path_name
            .insert(last_variable.clone(), path_variable.clone());
        let expr = VariablePathExpression {
            variable: last_variable.clone(),
            path_variable: path_variable.clone(),
            path_to_variable_expression: path_string_expression,
        };
        if let Some(_) = optional_index {
            self.optional_path_name_expressions.push(Some(expr));
        } else {
            self.path_name_expressions.push(expr);
            self.optional_path_name_expressions.push(None);
        }

        for (g, path) in glue_names_path {
            let path_len = path.len();
            let path_string_expression = build_concat_path_expression(
                path,
                connectives_path[0..(path_len - 1)].iter().collect(),
            );
            let path_variable = Variable::new_unchecked(format!("{}_path_name", g.as_str()));
            let expr = VariablePathExpression {
                variable: g.clone(),
                path_variable: path_variable.clone(),
                path_to_variable_expression: path_string_expression,
            };
            self.variable_has_path_name
                .insert(g.clone(), path_variable.clone());
            self.group_path_name_expressions.push(expr);
        }
    }

    fn add_value_and_timeseries_variable(
        &mut self,
        optional_index: Option<usize>,
        end_variable: &Variable,
    ) -> Variable {
        let timeseries_variable =
            Variable::new_unchecked(format!("{}_timeseries", end_variable.as_str()));
        let has_timeseries_triple = TriplePattern {
            subject: TermPattern::Variable(end_variable.clone()),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_TIMESERIES)),
            object: TermPattern::Variable(timeseries_variable.clone()),
        };
        let datapoint_variable =
            Variable::new_unchecked(format!("{}_datapoint", timeseries_variable.as_str()));
        let has_datapoint_triple = TriplePattern {
            subject: TermPattern::Variable(timeseries_variable.clone()),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_TIMESERIES)),
            object: TermPattern::Variable(datapoint_variable.clone()),
        };

        let value_variable =
            Variable::new_unchecked(format!("{}_value", datapoint_variable.as_str()));
        let has_value_triple = TriplePattern {
            subject: TermPattern::Variable(datapoint_variable.clone()),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_VALUE)),
            object: TermPattern::Variable(value_variable.clone()),
        };
        let timestamp_variable = Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME);
        let has_timestamp_triple = TriplePattern {
            subject: TermPattern::Variable(timeseries_variable.clone()),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_TIMESTAMP)),
            object: TermPattern::Variable(timestamp_variable),
        };
        if let Some(i) = optional_index {
            let opt_triples = self.optional_triples.get_mut(i).unwrap();
            opt_triples.push(has_timeseries_triple);
            opt_triples.push(has_datapoint_triple);
            opt_triples.push(has_value_triple);
            opt_triples.push(has_timestamp_triple);
        } else {
            self.triples.push(has_timeseries_triple);
            self.triples.push(has_datapoint_triple);
            self.triples.push(has_value_triple);
            self.triples.push(has_timestamp_triple);
        }
        self.variable_has_value
            .insert(end_variable.clone(), value_variable.clone());
        value_variable
    }
    fn translate_connective_named_node(&self, connective: &Connective) -> NamedNode {
        let connective_string = connective.to_string();
        let iri = self
            .connective_mapping
            .map
            .get(&connective_string)
            .expect(&format!("Connective {} not defined", &connective_string));
        NamedNode::new(iri).expect("Invalid iri")
    }
}

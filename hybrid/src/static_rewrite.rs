use crate::const_uris::HAS_EXTERNAL_ID;
use crate::constraints::Constraint;
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{GroundTerm, NamedNode, NamedNodePattern, TermPattern, TriplePattern, Variable};
use spargebra::Query;
use std::collections::{HashMap, HashSet};

pub fn rewrite_static_query(
    query: Query,
    has_constraint: &HashMap<TermPattern, Constraint>,
) -> Option<Query> {
    if let Query::Select {
        dataset,
        pattern,
        base_iri,
    } = &query
    {
        let mut external_ids_in_scope = HashMap::new();
        let pattern_rewrite_opt =
            rewrite_static_graph_pattern(pattern, has_constraint, &mut external_ids_in_scope);
        if let Some(pattern_rewrite) = pattern_rewrite_opt {
            Some(Query::Select {
                dataset: dataset.clone(),
                pattern: pattern_rewrite,
                base_iri: base_iri.clone(),
            })
        } else {
            None
        }
    } else {
        panic!("Only support for Select");
    }
}

pub fn rewrite_static_graph_pattern(
    graph_pattern: &GraphPattern,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    match graph_pattern {
        GraphPattern::Bgp { patterns } => {
            rewrite_static_bgp(patterns, has_constraint, external_ids_in_scope)
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => rewrite_static_path(subject, path, object, has_constraint, external_ids_in_scope),
        GraphPattern::Join { left, right } => {
            rewrite_static_join(left, right, has_constraint, external_ids_in_scope)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => rewrite_static_left_join(
            left,
            right,
            expression,
            has_constraint,
            external_ids_in_scope,
        ),
        GraphPattern::Filter { expr, inner } => {
            rewrite_static_filter(expr, inner, has_constraint, external_ids_in_scope)
        }
        GraphPattern::Union { left, right } => {
            rewrite_static_union(left, right, has_constraint, external_ids_in_scope)
        }
        GraphPattern::Graph { name, inner } => {
            rewrite_static_graph(name, inner, has_constraint, external_ids_in_scope)
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            todo!()
        }
        GraphPattern::Minus { left, right } => {
            rewrite_static_minus(left, right, has_constraint, external_ids_in_scope)
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => rewrite_static_values(variables, bindings, has_constraint),
        GraphPattern::OrderBy { inner, expression } => {
            rewrite_static_order_by(inner, expression, has_constraint, external_ids_in_scope)
        }
        GraphPattern::Project { inner, variables } => {
            rewrite_static_project(inner, variables, has_constraint, external_ids_in_scope)
        }
        GraphPattern::Distinct { inner } => {
            rewrite_static_distinct(inner, has_constraint, external_ids_in_scope)
        }
        GraphPattern::Reduced { inner } => {
            todo!()
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            todo!()
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => rewrite_static_group(
            inner,
            variables,
            aggregates,
            has_constraint,
            external_ids_in_scope,
        ),
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => {
            todo!()
        }
    }
}

fn rewrite_static_values(
    variables: &Vec<Variable>,
    ground_term_vecs: &Vec<Vec<Option<GroundTerm>>>,
    has_constraint: &HashMap<TermPattern, Constraint>,
) -> Option<GraphPattern> {
    todo!()
}

fn rewrite_static_graph(
    name: &NamedNodePattern,
    inner: &Box<GraphPattern>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    todo!()
}

fn rewrite_static_union(
    left: &Box<GraphPattern>,
    right: &Box<GraphPattern>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    todo!()
}

fn rewrite_static_join(
    left: &Box<GraphPattern>,
    right: &Box<GraphPattern>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let mut left_external_ids_in_scope = external_ids_in_scope.clone();
    let left_rewrite_opt =
        rewrite_static_graph_pattern(left, has_constraint, &mut left_external_ids_in_scope);
    let mut right_external_ids_in_scope = external_ids_in_scope.clone();
    let right_rewrite_opt =
        rewrite_static_graph_pattern(right, has_constraint, &mut right_external_ids_in_scope);
    for (k, v) in left_external_ids_in_scope.into_iter() {
        external_ids_in_scope.insert(k, v);
    }
    for (k, v) in right_external_ids_in_scope.into_iter() {
        external_ids_in_scope.insert(k, v);
    }

    if let Some(left_rewrite) = left_rewrite_opt {
        if let Some(right_rewrite) = right_rewrite_opt {
            Some(GraphPattern::Join {
                left: Box::from(left_rewrite),
                right: Box::from(right_rewrite),
            })
        } else {
            Some(left_rewrite)
        }
    } else if let Some(right_rewrite) = right_rewrite_opt {
        Some(right_rewrite)
    } else {
        None
    }
}

fn rewrite_static_left_join(
    left: &Box<GraphPattern>,
    right: &Box<GraphPattern>,
    expression_opt: &Option<Expression>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let mut left_external_ids_in_scope = external_ids_in_scope.clone();
    let left_rewrite_opt =
        rewrite_static_graph_pattern(left, has_constraint, &mut left_external_ids_in_scope);
    let mut right_external_ids_in_scope = external_ids_in_scope.clone();
    let right_rewrite_opt =
        rewrite_static_graph_pattern(right, has_constraint, &mut right_external_ids_in_scope);
    for (k, v) in left_external_ids_in_scope.into_iter() {
        external_ids_in_scope.insert(k, v);
    }
    for (k, v) in right_external_ids_in_scope.into_iter() {
        external_ids_in_scope.insert(k, v);
    }

    let mut expression_rewrite_opt = None;
    if let Some(expression) = expression_opt {
        expression_rewrite_opt =
            rewrite_static_expression(expression, has_constraint, external_ids_in_scope);
    }
    if let Some(left_rewrite) = left_rewrite_opt {
        if let Some(right_rewrite) = right_rewrite_opt {
            Some(GraphPattern::LeftJoin {
                left: Box::new(left_rewrite),
                right: Box::new(right_rewrite),
                expression: expression_rewrite_opt,
            })
        } else if let Some(expression_rewrite) = expression_rewrite_opt {
            Some(GraphPattern::Filter {
                expr: expression_rewrite,
                inner: Box::new(left_rewrite),
            })
        } else {
            None
        }
    } else {
        None
    }
}

fn rewrite_static_filter(
    expression: &Expression,
    inner: &Box<GraphPattern>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let inner_rewrite_opt =
        rewrite_static_graph_pattern(inner, has_constraint, external_ids_in_scope);
    if let Some(inner_rewrite) = inner_rewrite_opt {
        let expression_rewrite_opt =
            rewrite_static_expression(expression, has_constraint, external_ids_in_scope);
        if let Some(expression_rewrite) = expression_rewrite_opt {
            Some(GraphPattern::Filter {
                expr: expression_rewrite,
                inner: Box::new(inner_rewrite),
            })
        } else {
            Some(inner_rewrite)
        }
    } else {
        None
    }
}

fn rewrite_static_group(
    graph_pattern: &GraphPattern,
    variables: &Vec<Variable>,
    aggregates: &Vec<(Variable, AggregateExpression)>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let graph_pattern_rewrite_opt =
        rewrite_static_graph_pattern(graph_pattern, has_constraint, external_ids_in_scope);
    if let Some(graph_pattern_rewrite) = graph_pattern_rewrite_opt {
        let aggregates_rewrite = aggregates.iter().map(|(v, a)| {
            (
                rewrite_static_variable(v, has_constraint),
                rewrite_static_aggregate_expression(a, has_constraint, external_ids_in_scope),
            )
        });
        let aggregates_rewrite = aggregates_rewrite
            .into_iter()
            .filter(|(x, y)| x.is_some() && y.is_some())
            .map(|(x, y)| (x.unwrap(), y.unwrap()))
            .collect::<Vec<(Variable, AggregateExpression)>>();
        let variables_rewritten = variables
            .iter()
            .map(|v| rewrite_static_variable(v, has_constraint))
            .filter(|x| x.is_some());
        if aggregates_rewrite.len() > 0 {
            Some(GraphPattern::Group {
                inner: Box::new(graph_pattern_rewrite),
                variables: variables_rewritten.map(|x| x.unwrap()).collect(),
                aggregates: vec![],
            })
        } else {
            None
        }
    } else {
        None
    }
}

fn rewrite_static_aggregate_expression(
    aggregate_expression: &AggregateExpression,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &HashMap<Variable, Variable>,
) -> Option<AggregateExpression> {
    match aggregate_expression {
        AggregateExpression::Count { expr, distinct } => {
            if let Some(boxed_expression) = expr {
                if let Some(expr_rewritten) = rewrite_static_expression(
                    boxed_expression,
                    has_constraint,
                    external_ids_in_scope,
                ) {
                    Some(AggregateExpression::Count {
                        expr: Some(Box::new(expr_rewritten)),
                        distinct: *distinct,
                    })
                } else {
                    Some(AggregateExpression::Count {
                        expr: None,
                        distinct: *distinct,
                    })
                }
            } else {
                Some(AggregateExpression::Count {
                    expr: None,
                    distinct: *distinct,
                })
            }
        }
        AggregateExpression::Sum { expr, distinct } => {
            if let Some(rewritten_expression) =
                rewrite_static_expression(expr, has_constraint, external_ids_in_scope)
            {
                Some(AggregateExpression::Sum {
                    expr: Box::new(rewritten_expression),
                    distinct: *distinct,
                })
            } else {
                None
            }
        }
        AggregateExpression::Avg { expr, distinct } => {
            if let Some(rewritten_expression) =
                rewrite_static_expression(expr, has_constraint, external_ids_in_scope)
            {
                Some(AggregateExpression::Avg {
                    expr: Box::new(rewritten_expression),
                    distinct: *distinct,
                })
            } else {
                None
            }
        }
        AggregateExpression::Min { expr, distinct } => {
            if let Some(rewritten_expression) =
                rewrite_static_expression(expr, has_constraint, external_ids_in_scope)
            {
                Some(AggregateExpression::Min {
                    expr: Box::new(rewritten_expression),
                    distinct: *distinct,
                })
            } else {
                None
            }
        }
        AggregateExpression::Max { expr, distinct } => {
            if let Some(rewritten_expression) =
                rewrite_static_expression(expr, has_constraint, external_ids_in_scope)
            {
                Some(AggregateExpression::Max {
                    expr: Box::new(rewritten_expression),
                    distinct: *distinct,
                })
            } else {
                None
            }
        }
        AggregateExpression::GroupConcat {
            expr,
            distinct,
            separator,
        } => {
            if let Some(rewritten_expression) =
                rewrite_static_expression(expr, has_constraint, external_ids_in_scope)
            {
                Some(AggregateExpression::GroupConcat {
                    expr: Box::new(rewritten_expression),
                    distinct: *distinct,
                    separator: separator.clone(),
                })
            } else {
                None
            }
        }
        AggregateExpression::Sample { expr, distinct } => {
            if let Some(rewritten_expression) =
                rewrite_static_expression(expr, has_constraint, external_ids_in_scope)
            {
                Some(AggregateExpression::Sample {
                    expr: Box::new(rewritten_expression),
                    distinct: *distinct,
                })
            } else {
                None
            }
        }
        AggregateExpression::Custom {
            name,
            expr,
            distinct,
        } => {
            if let Some(rewritten_expression) =
                rewrite_static_expression(expr, has_constraint, external_ids_in_scope)
            {
                Some(AggregateExpression::Custom {
                    name: name.clone(),
                    expr: Box::new(rewritten_expression),
                    distinct: *distinct,
                })
            } else {
                None
            }
        }
    }
}

fn rewrite_static_distinct(
    inner: &Box<GraphPattern>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    if let Some(inner_rewrite) =
        rewrite_static_graph_pattern(inner, has_constraint, external_ids_in_scope)
    {
        Some(GraphPattern::Distinct {
            inner: Box::new(inner_rewrite),
        })
    } else {
        None
    }
}

fn rewrite_static_project(
    inner: &Box<GraphPattern>,
    variables: &Vec<Variable>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    if let Some(inner_rewrite) =
        rewrite_static_graph_pattern(inner, has_constraint, external_ids_in_scope)
    {
        let variables_rewrite = variables
            .iter()
            .map(|v| rewrite_static_variable(v, has_constraint))
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect::<Vec<Variable>>();
        if variables_rewrite.len() > 0 {
            Some(GraphPattern::Project {
                inner: Box::new(inner_rewrite),
                variables: variables_rewrite,
            })
        } else {
            None
        }
    } else {
        None
    }
}

fn rewrite_static_order_by(
    inner: &Box<GraphPattern>,
    order_expressions: &Vec<OrderExpression>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    if let Some(inner_rewrite) =
        rewrite_static_graph_pattern(inner, has_constraint, external_ids_in_scope)
    {
        let expressions_rewrite = order_expressions
            .iter()
            .map(|e| rewrite_static_order_expression(e, has_constraint, &external_ids_in_scope))
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect::<Vec<OrderExpression>>();
        if expressions_rewrite.len() > 0 {
            Some(GraphPattern::OrderBy {
                inner: Box::new(inner_rewrite),
                expression: expressions_rewrite,
            })
        } else {
            None
        }
    } else {
        None
    }
}

fn rewrite_static_order_expression(
    order_expression: &OrderExpression,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &HashMap<Variable, Variable>,
) -> Option<OrderExpression> {
    match order_expression {
        OrderExpression::Asc(e) => {
            if let Some(e_rewrite) =
                rewrite_static_expression(e, has_constraint, external_ids_in_scope)
            {
                Some(OrderExpression::Asc(e_rewrite))
            } else {
                None
            }
        }
        OrderExpression::Desc(e) => {
            if let Some(e_rewrite) =
                rewrite_static_expression(e, has_constraint, external_ids_in_scope)
            {
                Some(OrderExpression::Desc(e_rewrite))
            } else {
                None
            }
        }
    }
}

fn rewrite_static_minus(
    left: &Box<GraphPattern>,
    right: &Box<GraphPattern>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let mut left_external_ids_in_scope = external_ids_in_scope.clone();
    let left_rewrite_opt =
        rewrite_static_graph_pattern(left, has_constraint, &mut left_external_ids_in_scope);
    let mut right_external_ids_in_scope = external_ids_in_scope.clone();
    let right_rewrite_opt =
        rewrite_static_graph_pattern(right, has_constraint, &mut right_external_ids_in_scope);
    //Only append left side since minus does not introduce these..
    for (k, v) in left_external_ids_in_scope.into_iter() {
        external_ids_in_scope.insert(k, v);
    }

    if let Some(left_rewrite) = left_rewrite_opt {
        if let Some(right_rewrite) = right_rewrite_opt {
            Some(GraphPattern::Minus {
                left: Box::new(left_rewrite),
                right: Box::new(right_rewrite),
            })
        } else {
            Some(left_rewrite)
        }
    } else {
        None
    }
}

pub fn rewrite_static_bgp(
    patterns: &Vec<TriplePattern>,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let mut new_triples = vec![];
    for t in patterns {
        let obj_constr_opt = has_constraint.get(&t.object);
        if let TermPattern::Variable(object_var) = &t.object {
            if let Some(obj_constr) = obj_constr_opt {
                if obj_constr == &Constraint::ExternalTimeseries {
                    let obj_variable = match &t.object {
                        TermPattern::Variable(var) => var,
                        anything_else => {
                            panic!("No support for term pattern {}", anything_else)
                        }
                    };
                    if !external_ids_in_scope.contains_key(&obj_variable) {
                        let external_id_var =
                            Variable::new(obj_variable.as_str().to_string() + "_external_id").unwrap();
                        let new_triple = TriplePattern {
                            subject: TermPattern::Variable(obj_variable.clone()),
                            predicate: NamedNodePattern::NamedNode(NamedNode::new(HAS_EXTERNAL_ID).unwrap()),
                            object: TermPattern::Variable(external_id_var.clone()),
                        };
                        if !new_triples.contains(&new_triple) {
                            new_triples.push(new_triple);
                        }
                        external_ids_in_scope.insert(obj_variable.clone(), external_id_var.clone());
                    }
                }
            }
        }
        if let TermPattern::Variable(subject_var) = &t.subject {
            let subj_constr_opt = has_constraint.get(&t.subject);

            if subj_constr_opt != Some(&Constraint::ExternalDataPoint)
                && subj_constr_opt != Some(&Constraint::ExternalDataValue)
                && subj_constr_opt != Some(&Constraint::ExternalTimestamp)
                && obj_constr_opt != Some(&Constraint::ExternalDataPoint)
                && obj_constr_opt != Some(&Constraint::ExternalDataValue)
                && obj_constr_opt != Some(&Constraint::ExternalTimestamp)
            {
                if !new_triples.contains(t) {
                            new_triples.push(t.clone());
                }
            }
        }
    }

    if new_triples.is_empty() {
        None
    } else {
        Some(GraphPattern::Bgp {
            patterns: new_triples,
        })
    }
}

pub fn rewrite_static_path(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &mut HashMap<Variable, Variable>,
) -> Option<GraphPattern> {
    todo!()
    //Possibly rewrite into reduced path without something and bgp...
}

pub fn rewrite_static_expression(
    expression: &Expression,
    has_constraint: &HashMap<TermPattern, Constraint>,
    external_ids_in_scope: &HashMap<Variable, Variable>,
) -> Option<Expression> {
    match expression {
        Expression::NamedNode(nn) => Some(Expression::NamedNode(nn.clone())),
        Expression::Literal(l) => Some(Expression::Literal(l.clone())),
        Expression::Variable(v) => {
            if let Some(rewritten_variable) = rewrite_static_variable(v, has_constraint) {
                Some(Expression::Variable(rewritten_variable))
            } else {
                None
            }
        }
        Expression::Or(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Or(Box::new(left_trans), Box::new(right_trans)))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::And(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::And(Box::new(left_trans), Box::new(right_trans)))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::Equal(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Equal(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::SameTerm(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::SameTerm(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::Greater(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Greater(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::GreaterOrEqual(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::GreaterOrEqual(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::Less(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Less(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::LessOrEqual(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::LessOrEqual(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::In(left, expressions) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let expressions_rewritten = expressions
                .iter()
                .map(|e| rewrite_static_expression(e, has_constraint, external_ids_in_scope))
                .collect::<Vec<Option<Expression>>>();
            if expressions_rewritten.iter().any(|x| x.is_none()) {
                return None;
            }
            if let Some(left_trans) = left_trans_opt {
                Some(Expression::In(
                    Box::new(left_trans),
                    expressions_rewritten.into_iter().map(|x| x.unwrap()).collect(),
                ))
            } else {
                None
            }
        }
        Expression::Add(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);

            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Add(Box::new(left_trans), Box::new(right_trans)))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::Subtract(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Subtract(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::Multiply(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Multiply(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::Divide(left, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Divide(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::UnaryPlus(wrapped) => {
            let wrapped_trans_opt =
                rewrite_static_expression(wrapped, has_constraint, external_ids_in_scope);
            if let Some(wrapped_trans) = wrapped_trans_opt {
                Some(Expression::UnaryPlus(Box::new(wrapped_trans)))
            } else {
                None
            }
        }
        Expression::UnaryMinus(wrapped) => {
            let wrapped_trans_opt =
                rewrite_static_expression(wrapped, has_constraint, external_ids_in_scope);
            if let Some(wrapped_trans) = wrapped_trans_opt {
                Some(Expression::UnaryPlus(Box::new(wrapped_trans)))
            } else {
                None
            }
        }
        Expression::Not(wrapped) => {
            let wrapped_trans_opt =
                rewrite_static_expression(wrapped, has_constraint, external_ids_in_scope);
            if let Some(wrapped_trans) = wrapped_trans_opt {
                Some(Expression::UnaryPlus(Box::new(wrapped_trans)))
            } else {
                None
            }
        }
        Expression::Exists(wrapped) => {
            let wrapped_trans_opt = rewrite_static_graph_pattern(
                &wrapped,
                has_constraint,
                &mut external_ids_in_scope.clone(),
            );
            if let Some(wrapped_trans) = wrapped_trans_opt {
                Some(Expression::Exists(Box::new(wrapped_trans)))
            } else {
                None
            }
        }
        Expression::Bound(v) => {
            if let Some(v_rewritten) = rewrite_static_variable(v, has_constraint) {
                Some(Expression::Bound(v_rewritten))
            } else {
                None
            }
        }
        Expression::If(left, mid, right) => {
            let left_trans_opt =
                rewrite_static_expression(left, has_constraint, external_ids_in_scope);
            let mid_trans_opt =
                rewrite_static_expression(mid, has_constraint, external_ids_in_scope);
            let right_trans_opt =
                rewrite_static_expression(right, has_constraint, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    if let Some(mid_trans) = mid_trans_opt {
                        Some(Expression::If(
                            Box::new(left_trans),
                            Box::new(mid_trans),
                            Box::new(right_trans),
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        Expression::Coalesce(wrapped) => {
            let rewritten = wrapped
                .iter()
                .map(|e| rewrite_static_expression(e, has_constraint, external_ids_in_scope)).collect::<Vec<Option<Expression>>>();
            if rewritten.iter().all(|x| x.is_some()) {
                Some(Expression::Coalesce(
                    rewritten.into_iter().map(|x| x.unwrap()).collect(),
                ))
            } else {
                None
            }
        }
        Expression::FunctionCall(fun, args) => {
            let args_rewritten = args
                .iter()
                .map(|e| rewrite_static_expression(e, has_constraint, external_ids_in_scope)).collect::<Vec<Option<Expression>>>();
            if args_rewritten.iter().all(|x| x.is_some()) {
                Some(Expression::FunctionCall(
                    fun.clone(),
                    args_rewritten.into_iter().map(|x| x.unwrap()).collect(),
                ))
            } else {
                None
            }
        }
    }
}

fn rewrite_static_variable(
    v: &Variable,
    has_constraint: &HashMap<TermPattern, Constraint>,
) -> Option<Variable> {
    if let Some(ctr) = has_constraint.get(&TermPattern::Variable(v.clone())) {
        if !(ctr == &Constraint::ExternalDataPoint
            || ctr == &Constraint::ExternalDataValue
            || ctr == &Constraint::ExternalTimestamp)
        {
            Some(v.clone())
        } else {
            None
        }
    } else {
        Some(v.clone())
    }
}

use crate::const_uris::HAS_EXTERNAL_ID;
use crate::type_inference::Constraint;
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::NamedNodePattern::NamedNode;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};
use spargebra::Query;
use std::collections::{BTreeMap, BTreeSet};

pub fn rewrite_static_query(query: Query, tree: &BTreeMap<Variable, Constraint>) -> Option<Query> {
    if let Query::Select {
        dataset,
        pattern,
        base_iri,
    } = &query
    {
        let mut external_ids_in_scope = BTreeMap::new();
        let new_pattern = rewrite_static_graph_pattern(pattern, tree, &mut external_ids_in_scope);
        todo!()
        // let mut static_query = Query::Select {
        //     dataset: None,
        //     pattern: new_pattern,
        //     base_iri: None,
        // };
        // static_query
    } else {
        panic!("Only support for Select");
    }
}

pub fn rewrite_static_graph_pattern(
    graph_pattern: &GraphPattern,
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &mut BTreeMap<Variable, Variable>,
) -> Option<GraphPattern> {
    match graph_pattern {
        GraphPattern::Bgp { patterns } => rewrite_static_bgp(patterns, tree, external_ids_in_scope),
        GraphPattern::Path {
            subject,
            path,
            object,
        } => rewrite_static_path(subject, path, object, tree, external_ids_in_scope),
        GraphPattern::Join { left, right } => {
            rewrite_static_join(left, right, tree, external_ids_in_scope)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => rewrite_static_left_join(left, right, expression, external_ids_in_scope),
        GraphPattern::Filter { expr, inner } => rewrite_static_filter(expr, inner, tree, external_ids_in_scope),
        GraphPattern::Union { left, right } => rewrite_static_union(left, right, tree, external_ids_in_scope),
        GraphPattern::Graph { name, inner } => rewrite_static_graph(name, inner, tree, external_ids_in_scope),
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            todo!()
        }
        GraphPattern::Minus { left, right } => {
            rewrite_static_minus(left, right, tree, external_ids_in_scope)
        }
        GraphPattern::Values {
            variables,
            bindings,
        } => rewrite_static_values(variables, bindings, tree),
        GraphPattern::OrderBy { inner, expression } => {
            rewrite_static_order_by(inner, expression, tree)
        }
        GraphPattern::Project { inner, variables } => {
            rewrite_static_project(inner, variables, tree)
        }
        GraphPattern::Distinct { inner } => rewrite_static_distinct(inner, tree),
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
        } => rewrite_static_group(inner, variables, aggregates, tree, external_ids_in_scope),
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => {
            todo!()
        }
    }
}

fn rewrite_static_graph(
    name: &NamedNodePattern,
    inner: &Box<GraphPattern>,
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &mut BTreeMap<Variable, Variable>,
) -> GraphPattern {
    todo!()
    }

fn rewrite_static_union(
    left: &Box<GraphPattern>,
    right: &Box<GraphPattern>,
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &mut BTreeMap<Variable, Variable>,
) -> GraphPattern {
    todo!()
}

fn rewrite_static_join(
    left: &Box<GraphPattern>,
    right: &Box<GraphPattern>,
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &mut BTreeMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let mut left_external_ids_in_scope = external_ids_in_scope.clone();
    let left_rewrite_opt =
        rewrite_static_graph_pattern(left, tree, &mut left_external_ids_in_scope);
    let mut right_external_ids_in_scope = external_ids_in_scope.clone();
    let right_rewrite_opt =
        rewrite_static_graph_pattern(right, tree, &mut right_external_ids_in_scope);
    external_ids_in_scope.append(&mut left_external_ids_in_scope);
    external_ids_in_scope.append(&mut right_external_ids_in_scope);

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
    external_ids_in_scope: &mut BTreeMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let mut left_external_ids_in_scope = external_ids_in_scope.clone();
    let left_rewrite_opt =
        rewrite_static_graph_pattern(left, tree, &mut left_external_ids_in_scope);
    let mut right_external_ids_in_scope = external_ids_in_scope.clone();
    let right_rewrite_opt =
        rewrite_static_graph_pattern(right, tree, &mut right_external_ids_in_scope);
    external_ids_in_scope.append(&mut left_external_ids_in_scope);
    external_ids_in_scope.append(&mut right_external_ids_in_scope);

    let mut expression_rewrite_opt = None;
    if let Some(expression) = expression_opt {
        expression_rewrite_opt = rewrite_static_expression(expression, tree, external_ids_in_scope);
    }

    if let Some(left_rewrite) = left_rewrite_opt {
        if let Some(right_rewrite) = right_rewrite_opt {
            Some(GraphPattern::LeftJoin {
                left: Box::new(left_rewrite),
                right: Box::new(right_rewrite),
                expression: expression_rewrite,
            })
        } else if let Some(expression_rewrite) = expression_rewrite_opt {
            Some(GraphPattern::Filter {
                expr: expression_rewrite,
                inner: Box::new(left_rewrite),
            })
        }
    } else {
        None
    }
}

fn rewrite_static_filter(
    expression: &Expression,
    inner: &Box<GraphPattern>,
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &mut BTreeMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let inner_rewrite_opt = rewrite_static_graph_pattern(inner, tree, external_ids_in_scope);
    if let Some(inner_rewrite) = inner_rewrite_opt {
        let expression_rewrite_opt = rewrite_static_expression(expression, tree, external_ids_in_scope);
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
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &mut BTreeMap<Variable, Variable>,
) -> Some(GraphPattern) {
    let graph_pattern_rewrite_opt =
        rewrite_static_graph_pattern(graph_pattern, tree, external_ids_in_scope);
    if let Some(graph_pattern_rewrite) = graph_pattern_rewrite_opt {
        let mut aggregates_rewrite = aggregates.iter().map(|(v, a)| {
            (
                rewrite_static_variable(v, tree),
                rewrite_static_aggregate_expression(a, tree, external_ids_in_scope),
            )
        });
        let aggregates_rewrite = aggregates_rewrite.into_iter().filter(|(x,y)|x.is_some() && y.is_some());
        let variables_rewritten = variables.iter().map(|v|rewrite_static_variable(v, tree)).filter(|x|x.is_some());
        if aggregates_rewrite.len() > 0 {
            Some(GraphPattern::Group {
                inner: Box::new(graph_pattern_rewrite),
                variables: variables_rewritten.map(|x|x.unwrap()).collect(),
                aggregates: vec![]
            })
        }
        None
    }
}

fn rewrite_static_aggregate_expression(aggregate_expression: &AggregateExpression, tree: &BTreeMap<Variable, Constraint>, external_ids_in_scope: &BTreeMap<Variable, Variable>) -> Option<AggregateExpression> {
    match aggregate_expression {
        AggregateExpression::Count { expr, distinct } => {
            if let Some(boxed_expression) = expr {
                if let Some(expr_rewritten) = rewrite_static_expression(boxed_expression, tree, external_ids_in_scope) {
                    Some(AggregateExpression::Count { expr: Some(Box::new(expr_rewritten)), distinct: *distinct })
                } else {
                    Some(AggregateExpression::Count { expr: None, distinct: *distinct})
                }
            } else {
                Some(AggregateExpression::Count {expr:None, distinct:*distinct})
            }
        }
        AggregateExpression::Sum { expr, distinct } => {
            if let Some(rewritten_expression) = rewrite_static_expression(expr, tree, external_ids_in_scope) {
                Some(AggregateExpression::Sum {expr:Box::new(rewritten_expression), distinct:*distinct})
            } else {
                None
            }
        }
        AggregateExpression::Avg { expr, distinct } => {
            if let Some(rewritten_expression) = rewrite_static_expression(expr, tree, external_ids_in_scope) {
                Some(AggregateExpression::Avg {expr:Box::new(rewritten_expression), distinct:*distinct})
            } else {
                None
            }
        }
        AggregateExpression::Min { expr, distinct } => {
            if let Some(rewritten_expression) = rewrite_static_expression(expr, tree, external_ids_in_scope) {
                Some(AggregateExpression::Min {expr:Box::new(rewritten_expression), distinct:*distinct})
            } else {
                None
            }
        }
        AggregateExpression::Max { expr, distinct } => {
            if let Some(rewritten_expression) = rewrite_static_expression(expr, tree, external_ids_in_scope) {
                Some(AggregateExpression::Max {expr:Box::new(rewritten_expression), distinct:*distinct})
            } else {
                None
            }
        }
        AggregateExpression::GroupConcat { expr, distinct, separator } => {
            if let Some(rewritten_expression) = rewrite_static_expression(expr, tree, external_ids_in_scope) {
                Some(AggregateExpression::GroupConcat {expr:Box::new(rewritten_expression), distinct:*distinct, separator:separator.clone()})
            } else {
                None
            }        }
        AggregateExpression::Sample { expr, distinct } => {
            if let Some(rewritten_expression) = rewrite_static_expression(expr, tree, external_ids_in_scope) {
                Some(AggregateExpression::Sample {expr:Box::new(rewritten_expression), distinct:*distinct})
            } else {
                None
            }
        }
        AggregateExpression::Custom { name, expr, distinct } => {
            if let Some(rewritten_expression) = rewrite_static_expression(expr, tree, external_ids_in_scope) {
                Some(AggregateExpression::Custom {name: name.clone(), expr:Box::new(rewritten_expression), distinct:*distinct})
            } else {
                None
            }
        }
    }
}

fn rewrite_static_distinct(
    inner: &Box<GraphPattern>,
    tree: &BTreeMap<Variable, Constraint>,
) -> GraphPattern {
    todo!()
}

fn rewrite_static_project(
    inner: &Box<GraphPattern>,
    variables: &Vec<Variable>,
    tree: &BTreeMap<Variable, Constraint>,
) -> GraphPattern {
    todo!()
}

fn rewrite_static_order_by(
    inner: &Box<GraphPattern>,
    expression: &Vec<OrderExpression>,
    tree: &BTreeMap<Variable, Constraint>,
) -> GraphPattern {
    todo!()
}

fn rewrite_static_minus(
    left: &Box<GraphPattern>,
    right: &Box<GraphPattern>,
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &BTreeMap<Variable, Variable>,
) -> GraphPattern {
    //external_ids_in_scope are not mutated by minus, since this graph pattern does not introduce variables to scope.
    todo!()
}

pub fn rewrite_static_bgp(
    patterns: &Vec<TriplePattern>,
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &mut BTreeMap<Variable, Variable>,
) -> Option<GraphPattern> {
    let mut new_triples = BTreeSet::new();
    for t in patterns {
            if let TermPattern::Variable(object_var) = &t.object {
                let obj_constr_opt = tree.get(object_var);
                if let Some(obj_constr) = obj_constr_opt {
                    if obj_constr == Constraint::ExternalTimeseries {
                        let obj_variable = match &t.object {
                            TermPattern::Variable(var) => var,
                            anything_else => {
                                panic!("No support for term pattern {}", anything_else)
                            }
                        };
                        if !external_ids_in_scope.contains_key(&obj_variable) {
                            let external_id_var =
                                Variable::new(obj_variable.to_string() + "_external_id").unwrap();
                            let new_triple = TriplePattern {
                                subject: TermPattern::Variable(obj_variable.clone()),
                                predicate: NamedNodePattern::NamedNode(HAS_EXTERNAL_ID.clone()),
                                object: TermPattern::Variable(external_id_var),
                            };
                            new_triples.insert(new_triple);
                            external_ids_in_scope.insert(obj_variable.clone(), external_id_var.clone())
                        }
                    }
                }
            }
        if let TermPattern::Variable(subject_var) = &t.subject {
            let subj_constr_opt = tree.get(subject_var);

                if subj_constr_opt != Some(Constraint::ExternalDataPoint)
                    && subj_constr_opt != Some(Constraint::ExternalDataValue)
                    && subj_constr_opt != Some(Constraint::ExternalTimestamp)
                    && obj_constr_opt != Some(Constraint::ExternalDataPoint)
                    && obj_constr_opt != Some(Constraint::ExternalDataValue)
                    && obj_constr_opt != Some(Constraint::ExternalTimestamp)
                {
                    new_triples.insert(t.clone());
                }
            }
        }

    if new_triples.is_empty() {
        None
    } else {
        Some(GraphPattern::Bgp {
            patterns: new_triples.into_iter().collect(),
        })
    }
}

pub fn rewrite_static_path(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &mut BTreeMap<Variable, Variable>,
) -> GraphPattern {
    todo!()
    //Possibly rewrite into reduced path without something and bgp...
}

pub fn rewrite_static_expression(
    expression: &Expression,
    tree: &BTreeMap<Variable, Constraint>,
    external_ids_in_scope: &BTreeMap<Variable, Variable>,
) -> Option<Expression> {
    match expression {
        Expression::NamedNode(nn) => Some(Expression::NamedNode(nn.clone())),
        Expression::Literal(l) => Some(Expression::Literal(l.clone())),
        Expression::Variable(v) => {
            if let Some(rewritten_variable) = rewrite_static_variable(v, tree) {
                Expression::Variable(rewritten_variable)
            } else {
                None
            }
        }
        Expression::Or(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Or(Box::new(left_trans), Box::new(right_trans)))
                }
            } else {
                None
            }
        }
        Expression::And(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Expression::And(Box::new(left_trans), Box::new(right_trans))
                }
            } else {
                None
            }
        }
        Expression::Equal(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Expression::Equal(Box::new(left_trans), Box::new(right_trans))
                }
            } else {
                None
            }
        }
        Expression::SameTerm(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Expression::SameTerm(Box::new(left_trans), Box::new(right_trans))
                }
            } else {
                None
            }
        }
        Expression::Greater(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Expression::Greater(Box::new(left_trans), Box::new(right_trans))
                }
            } else {
                None
            }
        }
        Expression::GreaterOrEqual(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Expression::And(Box::new(left_trans), Box::new(right_trans))
                }
            } else {
                None
            }
        }
        Expression::Less(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Expression::Less(Box::new(left_trans), Box::new(right_trans))
                }
            } else {
                None
            }
        }
        Expression::LessOrEqual(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Expression::LessOrEqual(Box::new(left_trans), Box::new(right_trans))
                }
            } else {
                None
            }
        }
        Expression::In(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let rights_trans = right
                .iter()
                .map(|e| rewrite_static_expression(e, tree, external_ids_in_scope))
                .collect::<Vec<Expression>>();
            if let Some(left_trans) = left_trans_opt {
                if rights_trans.iter().map(|x| x.is_some()) {
                    Expression::In(Box::new(left_trans), rights_trans)
                }
            } else {
                None
            }
        }
        Expression::Add(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let rights_trans = right
                .iter()
                .map(|e| rewrite_static_expression(e, tree, external_ids_in_scope))
                .collect::<Vec<Expression>>();
            if let Some(left_trans) = left_trans_opt {
                if rights_trans.iter().map(|x| x.is_some()) {
                    Expression::In(Box::new(left_trans), rights_trans)
                }
            } else {
                None
            }
        }
        Expression::Subtract(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Subtract(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                }
            } else {
                None
            }
        }
        Expression::Multiply(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Multiply(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                }
            } else {
                None
            }
        }
        Expression::Divide(left, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    Some(Expression::Divide(
                        Box::new(left_trans),
                        Box::new(right_trans),
                    ))
                }
            } else {
                None
            }
        }
        Expression::UnaryPlus(wrapped) => {
            let wrapped_trans_opt = rewrite_static_expression(wrapped, tree, external_ids_in_scope);
            if let Some(wrapped_trans) = wrapped_trans_opt {
                Expression::UnaryPlus(Box::new(wrapped_trans))
            }
        }
        Expression::UnaryMinus(wrapped) => {
            let wrapped_trans_opt = rewrite_static_expression(wrapped, tree, external_ids_in_scope);
            if let Some(wrapped_trans) = wrapped_trans_opt {
                Expression::UnaryPlus(Box::new(wrapped_trans))
            }
        }
        Expression::Not(wrapped) => {
            let wrapped_trans_opt = rewrite_static_expression(wrapped, tree, external_ids_in_scope);
            if let Some(wrapped_trans) = wrapped_trans_opt {
                Expression::UnaryPlus(Box::new(wrapped_trans))
            }
        }
        Expression::Exists(wrapped) => {
            let wrapped_trans_opt = rewrite_static_graph_pattern(&wrapped, tree, &mut external_ids_in_scope.clone());
            if let Some(wrapped_trans) = wrapped_trans_opt {
                Expression::Exists(Box::new(wrapped_trans))
            }
        }
        Expression::Bound(v) => {
            if let Some(v_rewritten) = rewrite_static_variable(v, tree) {
                Expression::Bound(v_rewritten)
            } else {
                None
            }
        }
        Expression::If(left, mid, right) => {
            let left_trans_opt = rewrite_static_expression(left, tree, external_ids_in_scope);
            let mid_trans_opt = rewrite_static_expression(mid, tree, external_ids_in_scope);
            let right_trans_opt = rewrite_static_expression(right, tree, external_ids_in_scope);
            if let Some(left_trans) = left_trans_opt {
                if let Some(right_trans) = right_trans_opt {
                    if let Some(mid_trans) = mid_trans_opt {
                        Some(Expression::If(
                            Box::new(left_trans),
                            Box::new(mid_trans),
                            Box::new(right_trans),
                        ))
                    }
                }
            } else {
                None
            }
        }
        Expression::Coalesce(wrapped) => {
            let rewritten = wrapped.iter().map(|e| rewrite_static_expression(e, tree, external_ids_in_scope));
            if (&rewritten).all(|x| x.is_some()) {
                Expression::Coalesce(rewritten.into_iter().map(|x| x.unwrap()).collect())
            } else {
                None
            }
        }
        Expression::FunctionCall(fun, args) => {
            let args_rewritten = args.iter().map(|e| rewrite_static_expression(e, tree, external_ids_in_scope));
            if (&args_rewritten).all(|x| x.is_some()) {
                Expression::FunctionCall(fun.clone(), args_rewritten.map(|x| x.unwrap()).collect())
            } else {
                None
            }
        }
    }
}

fn rewrite_static_variable(
    v: &Variable,
    tree: &BTreeMap<Variable, Constraint>,
) -> Option<Variable> {
    if let Some(ctr) = tree.get(&v) {
        if !(ctr == Constraint::ExternalDataPoint
            || ctr == Constraint::ExternalDataValue
            || ctr == Constraint::ExternalTimestamp)
        {
            Some(v.clone())
        }
        None
    } else {
        Some(v.clone())
    }
}

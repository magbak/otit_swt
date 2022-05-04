use crate::type_inference::Constraint;
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};
use spargebra::Query;
use std::collections::BTreeMap;

pub fn rewrite_static_query(query: Query, tree: &BTreeMap<TermPattern, Constraint>) -> Query {
    if let Query::Select {
        dataset,
        pattern,
        base_iri,
    } = &query
    {
        let new_pattern = rewrite_static_graph_pattern(pattern, tree);
        let mut static_query = Query::Select {
            dataset: None,
            pattern: new_pattern,
            base_iri: None,
        };
        static_query
    } else {
        panic!("Only support for Select");
    }
}

pub fn rewrite_static_graph_pattern(
    graph_pattern: &GraphPattern,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    match graph_pattern {
        GraphPattern::Bgp { patterns } => GraphPattern::Bgp {
            patterns: rewrite_static_triple_pattern(patterns, tree),
        },
        GraphPattern::Path {
            subject,
            path,
            object,
        } => rewrite_static_path(subject, path, object, tree),
        GraphPattern::Join { left, right } => rewrite_static_join(left, right, tree),
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => rewrite_static_left_join(left, right, expression),
        GraphPattern::Filter { expr, inner } => rewrite_static_filter(expr, inner, tree),
        GraphPattern::Union { left, right } => rewrite_static_union(left, right, tree),
        GraphPattern::Graph { name, inner } => rewrite_static_graph(name, inner, tree),
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            todo!()
        }
        GraphPattern::Minus { left, right } => rewrite_static_minus(left, right, tree),
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
        } => rewrite_static_group(inner, variables, aggregates, tree),
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
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    GraphPattern::Graph {
        name: name.clone(),
        inner: Box::from(rewrite_static_graph_pattern(inner, tree)),
    }
}

fn rewrite_static_union(
    left: &Box<GraphPattern>,
    right: &Box<GraphPattern>,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    todo!()
}

fn rewrite_static_join(
    p0: &Box<GraphPattern>,
    p1: &Box<GraphPattern>,
    p2: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    GraphPattern::Join {
        left: Box::from(rewrite_static_graph_pattern(left, tree)),
        right: Box::from(rewrite_static_graph_pattern(right, tree)),
    }
}

fn rewrite_static_left_join(
    p0: &Box<GraphPattern>,
    p1: &Box<GraphPattern>,
    p2: &Option<Expression>,
) -> GraphPattern {
    GraphPattern::LeftJoin {
        left: Box::from(rewrite_static_graph_pattern(left, tree)),
        right: Box::from(rewrite_static_graph_pattern(right, tree)),
        expression: if let Some(actual_expression) = expression {
            Some(rewrite_static_expression(actual_expression, tree))
        } else {
            None
        },
    }
}

fn rewrite_static_filter(
    expr: &Expression,
    inner: &Box<GraphPattern>,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    GraphPattern::Filter {
        expr: rewrite_static_expression(expr, tree),
        inner: Box::from(rewrite_static_graph_pattern(inner, tree)),
    }
}

fn rewrite_static_group(
    graph_pattern: &Box<GraphPattern>,
    variables: &Vec<Variable>,
    aggregates: &Vec<(Variable, AggregateExpression)>,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    todo!()
}

fn rewrite_static_distinct(
    inner: &Box<GraphPattern>,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    todo!()
}

fn rewrite_static_project(
    inner: &Box<GraphPattern>,
    variables: &Vec<Variable>,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    todo!()
}

fn rewrite_static_order_by(
    inner: &Box<GraphPattern>,
    expression: &Vec<OrderExpression>,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    todo!()
}

fn rewrite_static_minus(
    left: &Box<GraphPattern>,
    right: &Box<GraphPattern>,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    todo!()
}

pub fn rewrite_static_triple_patterns(
    patterns: &Vec<TriplePattern>,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> Vec<TriplePattern> {
    let mut new_triples = vec![];
    for t in patterns {
        todo!();
    }
    new_triples
}

pub fn rewrite_static_path(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> GraphPattern {
    todo!()
}

pub fn rewrite_static_expression(
    expression: &Expression,
    tree: &BTreeMap<TermPattern, Constraint>,
) -> Expression {
    todo!()
}

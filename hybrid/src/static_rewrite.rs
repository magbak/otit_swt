use crate::const_uris::HAS_EXTERNAL_ID;
use crate::constraints::Constraint;
use log::debug;
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{
    GroundTerm, NamedNode, NamedNodePattern, TermPattern, TriplePattern, Variable,
};
use spargebra::Query;
use std::collections::HashMap;

#[derive(PartialEq, Debug, Clone)]
pub enum ChangeType {
    Relaxed,
    Constrained,
    NoChange,
}

impl ChangeType {
    fn opposite(&self) -> ChangeType {
        match self {
            ChangeType::Relaxed => ChangeType::Constrained,
            ChangeType::Constrained => ChangeType::Relaxed,
            ChangeType::NoChange => ChangeType::NoChange,
        }
    }
}

pub struct StaticQueryRewriter {
    variable_counter: u16,
}

impl StaticQueryRewriter {
    pub fn new() -> StaticQueryRewriter {
        StaticQueryRewriter {
            variable_counter: 0,
        }
    }

    pub fn rewrite_static_query(
        &mut self,
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
            let required_change_direction = ChangeType::Relaxed;
            let pattern_rewrite_opt = self.rewrite_static_graph_pattern(
                pattern,
                has_constraint,
                &required_change_direction,
                &mut external_ids_in_scope,
            );
            if let Some((pattern_rewrite, change_type)) = pattern_rewrite_opt {
                if change_type == ChangeType::NoChange || change_type == ChangeType::Relaxed {
                    return Some(Query::Select {
                        dataset: dataset.clone(),
                        pattern: pattern_rewrite,
                        base_iri: base_iri.clone(),
                    });
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            panic!("Only support for Select");
        }
    }

    pub fn rewrite_static_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                self.rewrite_static_bgp(patterns, has_constraint, external_ids_in_scope)
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.rewrite_static_path(subject, path, object),
            GraphPattern::Join { left, right } => self.rewrite_static_join(
                left,
                right,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => self.rewrite_static_left_join(
                left,
                right,
                expression,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Filter { expr, inner } => self.rewrite_static_filter(
                expr,
                inner,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Union { left, right } => self.rewrite_static_union(
                left,
                right,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Graph { name, inner } => self.rewrite_static_graph(
                name,
                inner,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => self.rewrite_static_extend(
                inner,
                variable,
                expression,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Minus { left, right } => self.rewrite_static_minus(
                left,
                right,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Values {
                variables,
                bindings,
            } => self.rewrite_static_values(variables, bindings),
            GraphPattern::OrderBy { inner, expression } => self.rewrite_static_order_by(
                inner,
                expression,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Project { inner, variables } => self.rewrite_static_project(
                inner,
                variables,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Distinct { inner } => self.rewrite_static_distinct(
                inner,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Reduced { inner } => self.rewrite_static_reduced(
                inner,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => self.rewrite_static_slice(
                inner,
                start,
                length,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => self.rewrite_static_group(
                inner,
                variables,
                aggregates,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            ),
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => self.rewrite_static_service(
                name,
                inner,
                silent,
                has_constraint,
                external_ids_in_scope,
            ),
        }
    }

    fn rewrite_static_values(
        &mut self,
        variables: &Vec<Variable>,
        bindings: &Vec<Vec<Option<GroundTerm>>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        return Some((
            GraphPattern::Values {
                variables: variables.iter().map(|v| v.clone()).collect(),
                bindings: bindings.iter().map(|b| b.clone()).collect(),
            },
            ChangeType::NoChange,
        ));
    }

    fn rewrite_static_graph(
        &mut self,
        name: &NamedNodePattern,
        inner: &Box<GraphPattern>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        if let Some((inner_rewrite, inner_change)) = self.rewrite_static_graph_pattern(
            inner,
            has_constraint,
            required_change_direction,
            external_ids_in_scope,
        ) {
            return Some((
                GraphPattern::Graph {
                    name: name.clone(),
                    inner: Box::new(inner_rewrite),
                },
                inner_change,
            ));
        }
        None
    }

    fn rewrite_static_union(
        &mut self,
        left: &Box<GraphPattern>,
        right: &Box<GraphPattern>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        let mut left_external_ids_in_scope = external_ids_in_scope.clone();
        let left_rewrite_opt = self.rewrite_static_graph_pattern(
            left,
            has_constraint,
            required_change_direction,
            &mut left_external_ids_in_scope,
        );
        let mut right_external_ids_in_scope = external_ids_in_scope.clone();
        let right_rewrite_opt = self.rewrite_static_graph_pattern(
            right,
            has_constraint,
            required_change_direction,
            &mut right_external_ids_in_scope,
        );
        merge_external_variables_in_scope(left_external_ids_in_scope, external_ids_in_scope);
        merge_external_variables_in_scope(right_external_ids_in_scope, external_ids_in_scope);

        match required_change_direction {
            ChangeType::Relaxed => {
                if let (Some((left_rewrite, left_change)), Some((right_rewrite, right_change))) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    let use_change;
                    if left_change == &ChangeType::NoChange && right_change == &ChangeType::NoChange
                    {
                        use_change = ChangeType::NoChange;
                    } else if left_change == &ChangeType::NoChange
                        || right_change == &ChangeType::NoChange
                        || left_change == &ChangeType::Relaxed
                        || right_change == &ChangeType::Relaxed
                    {
                        use_change = ChangeType::Relaxed;
                    } else {
                        return None;
                    }
                    return Some((
                        GraphPattern::Union {
                            left: Box::new(left_rewrite.clone()),
                            right: Box::new(right_rewrite.clone()),
                        },
                        use_change,
                    ));
                }
                if let (None, Some((right_rewrite, right_change))) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    if right_change == &ChangeType::Relaxed || right_change == &ChangeType::NoChange
                    {
                        return Some((right_rewrite.clone(), right_change.clone()));
                    }
                }
                if let (Some((left_rewrite, left_change)), None) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    if left_change == &ChangeType::Relaxed || left_change == &ChangeType::NoChange {
                        return Some((left_rewrite.clone(), left_change.clone()));
                    }
                }
            }
            ChangeType::Constrained => {
                if let (Some((left_rewrite, left_change)), Some((right_rewrite, right_change))) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    let use_change;
                    if left_change == &ChangeType::NoChange && right_change == &ChangeType::NoChange
                    {
                        use_change = ChangeType::NoChange;
                    } else if left_change == &ChangeType::NoChange
                        || right_change == &ChangeType::NoChange
                        || left_change == &ChangeType::Constrained
                        || right_change == &ChangeType::Constrained
                    {
                        use_change = ChangeType::Constrained;
                    } else {
                        return None;
                    }
                    return Some((
                        GraphPattern::Union {
                            left: Box::new(left_rewrite.clone()),
                            right: Box::new(right_rewrite.clone()),
                        },
                        use_change,
                    ));
                }
                if let (None, Some((right_rewrite, right_change))) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    if right_change == &ChangeType::Constrained
                        || right_change == &ChangeType::NoChange
                    {
                        return Some((right_rewrite.clone(), right_change.clone()));
                    }
                }
                if let (Some((left_rewrite, left_change)), None) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    if left_change == &ChangeType::Constrained
                        || left_change == &ChangeType::NoChange
                    {
                        return Some((left_rewrite.clone(), left_change.clone()));
                    }
                }
            }
            ChangeType::NoChange => {
                if let (Some((left_rewrite, left_change)), Some((right_rewrite, right_change))) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    if left_change == &ChangeType::NoChange && right_change == &ChangeType::NoChange
                    {
                        return Some((
                            GraphPattern::Union {
                                left: Box::new(left_rewrite.clone()),
                                right: Box::new(right_rewrite.clone()),
                            },
                            ChangeType::NoChange,
                        ));
                    }
                }
                if let (None, Some((right_rewrite, right_change))) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    if right_change == &ChangeType::NoChange {
                        return Some((right_rewrite.clone(), right_change.clone()));
                    }
                }
                if let (Some((left_rewrite, left_change)), None) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    if left_change == &ChangeType::NoChange {
                        return Some((left_rewrite.clone(), left_change.clone()));
                    }
                }
            }
        }

        None
    }

    fn rewrite_static_join(
        &mut self,
        left: &Box<GraphPattern>,
        right: &Box<GraphPattern>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        let mut left_external_ids_in_scope = external_ids_in_scope.clone();
        let left_rewrite_opt = self.rewrite_static_graph_pattern(
            left,
            has_constraint,
            required_change_direction,
            &mut left_external_ids_in_scope,
        );
        let mut right_external_ids_in_scope = external_ids_in_scope.clone();
        let right_rewrite_opt = self.rewrite_static_graph_pattern(
            right,
            has_constraint,
            required_change_direction,
            &mut right_external_ids_in_scope,
        );
        merge_external_variables_in_scope(left_external_ids_in_scope, external_ids_in_scope);
        merge_external_variables_in_scope(right_external_ids_in_scope, external_ids_in_scope);

        if let (Some((left_rewrite, left_change)), Some((right_rewrite, right_change))) =
            (&left_rewrite_opt, &right_rewrite_opt)
        {
            let use_change;
            if left_change == &ChangeType::NoChange && right_change == &ChangeType::NoChange {
                use_change = ChangeType::NoChange;
            } else if (left_change == &ChangeType::NoChange || left_change == &ChangeType::Relaxed)
                && (right_change == &ChangeType::NoChange || right_change == &ChangeType::Relaxed)
            {
                use_change = ChangeType::Relaxed;
            } else if (left_change == &ChangeType::NoChange
                || left_change == &ChangeType::Constrained)
                && (right_change == &ChangeType::NoChange
                    || right_change == &ChangeType::Constrained)
            {
                use_change = ChangeType::Constrained;
            } else {
                return None;
            }
            return Some((
                GraphPattern::Join {
                    left: Box::new(left_rewrite.clone()),
                    right: Box::new(right_rewrite.clone()),
                },
                use_change,
            ));
        }
        if let (Some((left_rewrite, left_change)), None) = (&left_rewrite_opt, &right_rewrite_opt) {
            if left_change == &ChangeType::NoChange || left_change == &ChangeType::Relaxed {
                return Some((left_rewrite.clone(), ChangeType::Relaxed));
            }
        }
        if let (None, Some((right_rewrite, right_change))) = (&left_rewrite_opt, &right_rewrite_opt)
        {
            if right_change == &ChangeType::NoChange || right_change == &ChangeType::Relaxed {
                return Some((right_rewrite.clone(), ChangeType::Relaxed));
            }
        }
        None
    }

    fn rewrite_static_left_join(
        &mut self,
        left: &Box<GraphPattern>,
        right: &Box<GraphPattern>,
        expression_opt: &Option<Expression>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        let mut left_external_ids_in_scope = external_ids_in_scope.clone();
        let left_rewrite_opt = self.rewrite_static_graph_pattern(
            left,
            has_constraint,
            required_change_direction,
            &mut left_external_ids_in_scope,
        );
        let mut right_external_ids_in_scope = external_ids_in_scope.clone();
        let right_rewrite_opt = self.rewrite_static_graph_pattern(
            right,
            has_constraint,
            required_change_direction,
            &mut right_external_ids_in_scope,
        );
        merge_external_variables_in_scope(left_external_ids_in_scope, external_ids_in_scope);
        merge_external_variables_in_scope(right_external_ids_in_scope, external_ids_in_scope);

        let mut expression_rewrite_opt = None;
        if let Some(expression) = expression_opt {
            expression_rewrite_opt = self.rewrite_static_expression(
                expression,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            );
        }
        if let (Some((left_rewrite, left_change)), Some((right_rewrite, right_change))) =
            (&left_rewrite_opt, &right_rewrite_opt)
        {
            if let Some((expression_rewrite, expression_change)) = expression_rewrite_opt {
                let use_change;
                if expression_change == ChangeType::NoChange
                    && left_change == &ChangeType::NoChange
                    && right_change == &ChangeType::NoChange
                {
                    use_change = ChangeType::NoChange;
                } else if (expression_change == ChangeType::NoChange
                    || expression_change == ChangeType::Relaxed)
                    && (left_change == &ChangeType::NoChange || left_change == &ChangeType::Relaxed)
                    && (right_change == &ChangeType::NoChange
                        || right_change == &ChangeType::Relaxed)
                {
                    use_change = ChangeType::Relaxed;
                } else if (expression_change == ChangeType::NoChange
                    || expression_change == ChangeType::Constrained)
                    && (left_change == &ChangeType::NoChange
                        || left_change == &ChangeType::Constrained)
                    && (right_change == &ChangeType::NoChange
                        || right_change == &ChangeType::Constrained)
                {
                    use_change = ChangeType::Constrained;
                } else {
                    return None;
                }
                return Some((
                    GraphPattern::LeftJoin {
                        left: Box::new(left_rewrite.clone()),
                        right: Box::new(right_rewrite.clone()),
                        expression: Some(expression_rewrite),
                    },
                    use_change,
                ));
            } else if expression_opt.is_some() && expression_rewrite_opt.is_none() {
                if (left_change == &ChangeType::NoChange || left_change == &ChangeType::Relaxed)
                    && (right_change == &ChangeType::NoChange
                        || right_change == &ChangeType::Relaxed)
                {
                    return Some((
                        GraphPattern::LeftJoin {
                            left: Box::new(left_rewrite.clone()),
                            right: Box::new(right_rewrite.clone()),
                            expression: None,
                        },
                        ChangeType::Relaxed,
                    ));
                } else {
                    return None;
                }
            } else if expression_opt.is_none() {
                if left_change == &ChangeType::NoChange && right_change == &ChangeType::NoChange {
                    return Some((
                        GraphPattern::LeftJoin {
                            left: Box::new(left_rewrite.clone()),
                            right: Box::new(right_rewrite.clone()),
                            expression: None,
                        },
                        ChangeType::NoChange,
                    ));
                } else if (left_change == &ChangeType::NoChange
                    || left_change == &ChangeType::Relaxed)
                    && (right_change == &ChangeType::NoChange
                        || right_change == &ChangeType::Relaxed)
                {
                    return Some((
                        GraphPattern::LeftJoin {
                            left: Box::new(left_rewrite.clone()),
                            right: Box::new(right_rewrite.clone()),
                            expression: None,
                        },
                        ChangeType::Relaxed,
                    ));
                } else if (left_change == &ChangeType::NoChange
                    || left_change == &ChangeType::Constrained)
                    && (right_change == &ChangeType::NoChange
                        || right_change == &ChangeType::Constrained)
                {
                    return Some((
                        GraphPattern::LeftJoin {
                            left: Box::new(left_rewrite.clone()),
                            right: Box::new(right_rewrite.clone()),
                            expression: None,
                        },
                        ChangeType::Constrained,
                    ));
                }
            }
        }
        if let (Some((left_rewrite, left_change)), None) = (&left_rewrite_opt, &right_rewrite_opt) {
            if let Some((expression_rewrite, expression_change)) = &expression_rewrite_opt {
                if (expression_change == &ChangeType::NoChange
                    || expression_change == &ChangeType::Relaxed)
                    && (left_change == &ChangeType::NoChange || left_change == &ChangeType::Relaxed)
                {
                    return Some((
                        GraphPattern::Filter {
                            expr: expression_rewrite.clone(),
                            inner: Box::new(left_rewrite.clone()),
                        },
                        ChangeType::Relaxed,
                    ));
                }
            } else if expression_opt.is_some() && expression_rewrite_opt.is_none() {
                if left_change == &ChangeType::NoChange || left_change == &ChangeType::Relaxed {
                    return Some((left_rewrite.clone(), ChangeType::Relaxed));
                }
            }
        }
        if let (None, Some((right_rewrite, right_change))) = (&left_rewrite_opt, &right_rewrite_opt)
        {
            if let Some((expression_rewrite, expression_change)) = &expression_rewrite_opt {
                if (expression_change == &ChangeType::NoChange
                    || expression_change == &ChangeType::Relaxed)
                    && (right_change == &ChangeType::NoChange
                        || right_change == &ChangeType::Relaxed)
                {
                    return Some((
                        GraphPattern::Filter {
                            inner: Box::new(right_rewrite.clone()),
                            expr: expression_rewrite.clone(),
                        },
                        ChangeType::Relaxed,
                    ));
                }
            } else if expression_opt.is_some() && expression_rewrite_opt.is_none() {
                if right_change == &ChangeType::NoChange || right_change == &ChangeType::Relaxed {
                    return Some((right_rewrite.clone(), ChangeType::Relaxed));
                }
            }
        }
        None
    }

    fn rewrite_static_filter(
        &mut self,
        expression: &Expression,
        inner: &Box<GraphPattern>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        let inner_rewrite_opt = self.rewrite_static_graph_pattern(
            inner,
            has_constraint,
            required_change_direction,
            external_ids_in_scope,
        );
        if let Some((inner_rewrite, inner_change)) = inner_rewrite_opt {
            let expression_rewrite_opt = self.rewrite_static_expression(
                expression,
                has_constraint,
                required_change_direction,
                external_ids_in_scope,
            );
            if let Some((expression_rewrite, expression_change)) = expression_rewrite_opt {
                let use_change;
                if expression_change == ChangeType::NoChange {
                    use_change = inner_change;
                } else if expression_change == ChangeType::Relaxed {
                    if inner_change == ChangeType::Relaxed || inner_change == ChangeType::NoChange {
                        use_change = ChangeType::Relaxed;
                    } else {
                        return None;
                    }
                } else if expression_change == ChangeType::Constrained {
                    if inner_change == ChangeType::Constrained {
                        use_change = ChangeType::Constrained;
                    } else {
                        return None;
                    }
                } else {
                    panic!("Should never happen");
                }
                return Some((
                    GraphPattern::Filter {
                        expr: expression_rewrite,
                        inner: Box::new(inner_rewrite),
                    },
                    use_change,
                ));
            } else {
                return Some((inner_rewrite, inner_change));
            }
        }
        debug!("Filter returned None");
        None
    }

    fn rewrite_static_group(
        &mut self,
        graph_pattern: &GraphPattern,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        let graph_pattern_rewrite_opt = self.rewrite_static_graph_pattern(
            graph_pattern,
            has_constraint,
            required_change_direction,
            external_ids_in_scope,
        );
        if let Some((graph_pattern_rewrite, graph_pattern_change)) = graph_pattern_rewrite_opt {
            let aggregates_rewrite = aggregates.iter().map(|(v, a)| {
                (
                    rewrite_static_variable(v, has_constraint),
                    self.rewrite_static_aggregate_expression(
                        a,
                        has_constraint,
                        external_ids_in_scope,
                    ),
                )
            });
            let aggregates_rewrite = aggregates_rewrite
                .into_iter()
                .filter(|(x, y)| x.is_some() && y.is_some())
                .map(|(x, y)| (x.unwrap(), y.unwrap()))
                .collect::<Vec<(Variable, AggregateExpression)>>();
            //TODO! Check if we need to handle variables_rewritten len=0
            let variables_rewritten = variables
                .iter()
                .map(|v| rewrite_static_variable(v, has_constraint))
                .filter(|x| x.is_some());
            if aggregates_rewrite.len() > 0 {
                return Some((
                    GraphPattern::Group {
                        inner: Box::new(graph_pattern_rewrite),
                        variables: variables_rewritten.map(|x| x.unwrap()).collect(),
                        aggregates: vec![],
                    },
                    graph_pattern_change,
                ));
            } else {
                return Some((graph_pattern_rewrite, graph_pattern_change));
            }
        }
        None
    }

    fn rewrite_static_aggregate_expression(
        &mut self,
        aggregate_expression: &AggregateExpression,
        has_constraint: &HashMap<TermPattern, Constraint>,
        external_ids_in_scope: &HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<AggregateExpression> {
        match aggregate_expression {
            AggregateExpression::Count { expr, distinct } => {
                if let Some(boxed_expression) = expr {
                    if let Some((expr_rewritten, ChangeType::NoChange)) = self
                        .rewrite_static_expression(
                            boxed_expression,
                            has_constraint,
                            &ChangeType::NoChange,
                            external_ids_in_scope,
                        )
                    {
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self
                    .rewrite_static_expression(
                        expr,
                        has_constraint,
                        &ChangeType::NoChange,
                        external_ids_in_scope,
                    )
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self
                    .rewrite_static_expression(
                        expr,
                        has_constraint,
                        &ChangeType::NoChange,
                        external_ids_in_scope,
                    )
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self
                    .rewrite_static_expression(
                        expr,
                        has_constraint,
                        &ChangeType::NoChange,
                        external_ids_in_scope,
                    )
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self
                    .rewrite_static_expression(
                        expr,
                        has_constraint,
                        &ChangeType::NoChange,
                        external_ids_in_scope,
                    )
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self
                    .rewrite_static_expression(
                        expr,
                        has_constraint,
                        &ChangeType::NoChange,
                        external_ids_in_scope,
                    )
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self
                    .rewrite_static_expression(
                        expr,
                        has_constraint,
                        &ChangeType::NoChange,
                        external_ids_in_scope,
                    )
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self
                    .rewrite_static_expression(
                        expr,
                        has_constraint,
                        &ChangeType::NoChange,
                        external_ids_in_scope,
                    )
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
        &mut self,
        inner: &Box<GraphPattern>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        if let Some((inner_rewrite, inner_change_type)) = self.rewrite_static_graph_pattern(
            inner,
            has_constraint,
            required_change_direction,
            external_ids_in_scope,
        ) {
            Some((
                GraphPattern::Distinct {
                    inner: Box::new(inner_rewrite),
                },
                inner_change_type,
            ))
        } else {
            None
        }
    }

    fn rewrite_static_project(
        &mut self,
        inner: &Box<GraphPattern>,
        variables: &Vec<Variable>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        if let Some((inner_rewrite, inner_change_type)) = self.rewrite_static_graph_pattern(
            inner,
            has_constraint,
            required_change_direction,
            external_ids_in_scope,
        ) {
            let mut variables_rewrite = variables
                .iter()
                .map(|v| rewrite_static_variable(v, has_constraint))
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect::<Vec<Variable>>();
            let mut keys_sorted = external_ids_in_scope.keys().collect::<Vec<&TermPattern>>();
            keys_sorted.sort_by_key(|v|v.to_string());
            for k in keys_sorted {
                let vs = external_ids_in_scope.get(k).unwrap();
                let mut vars = vs.iter().collect::<Vec<&Variable>>();
                //Sort to make rewrites deterministic
                vars.sort_by_key(|v|v.to_string());
                for v in vars {
                    variables_rewrite.push(v.clone());
                }
            }
            if variables_rewrite.len() > 0 {
                return Some((
                    GraphPattern::Project {
                        inner: Box::new(inner_rewrite),
                        variables: variables_rewrite,
                    },
                    inner_change_type,
                ));
            }
        }
        None
    }

    fn rewrite_static_order_by(
        &mut self,
        inner: &Box<GraphPattern>,
        order_expressions: &Vec<OrderExpression>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        if let Some((inner_rewrite, inner_change)) = self.rewrite_static_graph_pattern(
            inner,
            has_constraint,
            required_change_direction,
            external_ids_in_scope,
        ) {
            let expressions_rewrite = order_expressions
                .iter()
                .map(|e| {
                    self.rewrite_static_order_expression(e, has_constraint, &external_ids_in_scope)
                })
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect::<Vec<OrderExpression>>();
            if expressions_rewrite.len() > 0 {
                return Some((
                    GraphPattern::OrderBy {
                        inner: Box::new(inner_rewrite),
                        expression: expressions_rewrite,
                    },
                    inner_change,
                ));
            }
        }
        None
    }

    fn rewrite_static_order_expression(
        &mut self,
        order_expression: &OrderExpression,
        has_constraint: &HashMap<TermPattern, Constraint>,
        external_ids_in_scope: &HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<OrderExpression> {
        match order_expression {
            OrderExpression::Asc(e) => {
                if let Some((e_rewrite, ChangeType::NoChange)) = self.rewrite_static_expression(
                    e,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                ) {
                    Some(OrderExpression::Asc(e_rewrite))
                } else {
                    None
                }
            }
            OrderExpression::Desc(e) => {
                if let Some((e_rewrite, ChangeType::NoChange)) = self.rewrite_static_expression(
                    e,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                ) {
                    Some(OrderExpression::Desc(e_rewrite))
                } else {
                    None
                }
            }
        }
    }

    fn rewrite_static_minus(
        &mut self,
        left: &Box<GraphPattern>,
        right: &Box<GraphPattern>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        let mut left_external_ids_in_scope = external_ids_in_scope.clone();
        let left_rewrite_opt = self.rewrite_static_graph_pattern(
            left,
            has_constraint,
            required_change_direction,
            &mut left_external_ids_in_scope,
        );
        let mut right_external_ids_in_scope = external_ids_in_scope.clone();
        let right_rewrite_opt = self.rewrite_static_graph_pattern(
            right,
            has_constraint,
            &required_change_direction.opposite(),
            &mut right_external_ids_in_scope,
        );
        //Only append left side since minus does not introduce these..
        merge_external_variables_in_scope(left_external_ids_in_scope, external_ids_in_scope);

        if let (Some((left_rewrite, left_change)), Some((right_rewrite, right_change))) =
            (&left_rewrite_opt, &right_rewrite_opt)
        {
            if left_change == &ChangeType::NoChange && right_change == &ChangeType::NoChange {
                return Some((
                    GraphPattern::Minus {
                        left: Box::new(left_rewrite.clone()),
                        right: Box::new(right_rewrite.clone()),
                    },
                    ChangeType::NoChange,
                ));
            } else if (left_change == &ChangeType::Relaxed || left_change == &ChangeType::NoChange)
                && (right_change == &ChangeType::Constrained
                    || right_change == &ChangeType::NoChange)
            {
                return Some((
                    GraphPattern::Minus {
                        left: Box::new(left_rewrite.clone()),
                        right: Box::new(right_rewrite.clone()),
                    },
                    ChangeType::Relaxed,
                ));
            } else if (left_change == &ChangeType::Constrained
                || left_change == &ChangeType::NoChange)
                && (right_change == &ChangeType::Relaxed || right_change == &ChangeType::NoChange)
            {
                return Some((
                    GraphPattern::Minus {
                        left: Box::new(left_rewrite.clone()),
                        right: Box::new(right_rewrite.clone()),
                    },
                    ChangeType::Constrained,
                ));
            }
        }
        if let (None, Some(_)) = (&left_rewrite_opt, &right_rewrite_opt) {
            return None;
        }
        if let (Some((left_rewrite, left_change)), None) = (&left_rewrite_opt, &right_rewrite_opt) {
            if left_change == &ChangeType::NoChange || left_change == &ChangeType::Relaxed {
                return Some((left_rewrite.clone(), ChangeType::Relaxed));
            }
        }
        None
    }

    pub fn rewrite_static_bgp(
        &mut self,
        patterns: &Vec<TriplePattern>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        let mut new_triples = vec![];
        for t in patterns {
            let obj_constr_opt = has_constraint.get(&t.object);
                if let Some(obj_constr) = obj_constr_opt {
                    if obj_constr == &Constraint::ExternalTimeseries {
                        if !external_ids_in_scope.contains_key(&t.object) {
                            let external_id_var = Variable::new(
                                "ts_external_id_".to_string()
                                    + &self.variable_counter.to_string(),
                            )
                            .unwrap();
                            self.variable_counter += 1;
                            let new_triple = TriplePattern {
                                subject: t.object.clone(),
                                predicate: NamedNodePattern::NamedNode(
                                    NamedNode::new(HAS_EXTERNAL_ID).unwrap(),
                                ),
                                object: TermPattern::Variable(external_id_var.clone()),
                            };
                            if !new_triples.contains(&new_triple) {
                                new_triples.push(new_triple);
                            }
                            external_ids_in_scope
                                .insert(t.object.clone(), vec![external_id_var.clone()]);
                        }
                    }
            }
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

        if new_triples.is_empty() {
            debug!("New triples in static BGP was empty, returning None");
            None
        } else {
            Some((
                GraphPattern::Bgp {
                    patterns: new_triples,
                },
                ChangeType::NoChange,
            ))
        }
    }

    //We assume that all paths have been rewritten so as to not contain any datapoint, timestamp, or data value.
    //These should have been split into ordinary triples.
    pub fn rewrite_static_path(
        &mut self,
        subject: &TermPattern,
        path: &PropertyPathExpression,
        object: &TermPattern,
    ) -> Option<(GraphPattern, ChangeType)> {
        return Some((
            GraphPattern::Path {
                subject: subject.clone(),
                path: path.clone(),
                object: object.clone(),
            },
            ChangeType::NoChange,
        ));
    }

    pub fn rewrite_static_expression(
        &mut self,
        expression: &Expression,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(Expression, ChangeType)> {
        match expression {
            Expression::NamedNode(nn) => {
                Some((Expression::NamedNode(nn.clone()), ChangeType::NoChange))
            }
            Expression::Literal(l) => Some((Expression::Literal(l.clone()), ChangeType::NoChange)),
            Expression::Variable(v) => {
                if let Some(rewritten_variable) = rewrite_static_variable(v, has_constraint) {
                    Some((
                        Expression::Variable(rewritten_variable),
                        ChangeType::NoChange,
                    ))
                } else {
                    None
                }
            }
            Expression::Or(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    required_change_direction,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    required_change_direction,
                    external_ids_in_scope,
                );
                if let (Some((left_rewrite, left_change)), Some((right_rewrite, right_change))) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    if left_change == &ChangeType::NoChange && right_change == &ChangeType::NoChange
                    {
                        return Some((
                            Expression::Or(
                                Box::new(left_rewrite.clone()),
                                Box::new(right_rewrite.clone()),
                            ),
                            ChangeType::NoChange,
                        ));
                    }
                }
                match required_change_direction {
                    ChangeType::Relaxed => {
                        if let (
                            Some((left_rewrite, left_change)),
                            Some((right_rewrite, right_change)),
                        ) = (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if (left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Relaxed)
                                && (right_change == &ChangeType::NoChange
                                    || right_change == &ChangeType::Relaxed)
                            {
                                return Some((
                                    Expression::Or(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::Relaxed,
                                ));
                            }
                        }
                    }
                    ChangeType::Constrained => {
                        if let (
                            Some((left_rewrite, left_change)),
                            Some((right_rewrite, right_change)),
                        ) = (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if (left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Constrained)
                                && (right_change == &ChangeType::NoChange
                                    || right_change == &ChangeType::Constrained)
                            {
                                return Some((
                                    Expression::Or(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::Constrained,
                                ));
                            }
                        } else if let (Some((left_rewrite, left_change)), None) =
                            (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if left_change == &ChangeType::Constrained
                                || left_change == &ChangeType::NoChange
                            {
                                return Some((left_rewrite.clone(), ChangeType::Constrained));
                            }
                        } else if let (None, Some((right_rewrite, right_change))) =
                            (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if right_change == &ChangeType::Constrained
                                || right_change == &ChangeType::NoChange
                            {
                                return Some((right_rewrite.clone(), ChangeType::Constrained));
                            }
                        }
                    }
                    ChangeType::NoChange => {}
                }
                None
            }

            Expression::And(left, right) => {
                // We allow translations of left- or right hand sides of And-expressions to be None.
                // This allows us to enforce the remaining conditions that were not removed due to a rewrite
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    required_change_direction,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    required_change_direction,
                    external_ids_in_scope,
                );
                if let (Some((left_rewrite, left_change)), Some((right_rewrite, right_change))) =
                    (&left_rewrite_opt, &right_rewrite_opt)
                {
                    if left_change == &ChangeType::NoChange || right_change == &ChangeType::NoChange
                    {
                        return Some((
                            Expression::And(
                                Box::new(left_rewrite.clone()),
                                Box::new(right_rewrite.clone()),
                            ),
                            ChangeType::NoChange,
                        ));
                    }
                }
                match required_change_direction {
                    ChangeType::Relaxed => {
                        if let (
                            Some((left_rewrite, left_change)),
                            Some((right_rewrite, right_change)),
                        ) = (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if (left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Relaxed)
                                && (right_change == &ChangeType::NoChange
                                    || right_change == &ChangeType::Relaxed)
                            {
                                return Some((
                                    Expression::And(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::Relaxed,
                                ));
                            }
                        } else if let (Some((left_rewrite, left_change)), None) =
                            (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if left_change == &ChangeType::Relaxed
                                || left_change == &ChangeType::NoChange
                            {
                                return Some((left_rewrite.clone(), ChangeType::Relaxed));
                            }
                        } else if let (None, Some((right_rewrite, right_change))) =
                            (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if right_change == &ChangeType::Relaxed
                                || right_change == &ChangeType::NoChange
                            {
                                return Some((right_rewrite.clone(), ChangeType::Relaxed));
                            }
                        }
                    }
                    ChangeType::Constrained => {
                        if let (
                            Some((left_rewrite, left_change)),
                            Some((right_rewrite, right_change)),
                        ) = (&left_rewrite_opt, &right_rewrite_opt)
                        {
                            if (left_change == &ChangeType::NoChange
                                || left_change == &ChangeType::Constrained)
                                && (right_change == &ChangeType::NoChange
                                    || right_change == &ChangeType::Constrained)
                            {
                                return Some((
                                    Expression::And(
                                        Box::new(left_rewrite.clone()),
                                        Box::new(right_rewrite.clone()),
                                    ),
                                    ChangeType::Constrained,
                                ));
                            }
                        }
                    }
                    ChangeType::NoChange => {}
                }

                None
            }
            Expression::Equal(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::Equal(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::SameTerm(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::SameTerm(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::Greater(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::Greater(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::GreaterOrEqual(
                                Box::new(left_rewrite),
                                Box::new(right_rewrite),
                            ),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::Less(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::Less(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::LessOrEqual(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::LessOrEqual(
                                Box::new(left_rewrite),
                                Box::new(right_rewrite),
                            ),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::In(left, expressions) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let expressions_rewritten_opts = expressions
                    .iter()
                    .map(|e| {
                        self.rewrite_static_expression(
                            e,
                            has_constraint,
                            &ChangeType::NoChange,
                            external_ids_in_scope,
                        )
                    })
                    .collect::<Vec<Option<(Expression, ChangeType)>>>();
                if expressions_rewritten_opts.iter().any(|x| x.is_none()) {
                    return None;
                }
                let expressions_rewritten = expressions_rewritten_opts
                    .into_iter()
                    .map(|x| x.unwrap())
                    .collect::<Vec<(Expression, ChangeType)>>();
                if expressions_rewritten
                    .iter()
                    .any(|(_, c)| c != &ChangeType::NoChange)
                {
                    return None;
                }
                let expressions_rewritten_nochange = expressions_rewritten
                    .into_iter()
                    .map(|(e, _)| e)
                    .collect::<Vec<Expression>>();

                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if expressions_rewritten_nochange.len() == expressions.len() {
                        return Some((
                            Expression::In(Box::new(left_rewrite), expressions_rewritten_nochange),
                            ChangeType::NoChange,
                        ));
                    }
                    if required_change_direction == &ChangeType::Constrained {
                        if expressions_rewritten_nochange.is_empty()
                            && (expressions_rewritten_nochange.len() < expressions.len())
                        {
                            return Some((
                                Expression::In(
                                    Box::new(left_rewrite),
                                    expressions_rewritten_nochange,
                                ),
                                ChangeType::Constrained,
                            ));
                        }
                    }
                }
                None
            }
            Expression::Add(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );

                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::Add(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::Subtract(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::Subtract(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::Multiply(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::Multiply(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::Divide(left, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::Divide(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                None
            }
            Expression::UnaryPlus(wrapped) => {
                let wrapped_rewrite_opt = self.rewrite_static_expression(
                    wrapped,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((wrapped_rewrite, ChangeType::NoChange)) = wrapped_rewrite_opt {
                    return Some((
                        Expression::UnaryPlus(Box::new(wrapped_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::UnaryMinus(wrapped) => {
                let wrapped_rewrite_opt = self.rewrite_static_expression(
                    wrapped,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                if let Some((wrapped_rewrite, ChangeType::NoChange)) = wrapped_rewrite_opt {
                    return Some((
                        Expression::UnaryPlus(Box::new(wrapped_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::Not(wrapped) => {
                let wrapped_rewrite_opt = self.rewrite_static_expression(
                    wrapped,
                    has_constraint,
                    &required_change_direction.opposite(),
                    external_ids_in_scope,
                );
                if let Some((wrapped_rewrite, wrapped_change)) = wrapped_rewrite_opt {
                    let use_change_type = match wrapped_change {
                        ChangeType::NoChange => ChangeType::NoChange,
                        ChangeType::Relaxed => ChangeType::Constrained,
                        ChangeType::Constrained => ChangeType::Relaxed,
                    };
                    if use_change_type != ChangeType::NoChange
                        && &use_change_type != required_change_direction
                    {
                        return None;
                    }
                    Some((Expression::Not(Box::new(wrapped_rewrite)), use_change_type))
                } else {
                    None
                }
            }
            Expression::Exists(wrapped) => {
                let wrapped_rewrite_opt = self.rewrite_static_graph_pattern(
                    &wrapped,
                    has_constraint,
                    required_change_direction,
                    &mut external_ids_in_scope.clone(),
                );
                if let Some((wrapped_rewrite, wrapped_change)) = wrapped_rewrite_opt {
                    let use_change = match wrapped_change {
                        ChangeType::Relaxed => ChangeType::Relaxed,
                        ChangeType::Constrained => ChangeType::Constrained,
                        ChangeType::NoChange => ChangeType::NoChange,
                    };
                    Some((Expression::Exists(Box::new(wrapped_rewrite)), use_change))
                } else {
                    None
                }
            }
            Expression::Bound(v) => {
                if let Some(v_rewritten) = rewrite_static_variable(v, has_constraint) {
                    Some((Expression::Bound(v_rewritten), ChangeType::NoChange))
                } else {
                    None
                }
            }
            Expression::If(left, mid, right) => {
                let left_rewrite_opt = self.rewrite_static_expression(
                    left,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let mid_rewrite_opt = self.rewrite_static_expression(
                    mid,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );
                let right_rewrite_opt = self.rewrite_static_expression(
                    right,
                    has_constraint,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                );

                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        if let Some((mid_rewrite, ChangeType::NoChange)) = mid_rewrite_opt {
                            return Some((
                                Expression::If(
                                    Box::new(left_rewrite),
                                    Box::new(mid_rewrite),
                                    Box::new(right_rewrite),
                                ),
                                ChangeType::NoChange,
                            ));
                        }
                    }
                }
                None
            }
            Expression::Coalesce(wrapped) => {
                let rewritten = wrapped
                    .iter()
                    .map(|e| {
                        self.rewrite_static_expression(
                            e,
                            has_constraint,
                            &ChangeType::NoChange,
                            external_ids_in_scope,
                        )
                    })
                    .collect::<Vec<Option<(Expression, ChangeType)>>>();
                if !rewritten.iter().all(|x| x.is_some()) {
                    return None;
                }
                let rewritten_some = rewritten
                    .into_iter()
                    .map(|x| x.unwrap())
                    .collect::<Vec<(Expression, ChangeType)>>();
                if rewritten_some
                    .iter()
                    .all(|(_, c)| c == &ChangeType::NoChange)
                {
                    return Some((
                        Expression::Coalesce(rewritten_some.into_iter().map(|(e, _)| e).collect()),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
            Expression::FunctionCall(fun, args) => {
                let args_rewritten = args
                    .iter()
                    .map(|e| {
                        self.rewrite_static_expression(
                            e,
                            has_constraint,
                            &ChangeType::NoChange,
                            external_ids_in_scope,
                        )
                    })
                    .collect::<Vec<Option<(Expression, ChangeType)>>>();
                if !args_rewritten.iter().all(|x| x.is_some()) {
                    return None;
                }
                let args_rewritten_some = args_rewritten
                    .into_iter()
                    .map(|x| x.unwrap())
                    .collect::<Vec<(Expression, ChangeType)>>();
                if args_rewritten_some
                    .iter()
                    .all(|(_, c)| c == &ChangeType::NoChange)
                {
                    return Some((
                        Expression::FunctionCall(
                            fun.clone(),
                            args_rewritten_some.into_iter().map(|(e, _)| e).collect(),
                        ),
                        ChangeType::NoChange,
                    ));
                }
                None
            }
        }
    }
    fn rewrite_static_extend(
        &mut self,
        inner: &Box<GraphPattern>,
        var: &Variable,
        expr: &Expression,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        let inner_rewrite_opt = self.rewrite_static_graph_pattern(
            inner,
            has_constraint,
            required_change_direction,
            external_ids_in_scope,
        );
        let expr_rewrite_opt = self.rewrite_static_expression(
            expr,
            has_constraint,
            &ChangeType::NoChange,
            external_ids_in_scope,
        );
        if let Some((inner_rewrite, inner_change_type)) = inner_rewrite_opt {
            if let Some((expression_rewrite, _)) = expr_rewrite_opt {
                return Some((
                    GraphPattern::Extend {
                        inner: Box::new(inner_rewrite),
                        variable: var.clone(),
                        expression: expression_rewrite,
                    },
                    inner_change_type,
                ));
            } else {
                return Some((inner_rewrite, inner_change_type));
            }
        }
        None
    }
    fn rewrite_static_slice(
        &mut self,
        inner: &Box<GraphPattern>,
        start: &usize,
        length: &Option<usize>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        let rewrite_inner_opt = self.rewrite_static_graph_pattern(
            inner,
            has_constraint,
            required_change_direction,
            external_ids_in_scope,
        );
        if let Some((rewrite_inner, rewrite_change)) = rewrite_inner_opt {
            return Some((
                GraphPattern::Slice {
                    inner: Box::new(rewrite_inner),
                    start: start.clone(),
                    length: length.clone(),
                },
                rewrite_change,
            ));
        }
        None
    }
    fn rewrite_static_reduced(
        &mut self,
        inner: &Box<GraphPattern>,
        has_constraint: &HashMap<TermPattern, Constraint>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        if let Some((inner_rewrite, inner_change)) = self.rewrite_static_graph_pattern(
            inner,
            has_constraint,
            required_change_direction,
            external_ids_in_scope,
        ) {
            return Some((
                GraphPattern::Reduced {
                    inner: Box::new(inner_rewrite),
                },
                inner_change,
            ));
        }
        None
    }
    fn rewrite_static_service(
        &mut self,
        name: &NamedNodePattern,
        inner: &Box<GraphPattern>,
        silent: &bool,
        has_constraint: &HashMap<TermPattern, Constraint>,
        external_ids_in_scope: &mut HashMap<TermPattern, Vec<Variable>>,
    ) -> Option<(GraphPattern, ChangeType)> {
        if let Some((inner_rewrite, inner_change)) = self.rewrite_static_graph_pattern(
            inner,
            has_constraint,
            &ChangeType::NoChange,
            external_ids_in_scope,
        ) {
            return Some((
                GraphPattern::Service {
                    name: name.clone(),
                    inner: Box::new(inner_rewrite),
                    silent: silent.clone(),
                },
                inner_change,
            ));
        }
        None
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

fn merge_external_variables_in_scope(
    src: HashMap<TermPattern, Vec<Variable>>,
    trg: &mut HashMap<TermPattern, Vec<Variable>>,
) {
    for (k, v) in src {
        if let Some(vs) = trg.get_mut(&k) {
            for vee in v {
                vs.push(vee);
            }
        } else {
            trg.insert(k, v);
        }
    }
}

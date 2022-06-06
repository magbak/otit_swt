use crate::change_types::ChangeType;
use crate::constants::{HAS_DATA_POINT, HAS_EXTERNAL_ID, HAS_TIMESTAMP, HAS_VALUE};
use crate::constraints::Constraint;
use crate::timeseries_query::TimeSeriesQuery;
use log::debug;
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{
    GroundTerm, NamedNode, NamedNodePattern, TermPattern, TriplePattern, Variable,
};
use spargebra::Query;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::env::var;
use std::hash::{Hash, Hasher};

struct GPReturn {
    graph_pattern: Option<GraphPattern>,
    change_type: ChangeType,
    additional_projections: Vec<Variable>,
    variables_in_scope: HashSet<Variable>,
    external_ids_in_scope: HashMap<Variable, Vec<Variable>>,
}

impl GPReturn {
    fn new(graph_pattern:GraphPattern, change_type:ChangeType, additional_projections:Vec<Variable>, variables_in_scope:HashSet<Variable>, external_ids_in_scope: HashMap<Variable, Vec<Variable>>) -> GPReturn {
        GPReturn{
            graph_pattern:Some(graph_pattern), change_type, additional_projections, variables_in_scope, external_ids_in_scope
        }
    }

    fn with_graph_pattern(&mut self, graph_pattern:GraphPattern) -> &mut GPReturn {
        self.graph_pattern = Some(graph_pattern);
        self
    }
    
    fn with_change_type(&mut self, change_type:ChangeType) -> &mut GPReturn {
        self.change_type = change_type;
        self
    }

    fn with_projections_and_scope(&mut self, gpr:&mut GPReturn) -> &mut GPReturn {
        self.additional_projections.append(&mut gpr.additional_projections);
        self.variables_in_scope.extend(&mut gpr.variables_in_scope.drain());
        //TODO: ikke bra nok, må merges skikkelig!!!
        todo!();
        self.external_ids_in_scope.extend(&mut gpr.external_ids_in_scope.drain());
        self
    }
}

#[derive(Debug)]
pub struct StaticQueryRewriter {
    variable_counter: u16,
    additional_projections: HashSet<Variable>,
    has_constraint: HashMap<Variable, Constraint>,
    pub time_series_queries: Vec<TimeSeriesQuery>,
}

impl StaticQueryRewriter {
    pub fn new(has_constraint: &HashMap<Variable, Constraint>) -> StaticQueryRewriter {
        StaticQueryRewriter {
            variable_counter: 0,
            additional_projections: Default::default(),
            has_constraint: has_constraint.clone(),
            time_series_queries: vec![],
        }
    }

    pub fn rewrite_query(&mut self, query: Query) -> Option<(Query, Vec<TimeSeriesQuery>)> {
        if let Query::Select {
            dataset,
            pattern,
            base_iri,
        } = &query
        {
            let mut external_ids_in_scope = HashMap::new();
            let required_change_direction = ChangeType::Relaxed;
            let pattern_rewrite_opt = self.rewrite_graph_pattern(
                pattern,
                &required_change_direction,
                &mut external_ids_in_scope,
                HashSet::new(),
            );
            if let Some(mut gpr_inner) = pattern_rewrite_opt {
                if &gpr_inner.change_type == &ChangeType::NoChange || &gpr_inner.change_type == &ChangeType::Relaxed {
                    return Some((
                        Query::Select {
                            dataset: dataset.clone(),
                            pattern: gpr_inner.graph_pattern.take().unwrap(),
                            base_iri: base_iri.clone(),
                        },
                        self.time_series_queries.clone(),
                    ));
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

    fn rewrite_graph_pattern(
        &mut self,
        graph_pattern: & GraphPattern,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                self.rewrite_bgp(patterns, external_ids_in_scope, variables_in_scope)
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.rewrite_path(subject, path, object, variables_in_scope),
            GraphPattern::Join { left, right } => self.rewrite_join(
                left,
                right,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => self.rewrite_left_join(
                left,
                right,
                expression,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Filter { expr, inner } => self.rewrite_filter(
                expr,
                inner,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Union { left, right } => self.rewrite_union(
                left,
                right,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Graph { name, inner } => self.rewrite_graph(
                name,
                inner,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => self.rewrite_extend(
                inner,
                variable,
                expression,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Minus { left, right } => self.rewrite_minus(
                left,
                right,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Values {
                variables,
                bindings,
            } => self.rewrite_values(variables, bindings),
            GraphPattern::OrderBy { inner, expression } => self.rewrite_order_by(
                inner,
                expression,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Project { inner, variables } => self.rewrite_project(
                inner,
                variables,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Distinct { inner } => self.rewrite_distinct(
                inner,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Reduced { inner } => self.rewrite_reduced(
                inner,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => self.rewrite_slice(
                inner,
                start,
                length,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => self.rewrite_group(
                inner,
                variables,
                aggregates,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            ),
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => self.rewrite_service(
                name,
                inner,
                silent,
                external_ids_in_scope,
                variables_in_scope,
            ),
        }
    }

    fn rewrite_values(
        & mut self,
        variables: & Vec<Variable>,
        bindings: &Vec<Vec<Option<GroundTerm>>>,
    ) -> Option<GPReturn> {
        return Some(GPReturn::new(
            GraphPattern::Values {
                variables: variables.iter().map(|v| v.clone()).collect(),
                bindings: bindings.iter().map(|b| b.clone()).collect(),
            },
            ChangeType::NoChange,
            vec![],
            variables.iter().map(|v| v).collect(),
            HashMap::new(),
        ));
    }

    fn rewrite_graph(
        & mut self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        if let Some(mut inner_gpr) = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope,
        ) {
            inner_gpr.with_graph_pattern(
                GraphPattern::Graph {
                    name: name.clone(),
                    inner: Box::new(inner_gpr.graph_pattern.take().unwrap()),
                });
            return Some(inner_gpr);
        }
        None
    }

    fn rewrite_union(
        & mut self,
        left: & GraphPattern,
        right: & GraphPattern,

        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        let left_rewrite_opt = self.rewrite_graph_pattern(
            left,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope.clone(),
        );
        let mut right_external_ids_in_scope = external_ids_in_scope.clone();
        let right_rewrite_opt = self.rewrite_graph_pattern(
            right,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope,
        );
        
        match required_change_direction {
            ChangeType::Relaxed => {
                if let Some(mut gpr_left) =
                    left_rewrite_opt
                {
                    if let Some(mut gpr_right) =
                        right_rewrite_opt
                    {
                        let use_change;
                        if &gpr_left.change_type == &ChangeType::NoChange
                            && &gpr_right.change_type == &ChangeType::NoChange
                        {
                            use_change = ChangeType::NoChange;
                        } else if &gpr_left.change_type == &ChangeType::NoChange
                            || &gpr_right.change_type == &ChangeType::NoChange
                            || &gpr_left.change_type == &ChangeType::Relaxed
                            || &gpr_right.change_type == &ChangeType::Relaxed
                        {
                            use_change = ChangeType::Relaxed;
                        } else {
                            return None;
                        }
                        gpr_left.with_projections_and_scope(&mut gpr_right).with_graph_pattern(GraphPattern::Union {
                                left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                                right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                            },).with_change_type(use_change);
                        return Some(gpr_left);
                    } else {
                        //left is some, right is none
                        if &gpr_left.change_type == &ChangeType::Relaxed || &gpr_left.change_type == &ChangeType::NoChange
                        {
                            return Some(gpr_left);
                        }
                    }
                } else if let Some(gpr_right) =
                    right_rewrite_opt
                {
                    //left is none, right is some
                    if &gpr_right.change_type == &ChangeType::Relaxed || &gpr_right.change_type == &ChangeType::NoChange {
                        return Some(gpr_right);
                    }
                }
            }
            ChangeType::Constrained => {
                if let Some(mut gpr_left) =
                    left_rewrite_opt
                {
                    if let Some(mut gpr_right) =
                        right_rewrite_opt
                    {
                        let use_change;
                        if &gpr_left.change_type == &ChangeType::NoChange
                            && &gpr_right.change_type == &ChangeType::NoChange
                        {
                            use_change = ChangeType::NoChange;
                        } else if &gpr_left.change_type == &ChangeType::NoChange
                            || &gpr_right.change_type == &ChangeType::NoChange
                            || &gpr_left.change_type == &ChangeType::Constrained
                            || &gpr_right.change_type == &ChangeType::Constrained
                        {
                            use_change = ChangeType::Constrained;
                        } else {
                            return None;
                        }
                        gpr_left.with_projections_and_scope(&mut gpr_right).with_graph_pattern(GraphPattern::Union {
                                left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                                right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                            }).with_change_type(use_change);

                        return Some(gpr_left);
                    } else {
                        //right none
                        if &gpr_left.change_type == &ChangeType::Constrained
                            || &gpr_left.change_type == &ChangeType::NoChange
                        {
                            return Some(gpr_left);
                        }
                    }
                }
                if let Some(gpr_right) =
                    right_rewrite_opt
                {
                    // left none
                    if &gpr_right.change_type == &ChangeType::Constrained
                        || &gpr_right.change_type == &ChangeType::NoChange
                    {
                        return Some(gpr_right);
                    }
                }
            }
            ChangeType::NoChange => {
                if let Some(mut gpr_left) =
                    left_rewrite_opt
                {
                    if let Some(mut gpr_right) =
                        right_rewrite_opt
                    {
                        if &gpr_left.change_type == &ChangeType::NoChange
                            && &gpr_right.change_type == &ChangeType::NoChange
                        {
                            gpr_left.with_projections_and_scope(&mut gpr_right).with_graph_pattern(GraphPattern::Union {
                                    left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                                    right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                                },);
                            return Some(gpr_left);
                        }
                    } else {
                        //right none
                        if &gpr_left.change_type == &ChangeType::NoChange {
                            return Some(gpr_left);
                        }
                    }
                } else if let Some(gpr_right) =
                    right_rewrite_opt
                {
                    if &gpr_right.change_type == &ChangeType::NoChange {
                        return Some(gpr_right);
                    }
                }
            }
        }
        None
    }

    fn rewrite_join(
        & mut self,
        left: & GraphPattern,
        right: & GraphPattern,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        let left_rewrite_opt = self.rewrite_graph_pattern(
            left,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope.clone(),
        );
        let mut right_external_ids_in_scope = external_ids_in_scope.clone();
        let right_rewrite_opt = self.rewrite_graph_pattern(
            right,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope.clone(),
        );

        if let Some(mut gpr_left) = left_rewrite_opt {
            if let Some(mut gpr_right) = right_rewrite_opt
            {
                let use_change;
                if &gpr_left.change_type == &ChangeType::NoChange && &gpr_right.change_type == &ChangeType::NoChange {
                    use_change = ChangeType::NoChange;
                } else if (&gpr_left.change_type == &ChangeType::NoChange
                    || &gpr_left.change_type == &ChangeType::Relaxed)
                    && (&gpr_right.change_type == &ChangeType::NoChange || &gpr_right.change_type == &ChangeType::Relaxed)
                {
                    use_change = ChangeType::Relaxed;
                } else if (&gpr_left.change_type == &ChangeType::NoChange
                    || &gpr_left.change_type == &ChangeType::Constrained)
                    && (&gpr_right.change_type == &ChangeType::NoChange
                        || &gpr_right.change_type == &ChangeType::Constrained)
                {
                    use_change = ChangeType::Constrained;
                } else {
                    return None;
                }
                gpr_left.with_projections_and_scope(&mut gpr_right).with_graph_pattern(GraphPattern::Join {
                        left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                        right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                    }).with_change_type(use_change);
                return Some(gpr_left);
            } else {
                //left some, right none
                if &gpr_left.change_type == &ChangeType::NoChange || &gpr_left.change_type == &ChangeType::Relaxed {
                    gpr_left.with_change_type(ChangeType::Relaxed);
                    return Some(                        gpr_left);
                }
            }
        } else if let Some(mut gpr_right) =
            right_rewrite_opt
        {
            //left is none
            if &gpr_right.change_type == &ChangeType::NoChange || &gpr_right.change_type == &ChangeType::Relaxed {
                gpr_right.with_change_type(ChangeType::Relaxed);
                return Some(gpr_right);
            }
        }
        None
    }

    fn rewrite_left_join(
        & mut self,
        left: & GraphPattern,
        right: & GraphPattern,
        expression_opt: &Option<Expression>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        let mut left_external_ids_in_scope = external_ids_in_scope.clone();
        let left_rewrite_opt = self.rewrite_graph_pattern(
            left,
            required_change_direction,
            &mut left_external_ids_in_scope,
            variables_in_scope.clone(),
        );
        let mut right_external_ids_in_scope = external_ids_in_scope.clone();
        let right_rewrite_opt = self.rewrite_graph_pattern(
            right,
            required_change_direction,
            &mut right_external_ids_in_scope,
            variables_in_scope,
        );
        if let Some(expression) = expression_opt {
            self.pushdown_expression(expression);
        }
        let mut expression_rewrite_opt = None;

        if let Some(mut gpr_left) = left_rewrite_opt {
            if let Some(mut gpr_right) = right_rewrite_opt
            {
                gpr_left.with_projections_and_scope(&mut gpr_right);

                if let Some(expression) = expression_opt {
                    expression_rewrite_opt = self.rewrite_expression(
                        expression,
                        required_change_direction,
                        &gpr_left.external_ids_in_scope,
                        &gpr_left.variables_in_scope,
                    );
                }
                if let Some((expression_rewrite, expression_change)) = expression_rewrite_opt {
                    let use_change;
                    if expression_change == ChangeType::NoChange
                        && &gpr_left.change_type == &ChangeType::NoChange
                        && &gpr_right.change_type == &ChangeType::NoChange
                    {
                        use_change = ChangeType::NoChange;
                    } else if (expression_change == ChangeType::NoChange
                        || expression_change == ChangeType::Relaxed)
                        && (&gpr_left.change_type == &ChangeType::NoChange
                            || &gpr_left.change_type == &ChangeType::Relaxed)
                        && (&gpr_right.change_type == &ChangeType::NoChange
                            || &gpr_right.change_type == &ChangeType::Relaxed)
                    {
                        use_change = ChangeType::Relaxed;
                    } else if (expression_change == ChangeType::NoChange
                        || expression_change == ChangeType::Constrained)
                        && (&gpr_left.change_type == &ChangeType::NoChange
                            || &gpr_left.change_type == &ChangeType::Constrained)
                        && (&gpr_right.change_type == &ChangeType::NoChange
                            || &gpr_right.change_type == &ChangeType::Constrained)
                    {
                        use_change = ChangeType::Constrained;
                    } else {
                        return None;
                    }
                    gpr_left.with_graph_pattern(GraphPattern::LeftJoin {
                            left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                            right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                            expression: Some(expression_rewrite),
                        }).with_change_type(use_change);
                    return Some(gpr_left);
                } else if expression_opt.is_some() && expression_rewrite_opt.is_none() {
                    if (&gpr_left.change_type == &ChangeType::NoChange || &gpr_left.change_type == &ChangeType::Relaxed)
                        && (&gpr_right.change_type == &ChangeType::NoChange
                            || &gpr_right.change_type == &ChangeType::Relaxed)
                    {
                        gpr_left.with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                                right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                                expression: None,
                            },).with_change_type(ChangeType::Relaxed);
                        return Some(gpr_left);
                    } else {
                        return None;
                    }
                } else if expression_opt.is_none() {
                    if &gpr_left.change_type == &ChangeType::NoChange && &gpr_right.change_type == &ChangeType::NoChange {
                        gpr_left.with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                                right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                                expression: None,
                            }).with_change_type(ChangeType::NoChange);
                        return Some(gpr_left);
                    } else if (&gpr_left.change_type == &ChangeType::NoChange
                        || &gpr_left.change_type == &ChangeType::Relaxed)
                        && (&gpr_right.change_type == &ChangeType::NoChange
                            || &gpr_right.change_type == &ChangeType::Relaxed)
                    {
                        gpr_left.with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                                right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                                expression: None,
                            }).with_change_type(ChangeType::Relaxed);
                        return Some(gpr_left);
                    } else if (&gpr_left.change_type == &ChangeType::NoChange
                        || &gpr_left.change_type == &ChangeType::Constrained)
                        && (&gpr_right.change_type == &ChangeType::NoChange
                            || &gpr_right.change_type == &ChangeType::Constrained)
                    {
                        gpr_left.with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                                right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                                expression: None,
                            }).with_change_type(ChangeType::Constrained);
                        return Some(gpr_left);
                    }
                }
            } else {
                if let Some(expression) = expression_opt {
                    expression_rewrite_opt = self.rewrite_expression(
                        expression,
                        required_change_direction,
                        external_ids_in_scope,
                        &gpr_left.variables_in_scope,
                    );
                }
                //left some, right none
                if let Some((expression_rewrite, expression_change)) = expression_rewrite_opt {
                    if (expression_change == ChangeType::NoChange
                        || expression_change == ChangeType::Relaxed)
                        && (&gpr_left.change_type == &ChangeType::NoChange
                            || &gpr_left.change_type == &ChangeType::Relaxed)
                    {
                        gpr_left.with_graph_pattern(GraphPattern::Filter {
                                expr: expression_rewrite,
                                inner: Box::new(gpr_left.graph_pattern.take().unwrap()),
                            }).with_change_type(ChangeType::Relaxed);
                        return Some(gpr_left);
                    }
                } else if expression_opt.is_some() && expression_rewrite_opt.is_none() {
                    if &gpr_left.change_type == &ChangeType::NoChange || &gpr_left.change_type == &ChangeType::Relaxed {
                        gpr_left.with_change_type(ChangeType::Relaxed);
                        return Some(gpr_left);
                    }
                }
            }
        } else if let Some(mut gpr_right) =
            right_rewrite_opt
        //left none, right some
        {
            if let Some(expression) = expression_opt {
                expression_rewrite_opt = self.rewrite_expression(
                    expression,
                    required_change_direction,
                    external_ids_in_scope,
                    &gpr_right.variables_in_scope,
                );
            }
            if let Some((expression_rewrite, expression_change)) = expression_rewrite_opt {
                if (expression_change == ChangeType::NoChange
                    || expression_change == ChangeType::Relaxed)
                    && (&gpr_right.change_type == &ChangeType::NoChange || &gpr_right.change_type == &ChangeType::Relaxed)
                {
                    gpr_right.with_graph_pattern(GraphPattern::Filter {
                            inner: Box::new(gpr_right.graph_pattern.take().unwrap()),
                            expr: expression_rewrite,
                        }).with_change_type(ChangeType::Relaxed);
                    return Some(gpr_right);
                }
            } else if expression_opt.is_some() && expression_rewrite_opt.is_none() {
                if &gpr_right.change_type == &ChangeType::NoChange || &gpr_right.change_type == &ChangeType::Relaxed {
                    gpr_right.with_change_type(ChangeType::Relaxed);
                    return Some(gpr_right);
                }
            }
        }
        None
    }

    fn rewrite_filter(
        & mut self,
        expression: &Expression,
        inner: & GraphPattern,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        let inner_rewrite_opt = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope,
        );
        self.pushdown_expression(expression);
        if let Some(mut gpr_inner) = inner_rewrite_opt {
            let expression_rewrite_opt = self.rewrite_expression(
                expression,
                required_change_direction,
                external_ids_in_scope,
                &gpr_inner.variables_in_scope,
            );
            if let Some((expression_rewrite, expression_change)) = expression_rewrite_opt {
                let use_change;
                if expression_change == ChangeType::NoChange {
                    use_change = gpr_inner.change_type.clone();
                } else if expression_change == ChangeType::Relaxed {
                    if &gpr_inner.change_type == &ChangeType::Relaxed || &gpr_inner.change_type == &ChangeType::NoChange {
                        use_change = ChangeType::Relaxed;
                    } else {
                        return None;
                    }
                } else if expression_change == ChangeType::Constrained {
                    if &gpr_inner.change_type == &ChangeType::Constrained {
                        use_change = ChangeType::Constrained;
                    } else {
                        return None;
                    }
                } else {
                    panic!("Should never happen");
                }
                gpr_inner.with_graph_pattern(GraphPattern::Filter {
                        expr: expression_rewrite,
                        inner: Box::new(gpr_inner.graph_pattern.take().unwrap()),
                    }).with_change_type(use_change);
                return Some(gpr_inner);
            } else {
                return Some(gpr_inner);
            }
        }
        None
    }

    fn rewrite_group(
        & mut self,
        graph_pattern: & GraphPattern,
        variables: & Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn>{
        let graph_pattern_rewrite_opt = self.rewrite_graph_pattern(
            graph_pattern,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope,
        );
        if let Some(mut gpr_inner) = graph_pattern_rewrite_opt
        {
            if gpr_inner.change_type == ChangeType::NoChange {
                let aggregates_rewrite = aggregates.iter().map(|(v, a)| {
                    (
                        self.rewrite_variable(v),
                        self.rewrite_aggregate_expression(
                            a,
                            &external_ids_in_scope,
                            &gpr_inner.variables_in_scope,
                        ),
                    )
                });
                let aggregates_rewritten: Vec<(Variable, AggregateExpression)> = aggregates_rewrite
                    .into_iter()
                    .filter(|(x, y)| x.is_some() && y.is_some())
                    .map(|(x, y)| (x.unwrap(), y.unwrap()))
                    .collect();
                let variables_rewritten: Vec<Variable> = variables
                    .iter()
                    .map(|v| self.rewrite_variable(v))
                    .filter(|x| x.is_some())
                    .map(|x| x.unwrap())
                    .collect();

                if variables_rewritten.len() == variables.len()
                    && aggregates_rewritten.len() == aggregates.len()
                {
                    for v in &variables_rewritten {
                        gpr_inner.variables_in_scope.insert(v.clone());
                    }
                    gpr_inner.with_graph_pattern(GraphPattern::Group {
                            inner: Box::new(gpr_inner.graph_pattern.take().unwrap()),
                            variables: variables_rewritten,
                            aggregates: vec![],
                        });
                    return Some(gpr_inner);
                } else {
                    //Todo: fix variable collisions here..
                    return Some(gpr_inner);
                }
            }
        }
        None
    }

    fn rewrite_aggregate_expression(
        &mut self,
        aggregate_expression: &AggregateExpression,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: &HashSet<Variable>,
    ) -> Option<AggregateExpression> {
        match aggregate_expression {
            AggregateExpression::Count { expr, distinct } => {
                if let Some(boxed_expression) = expr {
                    if let Some((expr_rewritten, ChangeType::NoChange)) = self.rewrite_expression(
                        boxed_expression,
                        &ChangeType::NoChange,
                        external_ids_in_scope,
                        variables_in_scope,
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self.rewrite_expression(
                    expr,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                ) {
                    Some(AggregateExpression::Sum {
                        expr: Box::new(rewritten_expression),
                        distinct: *distinct,
                    })
                } else {
                    None
                }
            }
            AggregateExpression::Avg { expr, distinct } => {
                if let Some((rewritten_expression, ChangeType::NoChange)) = self.rewrite_expression(
                    expr,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                ) {
                    Some(AggregateExpression::Avg {
                        expr: Box::new(rewritten_expression),
                        distinct: *distinct,
                    })
                } else {
                    None
                }
            }
            AggregateExpression::Min { expr, distinct } => {
                if let Some((rewritten_expression, ChangeType::NoChange)) = self.rewrite_expression(
                    expr,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                ) {
                    Some(AggregateExpression::Min {
                        expr: Box::new(rewritten_expression),
                        distinct: *distinct,
                    })
                } else {
                    None
                }
            }
            AggregateExpression::Max { expr, distinct } => {
                if let Some((rewritten_expression, ChangeType::NoChange)) = self.rewrite_expression(
                    expr,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                ) {
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self.rewrite_expression(
                    expr,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                ) {
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self.rewrite_expression(
                    expr,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                ) {
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
                if let Some((rewritten_expression, ChangeType::NoChange)) = self.rewrite_expression(
                    expr,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                ) {
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

    fn rewrite_distinct(
        & mut self,
        inner: & GraphPattern,

        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        if let Some(mut gpr_inner) = self
            .rewrite_graph_pattern(
                inner,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            )
        {
            gpr_inner.with_graph_pattern(GraphPattern::Distinct {
                    inner: Box::new(gpr_inner.graph_pattern.take().unwrap()),
                });
            Some(gpr_inner)
        } else {
            None
        }
    }

    fn rewrite_project(
        & mut self,
        inner: & GraphPattern,
        variables: & Vec<Variable>,

        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        if let Some(mut gpr_inner) = self
            .rewrite_graph_pattern(
                inner,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            )
        {
            let mut variables_rewrite = variables
                .iter()
                .map(|v| self.rewrite_variable(v))
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect::<Vec<Variable>>();
            let mut keys_sorted = external_ids_in_scope.keys().collect::<Vec<&Variable>>();
            keys_sorted.sort_by_key(|v| v.to_string());
            for k in keys_sorted {
                let vs = external_ids_in_scope.get(k).unwrap();
                let mut vars = vs.iter().collect::<Vec<&Variable>>();
                //Sort to make rewrites deterministic
                vars.sort_by_key(|v| v.to_string());
                for v in vars {
                    variables_rewrite.push(v.clone());
                }
            }
            let mut additional_projections_sorted = self
                .additional_projections
                .iter()
                .collect::<Vec<&Variable>>();
            additional_projections_sorted.sort_by_key(|x| x.to_string());
            for v in additional_projections_sorted {
                if !variables_rewrite.contains(v) {
                    variables_rewrite.push(v.clone());
                }
            }
            //Todo: redusere scope??
            if variables_rewrite.len() > 0 {
                gpr_inner.with_graph_pattern(GraphPattern::Project {
                        inner: Box::new(gpr_inner.graph_pattern.take().unwrap()),
                        variables: variables_rewrite,
                    });
                return Some(gpr_inner);
            }
        }
        None
    }

    fn rewrite_order_by(
        & mut self,
        inner: & GraphPattern,
        order_expressions: &Vec<OrderExpression>,

        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        if let Some(mut gpr_inner) = self
            .rewrite_graph_pattern(
                inner,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            )
        {
            let expressions_rewrite = order_expressions
                .iter()
                .map(|e| {
                    self.rewrite_order_expression(
                        e,
                        &external_ids_in_scope,
                        &gpr_inner.variables_in_scope,
                    )
                })
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect::<Vec<OrderExpression>>();
            if expressions_rewrite.len() > 0 {
                gpr_inner.with_graph_pattern(GraphPattern::OrderBy {
                        inner: Box::new(gpr_inner.graph_pattern.take().unwrap()),
                        expression: expressions_rewrite,
                    });
                return Some(gpr_inner);
            }
        }
        None
    }

    fn rewrite_order_expression(
        &mut self,
        order_expression: &OrderExpression,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: &HashSet<Variable>,
    ) -> Option<OrderExpression> {
        match order_expression {
            OrderExpression::Asc(e) => {
                if let Some((e_rewrite, ChangeType::NoChange)) = self.rewrite_expression(
                    e,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                ) {
                    Some(OrderExpression::Asc(e_rewrite))
                } else {
                    None
                }
            }
            OrderExpression::Desc(e) => {
                if let Some((e_rewrite, ChangeType::NoChange)) = self.rewrite_expression(
                    e,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                ) {
                    Some(OrderExpression::Desc(e_rewrite))
                } else {
                    None
                }
            }
        }
    }

    fn rewrite_minus(
        & mut self,
        left: & GraphPattern,
        right: & GraphPattern,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        let left_rewrite_opt = self.rewrite_graph_pattern(
            left,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope.clone(),
        );
        let right_rewrite_opt = self.rewrite_graph_pattern(
            right,
            &required_change_direction.opposite(),
            external_ids_in_scope,
            variables_in_scope,
        );

        if let Some(mut gpr_left) = left_rewrite_opt {
            if let Some(mut gpr_right) = right_rewrite_opt {
                if &gpr_left.change_type == &ChangeType::NoChange && &gpr_right.change_type == &ChangeType::NoChange {
                    gpr_left.with_graph_pattern(GraphPattern::Minus {
                            left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                            right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                        });
                    return Some(gpr_left);
                } else if (&gpr_left.change_type == &ChangeType::Relaxed
                    || &gpr_left.change_type == &ChangeType::NoChange)
                    && (&gpr_right.change_type == &ChangeType::Constrained
                        || &gpr_right.change_type == &ChangeType::NoChange)
                {
                    gpr_left.with_graph_pattern(GraphPattern::Minus {
                            left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                            right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                        }).with_change_type(ChangeType::Relaxed);
                    return Some(gpr_left);
                } else if (&gpr_left.change_type == &ChangeType::Constrained
                    || &gpr_left.change_type == &ChangeType::NoChange)
                    && (&gpr_right.change_type == &ChangeType::Relaxed || &gpr_right.change_type == &ChangeType::NoChange)
                {
                    gpr_left.with_graph_pattern(GraphPattern::Minus {
                            left: Box::new(gpr_left.graph_pattern.take().unwrap()),
                            right: Box::new(gpr_right.graph_pattern.take().unwrap()),
                        }).with_change_type(ChangeType::Constrained);
                    return Some(gpr_left);
                }
            } else {
                //left some, right none
                if &gpr_left.change_type == &ChangeType::NoChange || &gpr_left.change_type == &ChangeType::Relaxed {
                    gpr_left.with_change_type(ChangeType::Relaxed);
                    return Some(gpr_left);
                }
            }
        }
        None
    }

    fn rewrite_bgp(
        &mut self,
        patterns: & Vec<TriplePattern>,

        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: &HashSet<Variable>,
    ) -> Option<GPReturn> {
        let mut new_triples = vec![];
        let mut dynamic_triples = vec![];
        for t in patterns {
            //If the object is an external timeseries, we need to do get the external id
            if let TermPattern::Variable(object_var) = &t.object {
                let obj_constr_opt = self.has_constraint.get(object_var).cloned();
                if let Some(obj_constr) = &obj_constr_opt {
                    if obj_constr == &Constraint::ExternalTimeseries {
                        if !external_ids_in_scope.contains_key(object_var) {
                            let external_id_var = Variable::new(
                                "ts_external_id_".to_string() + &self.variable_counter.to_string(),
                            )
                            .unwrap();
                            self.variable_counter += 1;
                            self.create_time_series_query(&object_var, &external_id_var);
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
                                .insert(object_var.clone(), vec![external_id_var.clone()]);
                        }
                    }
                }
            }

            fn is_external_variable(
                term_pattern: &TermPattern,
                has_constraint: &HashMap<Variable, Constraint>,
            ) -> bool {
                if let TermPattern::Variable(var) = term_pattern {
                    if let Some(ctr) = has_constraint.get(var) {
                        if ctr == &Constraint::ExternalDataPoint
                            || ctr == &Constraint::ExternalTimestamp
                            || ctr == &Constraint::ExternalDataValue
                        {
                            return true;
                        }
                    }
                }
                false
            }

            if !is_external_variable(&t.subject, &self.has_constraint)
                && !is_external_variable(&t.object, &self.has_constraint)
            {
                if !new_triples.contains(t) {
                    new_triples.push(t.clone());
                }
            } else {
                dynamic_triples.push(t)
            }
        }

        //We wait until last to process the dynamic triples, making sure all relationships are known first.
        self.process_dynamic_triples(dynamic_triples);

        if new_triples.is_empty() {
            debug!("New triples in static BGP was empty, returning None");
            None
        } else {
            for t in &new_triples {
                if let TermPattern::Variable(v) = &t.subject {
                    variables_in_scope.insert(v);
                }
                if let TermPattern::Variable(v) = &t.object {
                    variables_in_scope.insert(v);
                }
            }
            todo!("Change type fix!");
            GPReturn::new(GraphPattern::Bgp {
                    patterns: new_triples,
                }, ChangeType::Relaxed, vec![], Default::default(), Default::default())
            Some((

                ChangeType::NoChange,
                variables_in_scope,
            ))
        }
    }

    //We assume that all paths have been rewritten so as to not contain any datapoint, timestamp, or data value.
    //These should have been split into ordinary triples.
    fn rewrite_path(
        &mut self,
        subject: & TermPattern,
        path: &PropertyPathExpression,
        object: & TermPattern,
        mut variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        if let TermPattern::Variable(s) = subject {
            variables_in_scope.insert(s);
        }
        if let TermPattern::Variable(o) = object {
            variables_in_scope.insert(o);
        }
        return Some((
            GraphPattern::Path {
                subject: subject.clone(),
                path: path.clone(),
                object: object.clone(),
            },
            ChangeType::NoChange,
            variables_in_scope,
        ));
    }

    fn rewrite_expression(
        &mut self,
        expression: &Expression,
        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: &HashSet<Variable>,
    ) -> Option<(Expression, ChangeType)> {
        match expression {
            Expression::NamedNode(nn) => {
                Some((Expression::NamedNode(nn.clone()), ChangeType::NoChange))
            }
            Expression::Literal(l) => Some((Expression::Literal(l.clone()), ChangeType::NoChange)),
            Expression::Variable(v) => {
                if let Some(rewritten_variable) = self.rewrite_variable(v) {
                    Some((
                        Expression::Variable(rewritten_variable),
                        ChangeType::NoChange,
                    ))
                } else {
                    None
                }
            }
            Expression::Or(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    required_change_direction,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    required_change_direction,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let Some((left_rewrite, left_change)) = left_rewrite_opt {
                    if let Some((right_rewrite, right_change)) = right_rewrite_opt {
                        if left_change == ChangeType::NoChange && right_change == ChangeType::NoChange
                        {
                            return Some((
                                Expression::Or(
                                    Box::new(left_rewrite),
                                    Box::new(right_rewrite),
                                ),
                                ChangeType::NoChange,
                            ));
                        }
                    }
                } else {
                    match required_change_direction {
                        ChangeType::Relaxed => {
                            if let (
                                Some((left_rewrite, left_change)),
                                Some((right_rewrite, right_change)),
                            ) = (left_rewrite_opt, right_rewrite_opt)
                            {
                                if (left_change == ChangeType::NoChange
                                    || left_change == ChangeType::Relaxed)
                                    && (right_change == ChangeType::NoChange
                                    || right_change == ChangeType::Relaxed)
                                {
                                    return Some((
                                        Expression::Or(Box::new(left_rewrite), Box::new(right_rewrite)),
                                        ChangeType::Relaxed,
                                    ));
                                }
                            }
                        }
                        ChangeType::Constrained => {
                            if let Some((left_rewrite, left_change)) = left_rewrite_opt {
                                if let Some((right_rewrite, right_change)) = right_rewrite_opt {
                                    if (left_change == ChangeType::NoChange
                                        || left_change == ChangeType::Constrained)
                                        && (right_change == ChangeType::NoChange
                                        || right_change == ChangeType::Constrained)
                                    {
                                        return Some((
                                            Expression::Or(
                                                Box::new(left_rewrite),
                                                Box::new(right_rewrite),
                                            ),
                                            ChangeType::Constrained,
                                        ));
                                    }
                                } else {
                                    //left some
                                    if left_change == ChangeType::Constrained
                                        || left_change == ChangeType::NoChange
                                    {
                                        return Some((left_rewrite, ChangeType::Constrained));
                                    }
                                }
                            } else if let Some((right_rewrite, right_change)) = right_rewrite_opt {
                                if right_change == ChangeType::Constrained
                                    || right_change == ChangeType::NoChange
                                {
                                    return Some((right_rewrite, ChangeType::Constrained));
                                }
                            }
                        }
                        ChangeType::NoChange => {}
                    }

                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }

            Expression::And(left, right) => {
                // We allow translations of left- or right hand sides of And-expressions to be None.
                // This allows us to enforce the remaining conditions that were not removed due to a rewrite
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    required_change_direction,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    required_change_direction,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let Some((left_rewrite, left_change)) = left_rewrite_opt {
                    if let Some((right_rewrite, right_change)) = right_rewrite_opt {
                        if left_change == ChangeType::NoChange
                            || right_change == ChangeType::NoChange
                        {
                            return Some((
                                Expression::And(Box::new(left_rewrite), Box::new(right_rewrite)),
                                ChangeType::NoChange,
                            ));
                        }
                    }
                } else {
                    match required_change_direction {
                        ChangeType::Relaxed => {
                            if let Some((left_rewrite, left_change)) = left_rewrite_opt {
                                if let Some((right_rewrite, right_change)) = right_rewrite_opt {
                                    if (left_change == ChangeType::NoChange
                                        || left_change == ChangeType::Relaxed)
                                        && (right_change == ChangeType::NoChange
                                            || right_change == ChangeType::Relaxed)
                                    {
                                        return Some((
                                            Expression::And(
                                                Box::new(left_rewrite),
                                                Box::new(right_rewrite),
                                            ),
                                            ChangeType::Relaxed,
                                        ));
                                    }
                                } else {
                                    // left some, right none
                                    if left_change == ChangeType::Relaxed
                                        || left_change == ChangeType::NoChange
                                    {
                                        return Some((left_rewrite, ChangeType::Relaxed));
                                    }
                                }
                            } else if let Some((right_rewrite, right_change)) = right_rewrite_opt {
                                if right_change == ChangeType::Relaxed
                                    || right_change == ChangeType::NoChange
                                {
                                    return Some((right_rewrite, ChangeType::Relaxed));
                                }
                            }
                        }
                        ChangeType::Constrained => {
                            if let (
                                Some((left_rewrite, left_change)),
                                Some((right_rewrite, right_change)),
                            ) = (left_rewrite_opt, right_rewrite_opt)
                            {
                                if (left_change == ChangeType::NoChange
                                    || left_change == ChangeType::Constrained)
                                    && (right_change == ChangeType::NoChange
                                        || right_change == ChangeType::Constrained)
                                {
                                    return Some((
                                        Expression::And(
                                            Box::new(left_rewrite),
                                            Box::new(right_rewrite),
                                        ),
                                        ChangeType::Constrained,
                                    ));
                                }
                            }
                        }
                        ChangeType::NoChange => {}
                    }
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::Equal(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Equal(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::SameTerm(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::SameTerm(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::Greater(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let Some((left_rewrite, ChangeType::NoChange)) = left_rewrite_opt {
                    if let Some((right_rewrite, ChangeType::NoChange)) = right_rewrite_opt {
                        return Some((
                            Expression::Greater(Box::new(left_rewrite), Box::new(right_rewrite)),
                            ChangeType::NoChange,
                        ));
                    }
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::GreaterOrEqual(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::Less(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Less(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::LessOrEqual(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::LessOrEqual(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::In(left, expressions) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let expressions_rewritten_opts = expressions
                    .iter()
                    .map(|e| {
                        self.rewrite_expression(
                            e,
                            &ChangeType::NoChange,
                            external_ids_in_scope,
                            variables_in_scope,
                        )
                    })
                    .collect::<Vec<Option<(Expression, ChangeType)>>>();
                if expressions_rewritten_opts.iter().any(|x| x.is_none()) {
                    return None;
                }
                let expressions_rewritten = expressions_rewritten_opts
                    .iter()
                    .map(|x| x.clone())
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
                self.project_all_dynamic_variables(vec![left_rewrite_opt]);
                self.project_all_dynamic_variables(expressions_rewritten_opts);
                None
            }
            Expression::Add(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );

                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Add(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::Subtract(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Subtract(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::Multiply(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Multiply(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::Divide(left, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::Divide(Box::new(left_rewrite), Box::new(right_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![left_rewrite_opt, right_rewrite_opt]);
                None
            }
            Expression::UnaryPlus(wrapped) => {
                let wrapped_rewrite_opt = self.rewrite_expression(
                    wrapped,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let Some((wrapped_rewrite, ChangeType::NoChange)) = wrapped_rewrite_opt {
                    return Some((
                        Expression::UnaryPlus(Box::new(wrapped_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![wrapped_rewrite_opt]);
                None
            }
            Expression::UnaryMinus(wrapped) => {
                let wrapped_rewrite_opt = self.rewrite_expression(
                    wrapped,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let Some((wrapped_rewrite, ChangeType::NoChange)) = wrapped_rewrite_opt {
                    return Some((
                        Expression::UnaryPlus(Box::new(wrapped_rewrite)),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![wrapped_rewrite_opt]);
                None
            }
            Expression::Not(wrapped) => {
                let wrapped_rewrite_opt = self.rewrite_expression(
                    wrapped,
                    &required_change_direction.opposite(),
                    external_ids_in_scope,
                    variables_in_scope,
                );
                if let Some((wrapped_rewrite, wrapped_change)) = wrapped_rewrite_opt {
                    let use_change_type = match wrapped_change {
                        ChangeType::NoChange => ChangeType::NoChange,
                        ChangeType::Relaxed => ChangeType::Constrained,
                        ChangeType::Constrained => ChangeType::Relaxed,
                    };
                    if use_change_type == ChangeType::NoChange
                        || &use_change_type == required_change_direction
                    {
                        return Some((Expression::Not(Box::new(wrapped_rewrite)), use_change_type));
                    }
                }
                self.project_all_dynamic_variables(vec![wrapped_rewrite_opt]);
                None
            }
            Expression::Exists(wrapped) => {
                let wrapped_rewrite_opt = self.rewrite_graph_pattern(
                    &wrapped,
                    required_change_direction,
                    &mut external_ids_in_scope.clone(),
                    variables_in_scope.clone(),
                );
                if let Some((wrapped_rewrite, wrapped_change, _)) = wrapped_rewrite_opt {
                    let use_change = match wrapped_change {
                        ChangeType::Relaxed => ChangeType::Relaxed,
                        ChangeType::Constrained => ChangeType::Constrained,
                        ChangeType::NoChange => ChangeType::NoChange,
                    };
                    return Some((Expression::Exists(Box::new(wrapped_rewrite)), use_change));
                }
                None
            }
            Expression::Bound(v) => {
                if let Some(v_rewritten) = self.rewrite_variable(v) {
                    Some((Expression::Bound(v_rewritten), ChangeType::NoChange))
                } else {
                    None
                }
            }
            Expression::If(left, mid, right) => {
                let left_rewrite_opt = self.rewrite_expression(
                    left,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let mid_rewrite_opt = self.rewrite_expression(
                    mid,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );
                let right_rewrite_opt = self.rewrite_expression(
                    right,
                    &ChangeType::NoChange,
                    external_ids_in_scope,
                    variables_in_scope,
                );

                if let (
                    Some((left_rewrite, ChangeType::NoChange)),
                    Some((mid_rewrite, ChangeType::NoChange)),
                    Some((right_rewrite, ChangeType::NoChange)),
                ) = (left_rewrite_opt, mid_rewrite_opt, right_rewrite_opt)
                {
                    return Some((
                        Expression::If(
                            Box::new(left_rewrite),
                            Box::new(mid_rewrite),
                            Box::new(right_rewrite),
                        ),
                        ChangeType::NoChange,
                    ));
                }
                self.project_all_dynamic_variables(vec![
                    left_rewrite_opt,
                    mid_rewrite_opt,
                    right_rewrite_opt,
                ]);
                None
            }
            Expression::Coalesce(wrapped) => {
                let rewritten = wrapped
                    .iter()
                    .map(|e| {
                        self.rewrite_expression(
                            e,
                            &ChangeType::NoChange,
                            external_ids_in_scope,
                            variables_in_scope,
                        )
                    })
                    .collect::<Vec<Option<(Expression, ChangeType)>>>();
                if rewritten.iter().all(|x| x.is_some()) {
                    let rewritten_some = &rewritten
                        .iter()
                        .map(|x| x.clone())
                        .map(|x| x.unwrap())
                        .collect::<Vec<(Expression, ChangeType)>>();
                    if rewritten_some
                        .iter()
                        .all(|(_, c)| c == &ChangeType::NoChange)
                    {
                        return Some((
                            Expression::Coalesce(
                                rewritten_some.into_iter().map(|(e, _)| e.clone()).collect(),
                            ),
                            ChangeType::NoChange,
                        ));
                    }
                }
                self.project_all_dynamic_variables(rewritten);
                None
            }
            Expression::FunctionCall(fun, args) => {
                let args_rewritten = args
                    .iter()
                    .map(|e| {
                        self.rewrite_expression(
                            e,
                            &ChangeType::NoChange,
                            external_ids_in_scope,
                            variables_in_scope,
                        )
                    })
                    .collect::<Vec<Option<(Expression, ChangeType)>>>();
                if args_rewritten.iter().all(|x| x.is_some()) {
                    let args_rewritten_some = &args_rewritten
                        .iter()
                        .map(|x| x.clone())
                        .map(|x| x.unwrap())
                        .collect::<Vec<(Expression, ChangeType)>>();
                    if args_rewritten_some
                        .iter()
                        .all(|(_, c)| c == &ChangeType::NoChange)
                    {
                        return Some((
                            Expression::FunctionCall(
                                fun.clone(),
                                args_rewritten_some.iter().map(|(e, _)| e.clone()).collect(),
                            ),
                            ChangeType::NoChange,
                        ));
                    }
                }
                self.project_all_dynamic_variables(args_rewritten);
                None
            }
        }
    }

    fn project_all_dynamic_variables(&mut self, rewrites: Vec<Option<(Expression, ChangeType)>>) {
        for r in rewrites {
            if let Some((expr, _)) = r {
                self.project_all_static_variables_from_expression(&expr);
            }
        }
    }

    fn rewrite_extend(
        & mut self,
        inner: & GraphPattern,
        var: & Variable,
        expr: &Expression,

        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        let inner_rewrite_opt = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope,
        );

        if let Some(mut gpr_inner) =
            inner_rewrite_opt
        {
            let expr_rewrite_opt = self.rewrite_expression(
                expr,
                &ChangeType::NoChange,
                external_ids_in_scope,
                &gpr_inner.variables_in_scope,
            );
            if let Some((expression_rewrite, _)) = expr_rewrite_opt {
                gpr_inner.variables_in_scope.insert(var.clone());
                gpr_inner.with_graph_pattern(GraphPattern::Extend {
                        inner: Box::new(gpr_inner.graph_pattern.take().unwrap()),
                        variable: var.clone(),
                        expression: expression_rewrite,
                    });
                return Some(gpr_inner);
            } else {
                return Some(gpr_inner);
            }
        }
        None
    }

    fn rewrite_slice(
        & mut self,
        inner: & GraphPattern,
        start: &usize,
        length: &Option<usize>,

        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        let rewrite_inner_opt = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            external_ids_in_scope,
            variables_in_scope,
        );
        if let Some((rewrite_inner, rewrite_change, inner_variables_in_scope)) = rewrite_inner_opt {
            return Some((
                GraphPattern::Slice {
                    inner: Box::new(rewrite_inner),
                    start: start.clone(),
                    length: length.clone(),
                },
                rewrite_change,
                inner_variables_in_scope,
            ));
        }
        None
    }

    fn rewrite_reduced(
        & mut self,
        inner: & GraphPattern,

        required_change_direction: &ChangeType,
        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn> {
        if let Some(mut gpr_inner) = self
            .rewrite_graph_pattern(
                inner,
                required_change_direction,
                external_ids_in_scope,
                variables_in_scope,
            )
        {
            gpr_inner.with_graph_pattern(GraphPattern::Reduced {
                    inner: Box::new(gpr_inner.graph_pattern.take().unwrap()),
                });
            return Some(gpr_inner);
        }
        None
    }

    fn rewrite_service(
        & mut self,
        name: &NamedNodePattern,
        inner: & GraphPattern,
        silent: &bool,

        external_ids_in_scope: &HashMap<Variable, Vec<Variable>>,
        variables_in_scope: HashSet<& Variable>,
    ) -> Option<GPReturn>{
        if let Some(mut gpr_inner) = self
            .rewrite_graph_pattern(
                inner,
                &ChangeType::NoChange,
                external_ids_in_scope,
                variables_in_scope,
            )
        {
            gpr_inner.with_graph_pattern(GraphPattern::Service {
                    name: name.clone(),
                    inner: Box::new(gpr_inner.graph_pattern.take().unwrap()),
                    silent: silent.clone(),
                });
            return Some(gpr_inner);
        }
        None
    }

    fn project_all_static_variables_from_expression(&mut self, expr: &Expression) {
        match expr {
            Expression::Variable(var) => {
                self.project_variable_if_static(var);
            }
            Expression::Or(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::And(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::Equal(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::SameTerm(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::Greater(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::GreaterOrEqual(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::Less(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::LessOrEqual(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::In(expr, expressions) => {
                self.project_all_static_variables_from_expression(expr);
                for e in expressions {
                    self.project_all_static_variables_from_expression(e);
                }
            }
            Expression::Add(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::Subtract(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::Multiply(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::Divide(left, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::UnaryPlus(expr) => {
                self.project_all_static_variables_from_expression(expr);
            }
            Expression::UnaryMinus(expr) => {
                self.project_all_static_variables_from_expression(expr);
            }
            Expression::Not(expr) => {
                self.project_all_static_variables_from_expression(expr);
            }
            Expression::Exists(_) => {
                todo!("Fix handling..")
            }
            Expression::Bound(var) => {
                self.project_variable_if_static(var);
            }
            Expression::If(left, middle, right) => {
                self.project_all_static_variables_from_expression(left);
                self.project_all_static_variables_from_expression(middle);
                self.project_all_static_variables_from_expression(right);
            }
            Expression::Coalesce(expressions) => {
                for e in expressions {
                    self.project_all_static_variables_from_expression(e);
                }
            }
            Expression::FunctionCall(_, expressions) => {
                for e in expressions {
                    self.project_all_static_variables_from_expression(e);
                }
            }
            _ => {}
        }
    }

    fn project_variable_if_static(&mut self, variable: &Variable) {
        if !self.has_constraint.contains_key(variable) {
            self.additional_projections.insert(variable.clone());
        }
    }

    fn rewrite_variable(&self, v: &Variable) -> Option<Variable> {
        if let Some(ctr) = self.has_constraint.get(v) {
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

    fn pushdown_expression(&mut self, expr: &Expression) {
        for t in &mut self.time_series_queries {
            t.try_rewrite_expression(expr);
        }
    }

    fn process_dynamic_triples(&mut self, dynamic_triples: Vec<&TriplePattern>) {
        for t in &dynamic_triples {
            if let NamedNodePattern::NamedNode(named_predicate_node) = &t.predicate {
                if named_predicate_node == HAS_DATA_POINT {
                    for q in &mut self.time_series_queries {
                        if let (
                            Some(q_timeseries_variable),
                            TermPattern::Variable(subject_variable),
                        ) = (&q.timeseries_variable, &t.subject)
                        {
                            if subject_variable == q_timeseries_variable {
                                if let TermPattern::Variable(ts_var) = &t.object {
                                    q.data_point_variable = Some(ts_var.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        for t in &dynamic_triples {
            if let NamedNodePattern::NamedNode(named_predicate_node) = &t.predicate {
                if named_predicate_node == HAS_VALUE {
                    for q in &mut self.time_series_queries {
                        if let (
                            Some(q_data_point_variable),
                            TermPattern::Variable(subject_variable),
                        ) = (&q.data_point_variable, &t.subject)
                        {
                            if subject_variable == q_data_point_variable {
                                if let TermPattern::Variable(value_var) = &t.object {
                                    q.value_variable = Some(value_var.clone());
                                }
                            }
                        }
                    }
                } else if named_predicate_node == HAS_TIMESTAMP {
                    for q in &mut self.time_series_queries {
                        if let (
                            Some(q_data_point_variable),
                            TermPattern::Variable(subject_variable),
                        ) = (&q.data_point_variable, &t.subject)
                        {
                            if subject_variable == q_data_point_variable {
                                if let TermPattern::Variable(timestamp_var) = &t.object {
                                    q.timestamp_variable = Some(timestamp_var.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn create_time_series_query(
        &mut self,
        time_series_variable: &Variable,
        time_series_id_variable: &Variable,
    ) {
        let mut ts_query = TimeSeriesQuery::new();
        ts_query.identifier_variable = Some(time_series_id_variable.clone());
        ts_query.timeseries_variable = Some(time_series_variable.clone());
        self.time_series_queries.push(ts_query);
    }

    fn find_functions_of_timestamps(
        &self,
        graph_pattern: &GraphPattern,
    ) -> Vec<(Variable, GraphPattern)> {
        println!("{:?}", graph_pattern);
        todo!()
    }
}

fn merge_external_variables_in_scope(
    src: HashMap<Variable, Vec<Variable>>,
    trg: &mut HashMap<Variable, Vec<Variable>>,
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

pub(crate) fn hash_graph_pattern(graph_pattern: &GraphPattern) -> u64 {
    let mut hasher = DefaultHasher::new();
    graph_pattern.hash(&mut hasher);
    hasher.finish()
}

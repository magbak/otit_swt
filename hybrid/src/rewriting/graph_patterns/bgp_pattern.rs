use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::constants::HAS_EXTERNAL_ID;
use crate::constraints::{Constraint, VariableConstraints};
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use log::debug;
use oxrdf::{NamedNode, Variable};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

impl StaticQueryRewriter {
    pub(crate) fn rewrite_bgp(
        &mut self,
        patterns: &Vec<TriplePattern>,
        context: &Context,
    ) -> Option<GPReturn> {
        let context = context.extension_with(PathEntry::BGP);
        let mut new_triples = vec![];
        let mut dynamic_triples = vec![];
        let mut external_ids_in_scope = HashMap::new();
        for t in patterns {
            //If the object is an external timeseries, we need to do get the external id
            if let TermPattern::Variable(object_var) = &t.object {
                let obj_constr_opt = self
                    .variable_constraints
                    .get_constraint(object_var, &context)
                    .cloned();
                if let Some(obj_constr) = &obj_constr_opt {
                    if obj_constr == &Constraint::ExternalTimeseries {
                        if !external_ids_in_scope.contains_key(object_var) {
                            let external_id_var = Variable::new(
                                "ts_external_id_".to_string() + &self.variable_counter.to_string(),
                            )
                            .unwrap();
                            self.variable_counter += 1;
                            self.create_time_series_query(&object_var, &external_id_var, &context);
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
                context: &Context,
                variable_constraints: &VariableConstraints,
            ) -> bool {
                if let TermPattern::Variable(var) = term_pattern {
                    if let Some(ctr) = variable_constraints.get_constraint(var, context) {
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

            if !is_external_variable(&t.subject, &context, &self.variable_constraints)
                && !is_external_variable(&t.object, &context, &self.variable_constraints)
            {
                if !new_triples.contains(t) {
                    new_triples.push(t.clone());
                }
            } else {
                dynamic_triples.push(t)
            }
        }

        let use_change_type;
        if dynamic_triples.len() > 0 {
            use_change_type = ChangeType::Relaxed;
        } else {
            use_change_type = ChangeType::NoChange;
        }

        //We wait until last to process the dynamic triples, making sure all relationships are known first.
        self.process_dynamic_triples(dynamic_triples, &context);

        if new_triples.is_empty() {
            debug!("New triples in static BGP was empty, returning None");
            None
        } else {
            let mut variables_in_scope = HashSet::new();
            for t in &new_triples {
                if let TermPattern::Variable(v) = &t.subject {
                    variables_in_scope.insert(v.clone());
                }
                if let TermPattern::Variable(v) = &t.object {
                    variables_in_scope.insert(v.clone());
                }
            }

            let gpr = GPReturn::new(
                GraphPattern::Bgp {
                    patterns: new_triples,
                },
                use_change_type,
                variables_in_scope,
                external_ids_in_scope,
            );
            Some(gpr)
        }
    }
}

use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::aggregate_expression::AEReturn;
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::pushups::apply_pushups;
use oxrdf::Variable;
use spargebra::algebra::{AggregateExpression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_group(
        &mut self,
        graph_pattern: &GraphPattern,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> GPReturn {
        let mut graph_pattern_rewrite = self.rewrite_graph_pattern(
            graph_pattern,
            required_change_direction,
            &context.extension_with(PathEntry::GroupInner),
        );
        if graph_pattern_rewrite.graph_pattern.is_some() {
            if graph_pattern_rewrite.change_type == ChangeType::NoChange {
                let variables_rewritten: Vec<Option<Variable>> = variables
                    .iter()
                    .map(|v| self.rewrite_variable(v, context))
                    .collect();

                let mut aes_rewritten: Vec<(Option<Variable>, AEReturn)> = aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, (v, a))| {
                        (
                            self.rewrite_variable(v, context),
                            self.rewrite_aggregate_expression(
                                a,
                                &graph_pattern_rewrite.variables_in_scope,
                                &context.extension_with(PathEntry::GroupAggregation(i as u16)),
                            ),
                        )
                    })
                    .collect();
                if variables_rewritten.iter().all(|v| v.is_some())
                    && aes_rewritten
                        .iter()
                        .all(|(v, a)| v.is_some() && a.aggregate_expression.is_some())
                {
                    for v in &variables_rewritten {
                        graph_pattern_rewrite
                            .variables_in_scope
                            .insert(v.as_ref().unwrap().clone());
                    }
                    let mut inner_graph_pattern =
                        graph_pattern_rewrite.graph_pattern.take().unwrap();
                    for (_, aes) in aes_rewritten.iter_mut() {
                        inner_graph_pattern =
                            apply_pushups(inner_graph_pattern, &mut aes.graph_pattern_pushups);
                    }
                    graph_pattern_rewrite.with_graph_pattern(GraphPattern::Group {
                        inner: Box::new(inner_graph_pattern),
                        variables: variables_rewritten
                            .into_iter()
                            .map(|v| v.unwrap())
                            .collect(),
                        aggregates: vec![],
                    });
                    return graph_pattern_rewrite;
                }
            } else {
                //TODO: Possible problem with pushups here.
                return graph_pattern_rewrite;
            }
        }
        return GPReturn::none();
    }
}

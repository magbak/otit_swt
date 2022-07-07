use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<GPReturn> {
        if let Some(mut gpr_inner) = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::ProjectInner),
        ) {
            let mut variables_rewrite = variables
                .iter()
                .map(|v| self.rewrite_variable(v, context))
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect::<Vec<Variable>>();
            let mut keys_sorted = gpr_inner
                .external_ids_in_scope
                .keys()
                .collect::<Vec<&Variable>>();
            keys_sorted.sort_by_key(|v| v.to_string());
            for k in keys_sorted {
                let vs = gpr_inner.external_ids_in_scope.get(k).unwrap();
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
                let inner_graph_pattern = gpr_inner.graph_pattern.take().unwrap();
                gpr_inner.with_graph_pattern(GraphPattern::Project {
                    inner: Box::new(inner_graph_pattern),
                    variables: variables_rewrite,
                });
                return Some(gpr_inner);
            }
        }
        None
    }
}

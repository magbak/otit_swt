use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<GPReturn> {
        let left_rewrite_opt = self.rewrite_graph_pattern(
            left,
            required_change_direction,
            &context.extension_with(PathEntry::MinusLeftSide),
        );
        let right_rewrite_opt = self.rewrite_graph_pattern(
            right,
            &required_change_direction.opposite(),
            &context.extension_with(PathEntry::MinusRightSide),
        );

        if let Some(mut gpr_left) = left_rewrite_opt {
            if let Some(mut gpr_right) = right_rewrite_opt {
                if &gpr_left.change_type == &ChangeType::NoChange
                    && &gpr_right.change_type == &ChangeType::NoChange
                {
                    let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                    let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                    gpr_left.with_graph_pattern(GraphPattern::Minus {
                        left: Box::new(left_graph_pattern),
                        right: Box::new(right_graph_pattern),
                    });
                    return Some(gpr_left);
                } else if (&gpr_left.change_type == &ChangeType::Relaxed
                    || &gpr_left.change_type == &ChangeType::NoChange)
                    && (&gpr_right.change_type == &ChangeType::Constrained
                        || &gpr_right.change_type == &ChangeType::NoChange)
                {
                    let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                    let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                    gpr_left
                        .with_graph_pattern(GraphPattern::Minus {
                            left: Box::new(left_graph_pattern),
                            right: Box::new(right_graph_pattern),
                        })
                        .with_change_type(ChangeType::Relaxed);
                    return Some(gpr_left);
                } else if (&gpr_left.change_type == &ChangeType::Constrained
                    || &gpr_left.change_type == &ChangeType::NoChange)
                    && (&gpr_right.change_type == &ChangeType::Relaxed
                        || &gpr_right.change_type == &ChangeType::NoChange)
                {
                    let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                    let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                    gpr_left
                        .with_graph_pattern(GraphPattern::Minus {
                            left: Box::new(left_graph_pattern),
                            right: Box::new(right_graph_pattern),
                        })
                        .with_change_type(ChangeType::Constrained);
                    return Some(gpr_left);
                }
            } else {
                //left some, right none
                if &gpr_left.change_type == &ChangeType::NoChange
                    || &gpr_left.change_type == &ChangeType::Relaxed
                {
                    gpr_left.with_change_type(ChangeType::Relaxed);
                    return Some(gpr_left);
                }
            }
        }
        None
    }
}

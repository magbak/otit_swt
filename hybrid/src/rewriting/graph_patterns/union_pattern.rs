use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_union(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> Option<GPReturn> {
        let left_rewrite_opt = self.rewrite_graph_pattern(
            left,
            required_change_direction,
            &context.extension_with(PathEntry::UnionLeftSide),
        );
        let right_rewrite_opt = self.rewrite_graph_pattern(
            right,
            required_change_direction,
            &context.extension_with(PathEntry::UnionRightSide),
        );

        match required_change_direction {
            ChangeType::Relaxed => {
                if let Some(mut gpr_left) = left_rewrite_opt {
                    if let Some(mut gpr_right) = right_rewrite_opt {
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
                        let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                        let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                        gpr_left
                            .with_scope(&mut gpr_right)
                            .with_graph_pattern(GraphPattern::Union {
                                left: Box::new(left_graph_pattern),
                                right: Box::new(right_graph_pattern),
                            })
                            .with_change_type(use_change);
                        return Some(gpr_left);
                    } else {
                        //left is some, right is none
                        if &gpr_left.change_type == &ChangeType::Relaxed
                            || &gpr_left.change_type == &ChangeType::NoChange
                        {
                            return Some(gpr_left);
                        }
                    }
                } else if let Some(gpr_right) = right_rewrite_opt {
                    //left is none, right is some
                    if &gpr_right.change_type == &ChangeType::Relaxed
                        || &gpr_right.change_type == &ChangeType::NoChange
                    {
                        return Some(gpr_right);
                    }
                }
            }
            ChangeType::Constrained => {
                if let Some(mut gpr_left) = left_rewrite_opt {
                    if let Some(mut gpr_right) = right_rewrite_opt {
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
                        let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                        let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                        gpr_left
                            .with_scope(&mut gpr_right)
                            .with_graph_pattern(GraphPattern::Union {
                                left: Box::new(left_graph_pattern),
                                right: Box::new(right_graph_pattern),
                            })
                            .with_change_type(use_change);

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
                if let Some(gpr_right) = right_rewrite_opt {
                    // left none
                    if &gpr_right.change_type == &ChangeType::Constrained
                        || &gpr_right.change_type == &ChangeType::NoChange
                    {
                        return Some(gpr_right);
                    }
                }
            }
            ChangeType::NoChange => {
                if let Some(mut gpr_left) = left_rewrite_opt {
                    if let Some(mut gpr_right) = right_rewrite_opt {
                        if &gpr_left.change_type == &ChangeType::NoChange
                            && &gpr_right.change_type == &ChangeType::NoChange
                        {
                            let left_graph_pattern = gpr_left.graph_pattern.take().unwrap();
                            let right_graph_pattern = gpr_right.graph_pattern.take().unwrap();
                            gpr_left.with_scope(&mut gpr_right).with_graph_pattern(
                                GraphPattern::Union {
                                    left: Box::new(left_graph_pattern),
                                    right: Box::new(right_graph_pattern),
                                },
                            );
                            return Some(gpr_left);
                        }
                    } else {
                        //right none
                        if &gpr_left.change_type == &ChangeType::NoChange {
                            return Some(gpr_left);
                        }
                    }
                } else if let Some(gpr_right) = right_rewrite_opt {
                    if &gpr_right.change_type == &ChangeType::NoChange {
                        return Some(gpr_right);
                    }
                }
            }
        }
        None
    }
}

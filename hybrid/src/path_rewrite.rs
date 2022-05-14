use crate::const_uris::{HAS_DATA_POINT, HAS_TIMESERIES, HAS_VALUE};
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};

pub fn rewrite_path_graph_pattern(subject: Option<&TermPattern>,
    path: &PropertyPathExpression,
    object: Option<&TermPattern>) -> GraphPattern {
    let mut triples = vec![];
    let (subject_opt, path_opt, object_opt) = rewrite_path(subject, path, object, &mut triples);

    match path_opt {
        None => {
            assert!(triples.len() > 0);
            GraphPattern::Bgp { patterns: triples }
        }
        Some(path) => {
            if let (Some(rewrite_subject), Some(rewrite_object)) = (subject_opt, object_opt) {
                let property_path = GraphPattern::Path {
                    subject: rewrite_subject,
                    path: path,
                    object: rewrite_object
                };
                if triples.len() > 0 {
                    GraphPattern::Union {
                        left: Box::new(property_path),
                        right: Box::new(GraphPattern::Bgp { patterns: triples })
                    }
                } else {
                    property_path
                }
            } else {
                todo!("Create error!")
            }
        }
    }
}

fn rewrite_path(
    subject: Option<&TermPattern>,
    path: &PropertyPathExpression,
    object: Option<&TermPattern>,
    triples: &mut Vec<TriplePattern>,
) -> (
    Option<TermPattern>,
    Option<PropertyPathExpression>,
    Option<TermPattern>,
) {
    match path {
        PropertyPathExpression::NamedNode(nn) => {
            if nn == HAS_TIMESERIES
                || nn == HAS_VALUE
                || nn == HAS_DATA_POINT
                || nn == HAS_TIMESERIES
            {
                match object {
                    None => {
                        todo!("Create error")
                    }
                    Some(o) => match subject {
                        None => {
                            let new_s =
                                TermPattern::Variable(Variable::new("new_s").expect("Noprob"));
                            let new_trip = TriplePattern {
                                subject: new_s.clone(),
                                predicate: NamedNodePattern::NamedNode(nn.clone()),
                                object: o.clone(),
                            };
                            triples.push(new_trip);
                            (Some(new_s), None, None)
                        }
                        Some(s) => {
                            let new_trip = TriplePattern {
                                subject: s.clone(),
                                predicate: NamedNodePattern::NamedNode(nn.clone()),
                                object: o.clone(),
                            };
                            triples.push(new_trip);
                            (None, None, None)
                        }
                    },
                }
            } else {
                let use_subj = if let Some(some_subj) = subject {
                Some(some_subj.clone())
            } else {
                None
            };
            let use_obj = if let Some(some_obj) = object {
                Some(some_obj.clone())
            } else {
                None
            };
            (use_subj, Some(PropertyPathExpression::NamedNode(nn.clone())), use_obj)
            }
        }
        PropertyPathExpression::Reverse(p) => {
            let (rewrite_o, rewrite_p, rewrite_s) = rewrite_path(object, p, subject, triples);
            match rewrite_p {
                None => return (rewrite_s, rewrite_p, rewrite_o),
                Some(some_rewrite_p) => {
                    return (
                        rewrite_o,
                        Some(PropertyPathExpression::Reverse(Box::new(some_rewrite_p))),
                        rewrite_s,
                    )
                }
            }
        }
        PropertyPathExpression::Sequence(a, b) => {
            let (a_rewrite_s, a_rewrite_p, a_rewrite_o) = rewrite_path(subject, a, None, triples);
            let (b_rewrite_s, b_rewrite_p, b_rewrite_o) = rewrite_path(subject, b, None, triples);
            match a_rewrite_p {
                None => match b_rewrite_p {
                    None => (a_rewrite_s, None, b_rewrite_s),
                    Some(_) => {
                        todo!("Handle this case, arises with reverse?")
                    }
                },
                Some(some_a_rewrite_p) => match b_rewrite_p {
                    None => {
                        let use_obj = if a_rewrite_o.is_none() {
                            b_rewrite_s
                        } else {
                            a_rewrite_o
                        };
                        (a_rewrite_s, Some(some_a_rewrite_p), use_obj)
                    }
                    Some(some_b_rewrite_p) => (
                        a_rewrite_s,
                        Some(PropertyPathExpression::Sequence(
                            Box::new(some_a_rewrite_p),
                            Box::new(some_b_rewrite_p),
                        )),
                        b_rewrite_o,
                    ),
                },
            }
        }
        p => {
            //No support for alternatives with
            let use_subj = if let Some(some_subj) = subject {
                Some(some_subj.clone())
            } else {
                None
            };
            let use_obj = if let Some(some_obj) = object {
                Some(some_obj.clone())
            } else {
                None
            };
            (use_subj, Some(p.clone()), use_obj)
        }
    }
}

use std::cmp::{min};
use oxrdf::Variable;
use spargebra::algebra::{AggregateExpression, Expression};
use std::string::ToString;

#[derive(Clone, Debug, PartialEq, Display)]
pub enum PathEntry {
    BGP,
    UnionLeftSide,
    UnionRightSide,
    JoinLeftSide,
    JoinRightSide,
    LeftJoinLeftSide,
    LeftJoinRightSide,
    LeftJoinExpression,
    MinusLeftSide,
    MinusRightSide,
    FilterInner,
    FilterExpression,
    GraphInner,
    ExtendInner,
    ExtendExpression,
    OrderByInner,
    OrderByExpression,
    ProjectInner,
    DistinctInner,
    ReducedInner,
    SliceInner,
    ServiceInner,
    GroupInner,
    GroupAggregation,
    IfLeft,
    IfMiddle,
    IfRight,
    OrLeft,
    OrRight,
    AndLeft,
    AndRight,
    EqualLeft,
    EqualRight,
    SameTermLeft,
    SameTermRight,
    GreaterLeft,
    GreaterRight,
    GreaterOrEqualLeft,
    GreaterOrEqualRight,
    LessLeft,
    LessRight,
    LessOrEqualLeft,
    LessOrEqualRight,
    InLeft,
    InRight,
    MultiplyLeft,
    MultiplyRight,
    AddLeft,
    AddRight,
    SubtractLeft,
    SubtractRight,
    DivideLeft,
    DivideRight,
    UnaryPlus,
    UnaryMinus,
    Not,
    Exists,
    Coalesce,
    FunctionCall,
    AggregationOperation,
    OrderingOperation
}

#[derive(Clone, PartialEq, Debug)]
pub struct Context {
    pub path: Vec<PathEntry>
}

impl Context {
    pub fn in_scope(&self, other: &Context, partial_scope:bool) -> bool {
        let min_i = min(self.path.len(), other.path.len());
        let mut self_divergence = vec![];
        let mut other_divergence = vec![];

        for i in 0..min_i {
            let other_entry = other.path.get(i).unwrap();
            let my_entry = self.path.get(i).unwrap();
            if other_entry != my_entry {
                self_divergence = self.path[i..self.path.len()].iter().collect();
                other_divergence = other.path[i..other.path.len()].iter().collect();
                break;
            }
        }

        for my_entry in self_divergence {
            if !exposes_variables(my_entry) {
                return false;
            }
        }
        if !partial_scope {
            for other_entry in other_divergence {
                if !maintains_full_downward_scope(other_entry) {
                    return false;
                }
            }
        }
        true
    }

    pub fn contains(&self, path_entry: &PathEntry) -> bool {
        self.path.contains(path_entry)
    }
}

fn exposes_variables(path_entry: &PathEntry) -> bool {
    match path_entry {
        PathEntry::BGP => {true}
        PathEntry::UnionLeftSide => {true}
        PathEntry::UnionRightSide => {true}
        PathEntry::JoinLeftSide => {true}
        PathEntry::JoinRightSide => {true}
        PathEntry::LeftJoinLeftSide => {true}
        PathEntry::LeftJoinRightSide => {true}
        PathEntry::LeftJoinExpression => {false}
        PathEntry::MinusLeftSide => {true}
        PathEntry::MinusRightSide => {false}
        PathEntry::FilterInner => {true}
        PathEntry::FilterExpression => {false}
        PathEntry::GraphInner => {true} //TODO: Check
        PathEntry::ExtendInner => {true}
        PathEntry::ExtendExpression => {false}
        PathEntry::OrderByInner => {true}
        PathEntry::OrderByExpression => {false}
        PathEntry::ProjectInner => {true} //TODO: Depends on projection! Extend later..
        PathEntry::DistinctInner => {true}
        PathEntry::ReducedInner => {true}
        PathEntry::SliceInner => {true}
        PathEntry::ServiceInner => {true}
        PathEntry::GroupInner => {true}
        PathEntry::GroupAggregation => {false}
        PathEntry::IfLeft => {false}
        PathEntry::IfMiddle => {false}
        PathEntry::IfRight => {false}
        PathEntry::OrLeft => {false}
        PathEntry::OrRight => {false}
        PathEntry::AndLeft => {false}
        PathEntry::AndRight => {false}
        PathEntry::EqualLeft => {false}
        PathEntry::EqualRight => {false}
        PathEntry::SameTermLeft => {false}
        PathEntry::SameTermRight => {false}
        PathEntry::GreaterLeft => {false}
        PathEntry::GreaterRight => {false}
        PathEntry::GreaterOrEqualLeft => {false}
        PathEntry::GreaterOrEqualRight => {false}
        PathEntry::LessLeft => {false}
        PathEntry::LessRight => {false}
        PathEntry::LessOrEqualLeft => {false}
        PathEntry::LessOrEqualRight => {false}
        PathEntry::InLeft => {false}
        PathEntry::InRight => {false}
        PathEntry::MultiplyLeft => {false}
        PathEntry::MultiplyRight => {false}
        PathEntry::AddLeft => {false}
        PathEntry::AddRight => {false}
        PathEntry::SubtractLeft => {false}
        PathEntry::SubtractRight => {false}
        PathEntry::DivideLeft => {false}
        PathEntry::DivideRight => {false}
        PathEntry::UnaryPlus => {false}
        PathEntry::UnaryMinus => {false}
        PathEntry::Not => {false}
        PathEntry::Exists => {false}
        PathEntry::Coalesce => {false}
        PathEntry::FunctionCall => {false}
        PathEntry::AggregationOperation => {false}
        PathEntry::OrderingOperation => {false}
    }
}

fn maintains_full_downward_scope(path_entry: &PathEntry) -> bool {
    match path_entry {
        PathEntry::BGP => {false}
        PathEntry::UnionLeftSide => {false}
        PathEntry::UnionRightSide => {false}
        PathEntry::JoinLeftSide => {false}
        PathEntry::JoinRightSide => {false}
        PathEntry::LeftJoinLeftSide => {false}
        PathEntry::LeftJoinRightSide => {false}
        PathEntry::LeftJoinExpression => {false}
        PathEntry::MinusLeftSide => {false}
        PathEntry::MinusRightSide => {false}
        PathEntry::FilterInner => {false}
        PathEntry::FilterExpression => {true}
        PathEntry::GraphInner => {false}
        PathEntry::ExtendInner => {false}
        PathEntry::ExtendExpression => {true}
        PathEntry::OrderByInner => {false}
        PathEntry::OrderByExpression => {true}
        PathEntry::ProjectInner => {false}
        PathEntry::DistinctInner => {false}
        PathEntry::ReducedInner => {false}
        PathEntry::SliceInner => {false}
        PathEntry::ServiceInner => {false}
        PathEntry::GroupInner => {false}
        PathEntry::GroupAggregation => {true}
        PathEntry::IfLeft => {true}
        PathEntry::IfMiddle => {true}
        PathEntry::IfRight => {true}
        PathEntry::OrLeft => {true}
        PathEntry::OrRight => {true}
        PathEntry::AndLeft => {true}
        PathEntry::AndRight => {true}
        PathEntry::EqualLeft => {true}
        PathEntry::EqualRight => {true}
        PathEntry::SameTermLeft => {true}
        PathEntry::SameTermRight => {true}
        PathEntry::GreaterLeft => {true}
        PathEntry::GreaterRight => {true}
        PathEntry::GreaterOrEqualLeft => {true}
        PathEntry::GreaterOrEqualRight => {true}
        PathEntry::LessLeft => {true}
        PathEntry::LessRight => {true}
        PathEntry::LessOrEqualLeft => {true}
        PathEntry::LessOrEqualRight => {true}
        PathEntry::InLeft => {true}
        PathEntry::InRight => {true}
        PathEntry::MultiplyLeft => {true}
        PathEntry::MultiplyRight => {true}
        PathEntry::AddLeft => {true}
        PathEntry::AddRight => {true}
        PathEntry::SubtractLeft => {true}
        PathEntry::SubtractRight => {true}
        PathEntry::DivideLeft => {true}
        PathEntry::DivideRight => {true}
        PathEntry::UnaryPlus => {true}
        PathEntry::UnaryMinus => {true}
        PathEntry::Not => {true}
        PathEntry::Exists => {true}
        PathEntry::Coalesce => {true}
        PathEntry::FunctionCall => {true}
        PathEntry::AggregationOperation => {true}
        PathEntry::OrderingOperation => {true}
    }
}

impl Context {
    pub fn new() -> Context {
        Context {
            path: vec![]
        }
    }

    pub fn to_string(&self) -> String {
        let strings: Vec<String> = self.path.iter().map(|x|x.to_string()).collect();
        strings.join("-")
    }

    pub fn extension_with(&self, p:PathEntry) -> Context {
        let mut path = self.path.clone();
        path.push(p);
        Context {
           path
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct VariableInContext {
    pub variable: Variable,
    context: Context
}

impl VariableInContext {
    pub fn same_name(&self, v: &Variable) -> bool {
        self.variable.as_str() == v.as_str()
    }

    pub fn in_scope(&self, context: &Context, partial_scope:bool) -> bool {
        self.context.in_scope(context, partial_scope)
    }

    pub fn equivalent(&self, variable:&Variable, context:&Context) -> bool {
        let ret = self.same_name(variable) && self.in_scope(context,false);
        ret
    }

    pub fn partial(&self, variable:&Variable, context:&Context) -> bool {
        self.same_name(variable) && self.in_scope(context, true)
    }
}

impl VariableInContext {
    pub fn new(variable:Variable, context:Context) -> VariableInContext {
        VariableInContext {
            variable,
            context
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ExpressionInContext {
    pub expression: Expression, 
    pub context: Context,
}

impl ExpressionInContext {
    pub fn new(expression: Expression, context:Context) -> ExpressionInContext {
        ExpressionInContext {
            expression,
            context
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct AggregateExpressionInContext {
    pub aggregate_expression: AggregateExpression,
    pub context: Context,
}

impl AggregateExpressionInContext {
    pub fn new(aggregate_expression: AggregateExpression, context:Context) -> AggregateExpressionInContext {
        AggregateExpressionInContext {
            aggregate_expression,
            context
        }
    }
}
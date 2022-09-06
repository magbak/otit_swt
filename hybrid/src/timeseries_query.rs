use crate::find_query_variables::find_all_used_variables_in_expression;
use crate::query_context::{Context, VariableInContext};
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use spargebra::algebra::{AggregateExpression, Expression};
use spargebra::term::Variable;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum TimeSeriesQuery {
    Basic(BasicTimeSeriesQuery),
    GroupedBasic(BasicTimeSeriesQuery, DataFrame, String),
    Filtered(Box<TimeSeriesQuery>, Expression), //Flag lets us know if filtering is complete.
    InnerSynchronized(Vec<Box<TimeSeriesQuery>>, Vec<Synchronizer>),
    ExpressionAs(Box<TimeSeriesQuery>, Variable, Expression),
    Grouped(GroupedTimeSeriesQuery),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Synchronizer {
    Identity(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupedTimeSeriesQuery {
    pub tsq: Box<TimeSeriesQuery>,
    pub graph_pattern_context: Context,
    pub by: Vec<Variable>,
    pub aggregations: Vec<(Variable, AggregateExpression)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicTimeSeriesQuery {
    pub identifier_variable: Option<Variable>,
    pub timeseries_variable: Option<VariableInContext>,
    pub data_point_variable: Option<VariableInContext>,
    pub value_variable: Option<VariableInContext>,
    pub datatype_variable: Option<Variable>,
    pub datatype: Option<NamedNode>,
    pub timestamp_variable: Option<VariableInContext>,
    pub ids: Option<Vec<String>>,
}

impl BasicTimeSeriesQuery {
    fn expected_columns(&self) -> HashSet<&str> {
        let mut expected_columns = HashSet::new();
        expected_columns.insert(self.identifier_variable.as_ref().unwrap().as_str());
        if let Some(vv) = &self.value_variable {
            expected_columns.insert(vv.variable.as_str());
        }
        if let Some(tsv) = &self.timestamp_variable {
            expected_columns.insert(tsv.variable.as_str());
        }
        expected_columns
    }
}

#[derive(Debug)]
pub struct TimeSeriesValidationError {
    missing_columns: Vec<String>,
    extra_columns: Vec<String>,
}

impl Display for TimeSeriesValidationError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Missing columns: {}, Extra columns: {}",
            &self.missing_columns.join(","),
            &self.extra_columns.join(",")
        )
    }
}

impl Error for TimeSeriesValidationError {}

impl TimeSeriesQuery {
    pub(crate) fn validate(&self, df: &DataFrame) -> Result<(), TimeSeriesValidationError> {
        let expected_columns = self.expected_columns();
        let df_columns: HashSet<&str> = df.get_column_names().into_iter().collect();
        if expected_columns != df_columns {
            let err = TimeSeriesValidationError {
                missing_columns: expected_columns
                    .difference(&df_columns)
                    .map(|x| x.to_string())
                    .collect(),
                extra_columns: df_columns
                    .difference(&expected_columns)
                    .map(|x| x.to_string())
                    .collect(),
            };
            Err(err)
        } else {
            Ok(())
        }
    }

    fn expected_columns<'a>(&'a self) -> HashSet<&'a str> {
        match self {
            TimeSeriesQuery::Basic(b) => b.expected_columns(),
            TimeSeriesQuery::Filtered(inner, ..) => inner.expected_columns(),
            TimeSeriesQuery::InnerSynchronized(inners, _synchronizers) => {
                inners.iter().fold(HashSet::new(), |mut exp, tsq| {
                    exp.extend(tsq.expected_columns());
                    exp
                })
            }
            TimeSeriesQuery::Grouped(g) => {
                let mut expected_columns = HashSet::new();
                for (v, _) in &g.aggregations {
                    expected_columns.insert(v.as_str());
                }
                expected_columns
            }
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                b.expected_columns()
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => {
                t.expected_columns()
            }
        }
    }

    pub(crate) fn get_mut_basic_queries(&mut self) -> Vec<&mut BasicTimeSeriesQuery> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                vec![b]
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_mut_basic_queries(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut basics = vec![];
                for inner in inners {
                    basics.extend(inner.get_mut_basic_queries())
                }
                basics
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_mut_basic_queries(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                vec![b]
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => t.get_mut_basic_queries(),
        }
    }

    pub(crate) fn has_equivalent_value_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for value_variable in self.get_value_variables() {
            if value_variable.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn has_equivalent_data_point_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for data_point_variable in self.get_data_point_variables() {
            if data_point_variable.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn has_equivalent_timeseries_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for timeseries_variable in self.get_timeseries_variables() {
            if timeseries_variable.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_ids(&self) -> Vec<&String> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_ids(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut ss = vec![];
                for inner in inners {
                    ss.extend(inner.get_ids())
                }
                ss
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_ids(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(tsq, ..) => tsq.get_ids(),
        }
    }

    pub(crate) fn get_data_point_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(data_point_var) = &b.data_point_variable {
                    vec![data_point_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_data_point_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_data_point_variables())
                }
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_data_point_variables(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                if let Some(data_point_var) = &b.data_point_variable {
                    vec![data_point_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => t.get_data_point_variables(),
        }
    }

    pub(crate) fn get_timeseries_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(var) = &b.timeseries_variable {
                    vec![var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_timeseries_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_timeseries_variables())
                }
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_timeseries_variables(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                if let Some(var) = &b.timeseries_variable {
                    vec![var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(tsq, ..) => {
                tsq.get_timeseries_variables()
            }
        }
    }

    pub(crate) fn get_value_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(val_var) = &b.value_variable {
                    vec![val_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_value_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_value_variables())
                }
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_value_variables(),
            TimeSeriesQuery::GroupedBasic(b,..) => {
                if let Some(val_var) = &b.value_variable {
                    vec![val_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => {
                t.get_value_variables()
            }
        }
    }

    pub(crate) fn get_identifier_variables(&self) -> Vec<&Variable> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(id_var) = &b.identifier_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_identifier_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_identifier_variables())
                }
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_identifier_variables(),
            TimeSeriesQuery::GroupedBasic(b,..) => {
                if let Some(id_var) = &b.identifier_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => {
                t.get_identifier_variables()
            }
        }
    }

    pub(crate) fn has_equivalent_timestamp_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for ts in self.get_timestamp_variables() {
            if ts.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_timestamp_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(v) = &b.timestamp_variable {
                    vec![v]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(t, _) => t.get_timestamp_variables(),
            TimeSeriesQuery::InnerSynchronized(ts, _) => {
                let mut vs = vec![];
                for t in ts {
                    vs.extend(t.get_timestamp_variables())
                }
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_timestamp_variables(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                if let Some(v) = &b.timestamp_variable {
                    vec![v]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => {
                t.get_timestamp_variables()
            }
        }
    }
}

impl BasicTimeSeriesQuery {
    pub fn new_empty() -> BasicTimeSeriesQuery {
        BasicTimeSeriesQuery {
            identifier_variable: None,
            timeseries_variable: None,
            data_point_variable: None,
            value_variable: None,
            datatype_variable: None,
            datatype: None,
            timestamp_variable: None,
            ids: None,
        }
    }
}

impl TimeSeriesQuery {
    pub fn get_timeseries_functions(&self, context: &Context) -> Vec<(&Variable, &Expression)> {
        match self {
            TimeSeriesQuery::Basic(..) => {
                vec![]
            }
            TimeSeriesQuery::GroupedBasic(..) => {
                vec![]
            }
            TimeSeriesQuery::Filtered(tsq, _) => tsq.get_timeseries_functions(context),
            TimeSeriesQuery::InnerSynchronized(tsqs, _) => {
                let mut out_tsfs = vec![];
                for tsq in tsqs {
                    out_tsfs.extend(tsq.get_timeseries_functions(context))
                }
                out_tsfs
            }
            TimeSeriesQuery::ExpressionAs(tsq, v, e) => {
                let mut tsfs = vec![];
                let mut used_vars = HashSet::new();
                find_all_used_variables_in_expression(e, &mut used_vars);
                let mut exists_timeseries_var = false;
                let mut all_are_timeseries_var = true;
                for v in &used_vars {
                    if tsq.has_equivalent_timestamp_variable(v, context) {
                        exists_timeseries_var = true;
                    } else {
                        all_are_timeseries_var = false;
                        break;
                    }
                }
                if exists_timeseries_var && all_are_timeseries_var {
                    tsfs.push((v, e))
                }
                tsfs.extend(tsq.get_timeseries_functions(context));
                tsfs
            }
            TimeSeriesQuery::Grouped(..) => {
                panic!("Not supported")
            }
        }
    }
}

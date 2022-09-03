mod aggregate_expressions;
mod expression_rewrite;
mod partitioning_support;

use crate::query_context::{AggregateExpressionInContext, ExpressionInContext};
use crate::timeseries_database::timeseries_sql_rewrite::aggregate_expressions::sparql_aggregate_expression_to_sql_expression;
use crate::timeseries_database::timeseries_sql_rewrite::expression_rewrite::sparql_expression_to_sql_expression;
use crate::timeseries_database::timeseries_sql_rewrite::partitioning_support::add_partitioned_timestamp_conditions;
use crate::timeseries_query::{BasicTimeSeriesQuery, Synchronizer, TimeSeriesQuery};
use oxrdf::{NamedNode, Variable};
use sea_query::{Alias, ColumnRef, JoinType, Query, SelectStatement, SimpleExpr, TableRef};
use sea_query::{Expr as SeaExpr, Iden, Value};
use spargebra::algebra::Expression;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::rc::Rc;

const YEAR_PARTITION_COLUMN_NAME: &str = "year_partition_column_name";
const MONTH_PARTITION_COLUMN_NAME: &str = "month_partition_column_name";
const DAY_PARTITION_COLUMN_NAME: &str = "day_partition_column_name";

#[derive(Debug)]
pub enum TimeSeriesQueryToSQLError {
    UnknownVariable(String),
    UnknownDatatype(String),
    FoundNonValueInInExpression,
    DatatypeNotSupported(String),
    MissingTimeseriesQueryDatatype,
}

impl Display for TimeSeriesQueryToSQLError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeSeriesQueryToSQLError::UnknownVariable(v) => {
                write!(f, "Unknown variable {}", v)
            }
            TimeSeriesQueryToSQLError::UnknownDatatype(d) => {
                write!(f, "Unknown datatype: {}", d)
            }
            TimeSeriesQueryToSQLError::FoundNonValueInInExpression => {
                write!(f, "In-expression contained non-literal alternative")
            }
            TimeSeriesQueryToSQLError::DatatypeNotSupported(dt) => {
                write!(f, "Datatype not supported: {}", dt)
            }
            TimeSeriesQueryToSQLError::MissingTimeseriesQueryDatatype => {
                write!(f, "Timeseries value datatype missing")
            }
        }
    }
}

impl Error for TimeSeriesQueryToSQLError {}

#[derive(Clone)]
pub struct TimeSeriesTable {
    pub schema: Option<String>,
    pub time_series_table: String,
    pub value_column: String,
    pub timestamp_column: String,
    pub identifier_column: String,
    pub value_datatype: NamedNode,
    pub year_column: Option<String>,
    pub month_column: Option<String>,
    pub day_column: Option<String>,
}

pub fn create_query(
    tsq: &TimeSeriesQuery,
    tables: &Vec<TimeSeriesTable>,
    project_date_partition: bool,
) -> Result<(SelectStatement, HashSet<String>), TimeSeriesQueryToSQLError> {
    match tsq {
        TimeSeriesQuery::Basic(b) => {
            let table = find_right_table(b, tables)?;
            let (mut select, columns) =
                table.create_basic_query(b, project_date_partition)?;

            Ok((select, columns))
        }
        TimeSeriesQuery::Filtered(tsq, filter, _) => {
            let mut and_where = None;
            let mut need_partition_columns = false;
            if let Some(f) = filter {
                let (se, added_partitioning) = create_filter_expressions(
                    f,
                    Some(&tsq.get_timestamp_variables().get(0).unwrap().variable.as_str().to_string()),
                    check_partitioning_support(tables),
                )?;
                need_partition_columns = added_partitioning;
                and_where = Some(se);
            }
            let (mut select, columns) = create_query(
                tsq,
                tables,
                need_partition_columns || project_date_partition,
            )?;
            if let Some(se) = and_where {
                select.and_where(se);
            }
            let use_select;
            if !project_date_partition && need_partition_columns {
                let alias = "filtering_query";
                let mut outer_select = Query::select();
                outer_select.from_subquery(select, Alias::new(alias));
                let mut sorted_cols: Vec<&String> = columns.iter().collect();
                sorted_cols.sort();
                for c in sorted_cols {
                    if c != YEAR_PARTITION_COLUMN_NAME
                        && c != MONTH_PARTITION_COLUMN_NAME
                        && c != DAY_PARTITION_COLUMN_NAME
                    {
                        outer_select.expr(SimpleExpr::Column(ColumnRef::Column(Rc::new(Name::Column(c.clone())))));
                    }
                }
                use_select = outer_select;
            } else {
                use_select = select;
            }

            Ok((use_select, columns))
        }
        TimeSeriesQuery::InnerSynchronized(inner, synchronizers) => {
            if synchronizers.iter().all(|x| {
                if let Synchronizer::Identity(_) = x {
                    true
                } else {
                    false
                }
            }) {
                let mut selects = vec![];
                for s in inner {
                    selects.push(create_query(s, tables, false)?);
                }
                if let Some(Synchronizer::Identity(timestamp_col)) = &synchronizers.get(0) {
                    Ok(inner_join_selects(selects, timestamp_col))
                } else {
                    panic!()
                }
            } else {
                todo!("Not implemented yet")
            }
        }
        TimeSeriesQuery::LeftSynchronized(_left, _right, _synchronizers, _filter, _) => {
            todo!()
        }
        TimeSeriesQuery::Grouped(grouped) => {
            let (inner_select, variable_column_name_map) =
                create_query(&grouped.tsq, tables, false)?;
            create_grouped_query(
                inner_select,
                variable_column_name_map,
                &grouped.by,
                &grouped.aggregations,
                &grouped.timeseries_funcs,
            )
        }
    }
}

fn inner_join_selects(
    mut selects_and_timestamp_cols: Vec<(SelectStatement, HashSet<String>)>,
    timestamp_col: &String,
) -> (SelectStatement, HashSet<String>) {
    let (mut first_select, mut first_columns) = selects_and_timestamp_cols.remove(0);
    for (i, (s, cols)) in selects_and_timestamp_cols.into_iter().enumerate() {
        let select_name = format!("other_{}", i);

        first_select.join(
            JoinType::InnerJoin,
            TableRef::SubQuery(s, Rc::new(Alias::new(&select_name))),
            SimpleExpr::Column(ColumnRef::Column(Rc::new(Name::Column(
                timestamp_col.clone(),
            ))))
            .equals(SimpleExpr::Column(ColumnRef::TableColumn(
                Rc::new(Name::Table(select_name.clone())),
                Rc::new(Name::Column(timestamp_col.clone())),
            ))),
        );
        let mut sorted_cols: Vec<&String> = cols.iter().collect();
        sorted_cols.sort();
        for c in sorted_cols {
            if c != timestamp_col {
                first_select.expr_as(
                    SimpleExpr::Column(ColumnRef::TableColumn(
                        Rc::new(Name::Table(select_name.clone())),
                        Rc::new(Name::Column(c.clone())),
                    )),
                    Alias::new(&c),
                );
                first_columns.insert(c.clone());
            }
        }
    }
    (first_select, first_columns)
}

fn find_right_table<'a>(
    btsq: &BasicTimeSeriesQuery,
    tables: &'a Vec<TimeSeriesTable>,
) -> Result<&'a TimeSeriesTable, TimeSeriesQueryToSQLError> {
    if let Some(b_datatype) = &btsq.datatype {
        for table in tables {
            if table.value_datatype.as_str() == b_datatype.as_str() {
                return Ok(table);
            }
        }
        Err(TimeSeriesQueryToSQLError::DatatypeNotSupported(
            b_datatype.as_str().to_string(),
        ))
    } else {
        Err(TimeSeriesQueryToSQLError::MissingTimeseriesQueryDatatype)
    }
}

fn create_filter_expressions(
    expression: &Expression,
    timestamp_column: Option<&String>,
    partitioning_support: bool,
) -> Result<(SimpleExpr, bool), TimeSeriesQueryToSQLError> {
    let mut se = sparql_expression_to_sql_expression(expression, None)?;
    let mut partitioned = false;
    if partitioning_support {
        let (se_part, part_status) = add_partitioned_timestamp_conditions(
            se,
            &timestamp_column.unwrap(),
            YEAR_PARTITION_COLUMN_NAME,
            MONTH_PARTITION_COLUMN_NAME,
            DAY_PARTITION_COLUMN_NAME,
        );
        se = se_part;
        partitioned = part_status;
    }
    Ok((se, partitioned))
}

fn create_grouped_query(
    query: SelectStatement,
    mut columns: HashSet<String>,
    by: &Vec<Variable>,
    aggregations: &Vec<(Variable, AggregateExpressionInContext)>,
    timeseries_funcs: &Vec<(Variable, ExpressionInContext)>,
) -> Result<(SelectStatement, HashSet<String>), TimeSeriesQueryToSQLError> {
    let mut inner_query = Query::select();
    let inner_query_str = "inner_query";
    let inner_query_name = Name::Table(inner_query_str.to_string());
    inner_query.from_subquery(query, inner_query_name.clone());
    let mut sorted_cols: Vec<&String> = columns.iter().collect();
    sorted_cols.sort();
    for c in &sorted_cols {
        inner_query.expr(SimpleExpr::Column(ColumnRef::TableColumn(
            Rc::new(inner_query_name.clone()),
            Rc::new(Name::Column(c.to_string())),
        )));
    }

    for (v, e) in timeseries_funcs.iter().rev() {
        inner_query.expr_as(
            sparql_expression_to_sql_expression(
                &e.expression,
                Some(&inner_query_name),
            )?,
            Alias::new(v.as_str()),
        );
        columns.insert(v.as_str().to_string());
    }
    let mut outer_query = Query::select();
    let outer_query_str = "outer_query";
    let outer_query_name = Name::Table(outer_query_str.to_string());
    outer_query.from_subquery(inner_query, Alias::new(outer_query_str));

    let mut new_columns = HashSet::new();
    for (v, agg) in aggregations {
        outer_query.expr_as(
            sparql_aggregate_expression_to_sql_expression(
                &agg.aggregate_expression,
                Some(&outer_query_name),
            )?,
            Alias::new(v.as_str()),
        );
        new_columns.insert(v.as_str().to_string());
    }

    outer_query.group_by_columns(
        by.iter()
            .map(|x| {
                ColumnRef::TableColumn(
                    Rc::new(outer_query_name.clone()),
                    Rc::new(Name::Column(x.as_str().to_string())),
                )
            })
            .collect::<Vec<ColumnRef>>(),
    );
    for v in by {
        outer_query.expr_as(
            SimpleExpr::Column(ColumnRef::TableColumn(
                Rc::new(outer_query_name.clone()),
                Rc::new(Name::Column(v.as_str().to_string())),
            )),
            Alias::new(v.as_str()),
        );
        new_columns.insert(v.as_str().to_string());
    }
    Ok((outer_query, new_columns))
}

impl TimeSeriesTable {
    pub fn create_basic_query(
        &self,
        btsq: &BasicTimeSeriesQuery,
        project_date_partition: bool,
    ) -> Result<(SelectStatement, HashSet<String>), TimeSeriesQueryToSQLError> {
        let mut basic_query = Query::select();
        let mut variable_column_name_map = HashMap::new();
        variable_column_name_map.insert(
            btsq.identifier_variable
                .as_ref()
                .unwrap()
                .as_str()
                .to_string(),
            self.identifier_column.clone(),
        );
        variable_column_name_map.insert(
            btsq.value_variable
                .as_ref()
                .unwrap()
                .variable
                .as_str()
                .to_string(),
            self.value_column.clone(),
        );
        variable_column_name_map.insert(
            btsq.timestamp_variable
                .as_ref()
                .unwrap()
                .variable
                .as_str()
                .to_string(),
            self.timestamp_column.clone(),
        );
        if project_date_partition {
            variable_column_name_map.insert(
                YEAR_PARTITION_COLUMN_NAME.to_string(),
                self.year_column.as_ref().unwrap().clone(),
            );
            variable_column_name_map.insert(
                MONTH_PARTITION_COLUMN_NAME.to_string(),
                self.month_column.as_ref().unwrap().clone(),
            );
            variable_column_name_map.insert(
                DAY_PARTITION_COLUMN_NAME.to_string(),
                self.day_column.as_ref().unwrap().clone(),
            );
        }

        let mut kvs: Vec<_> = variable_column_name_map.iter().collect();
        kvs.sort();
        for (k, v) in kvs {
            basic_query.expr_as(SeaExpr::col(Name::Column(v.clone())), Alias::new(k));
        }
        if let Some(schema) = &self.schema {
            basic_query.from((
                Name::Schema(schema.clone()),
                Name::Table(self.time_series_table.clone()),
            ));
        } else {
            basic_query.from(Name::Table(self.time_series_table.clone()));
        }

        if let Some(ids) = &btsq.ids {
            basic_query.and_where(
                SeaExpr::col(Name::Column(self.identifier_column.clone())).is_in(
                    ids.iter()
                        .map(|x| Value::String(Some(Box::new(x.to_string())))),
                ),
            );
        }

        Ok((basic_query, variable_column_name_map.into_keys().collect()))
    }
}

#[derive(Clone)]
pub(crate) enum Name {
    Schema(String),
    Table(String),
    Column(String),
    Function(String),
}

impl Iden for Name {
    fn unquoted(&self, s: &mut dyn Write) {
        write!(
            s,
            "{}",
            match self {
                Name::Schema(s) => {
                    s
                }
                Name::Table(s) => {
                    s
                }
                Name::Column(s) => {
                    s
                }
                Name::Function(s) => {
                    s
                }
            }
        )
        .unwrap();
    }
}

fn check_partitioning_support(tables: &Vec<TimeSeriesTable>) -> bool {
    tables
        .iter()
        .all(|x| x.day_column.is_some() && x.month_column.is_some() && x.day_column.is_some())
}
#[cfg(test)]
mod tests {
    use crate::query_context::{Context, VariableInContext};
    use crate::timeseries_database::timeseries_sql_rewrite::{create_query, TimeSeriesTable};
    use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
    use oxrdf::vocab::xsd;
    use oxrdf::{Literal, NamedNode, Variable};
    use sea_query::PostgresQueryBuilder;
    use spargebra::algebra::Expression;
    use std::vec;

    #[test]
    pub fn test_translate() {
        let basic_tsq = BasicTimeSeriesQuery {
            identifier_variable: Some(Variable::new_unchecked("id")),
            timeseries_variable: Some(VariableInContext::new(
                Variable::new_unchecked("ts"),
                Context::new(),
            )),
            data_point_variable: Some(VariableInContext::new(
                Variable::new_unchecked("dp"),
                Context::new(),
            )),
            value_variable: Some(VariableInContext::new(
                Variable::new_unchecked("v"),
                Context::new(),
            )),
            datatype_variable: Some(Variable::new_unchecked("dt")),
            datatype: Some(xsd::DOUBLE.into_owned()),
            timestamp_variable: Some(VariableInContext::new(
                Variable::new_unchecked("t"),
                Context::new(),
            )),
            ids: Some(vec!["A".to_string(), "B".to_string()]),
        };
        let tsq = TimeSeriesQuery::Filtered(
            Box::new(TimeSeriesQuery::Basic(basic_tsq)),
            Some(Expression::LessOrEqual(
                Box::new(Expression::Variable(Variable::new_unchecked("t"))),
                Box::new(Expression::Literal(Literal::new_typed_literal(
                    "2022-06-01T08:46:53",
                    xsd::DATE_TIME,
                ))),
            )),
            false,
        );

        let table = TimeSeriesTable {
            schema: Some("s3.otit-benchmark".into()),
            time_series_table: "timeseries_double".into(),
            value_column: "value".into(),
            timestamp_column: "timestamp".into(),
            identifier_column: "dir3".into(),
            value_datatype: NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double"),
            year_column: Some("dir0".to_string()),
            month_column: Some("dir1".to_string()),
            day_column: Some("dir2".to_string()),
        };

        let (sql_query, _) = create_query(&tsq, &vec![table], false).unwrap();
        //println!("{}", sql_query)
        assert_eq!(
            &sql_query.to_string(PostgresQueryBuilder),
            r#"SELECT "id", "t", "v" FROM (SELECT "dir2" AS "day_partition_column_name", "dir3" AS "id", "dir1" AS "month_partition_column_name", "timestamp" AS "t", "value" AS "v", "dir0" AS "year_partition_column_name" FROM "s3.otit-benchmark"."timeseries_double" WHERE "dir3" IN ('A', 'B') AND (("year_partition_column_name" < 2022) OR (("year_partition_column_name" = 2022) AND ("month_partition_column_name" < 6)) OR ("year_partition_column_name" = 2022) AND ("month_partition_column_name" = 6) AND ("day_partition_column_name" < 1) OR ("year_partition_column_name" = 2022) AND ("month_partition_column_name" = 6) AND ("day_partition_column_name" = 1) AND ("t" <= '2022-06-01 08:46:53'))) AS "filtering_query""#
        );
    }
}

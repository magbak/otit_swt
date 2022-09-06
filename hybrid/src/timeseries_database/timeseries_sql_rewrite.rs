mod expression_rewrite;
mod partitioning_support;

use crate::timeseries_database::timeseries_sql_rewrite::expression_rewrite::SPARQLToSQLExpressionTransformer;
use crate::timeseries_database::timeseries_sql_rewrite::partitioning_support::add_partitioned_timestamp_conditions;
use crate::timeseries_query::{BasicTimeSeriesQuery, Synchronizer, TimeSeriesQuery};
use oxrdf::{NamedNode, Variable};
use sea_query::{
    Alias, BinOper, ColumnRef, JoinType, Query, SelectStatement, SimpleExpr, TableRef,
};
use sea_query::{Expr as SeaExpr, Iden, Value};
use spargebra::algebra::{AggregateExpression, Expression};
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
            let (mut select, columns) = table.create_basic_query(b, project_date_partition)?;

            Ok((select, columns))
        }
        TimeSeriesQuery::Filtered(tsq, filter) => {
            let mut need_partition_columns = false;

            let (se, added_partitioning) = create_filter_expressions(
                filter,
                Some(
                    &tsq.get_timestamp_variables()
                        .get(0)
                        .unwrap()
                        .variable
                        .as_str()
                        .to_string(),
                ),
                check_partitioning_support(tables),
            )?;
            need_partition_columns = added_partitioning;

            let (mut select, columns) = create_query(
                tsq,
                tables,
                need_partition_columns || project_date_partition,
            )?;

            let wraps_inner = if let TimeSeriesQuery::Basic(_) = **tsq {
                true
            } else {
                false
            };
            let mut use_select;
            if wraps_inner || (!project_date_partition && need_partition_columns) {
                let alias = "filtering_query";
                let mut outer_select = Query::select();
                outer_select.from_subquery(select, Alias::new(alias));
                let mut sorted_cols: Vec<&String> = columns.iter().collect();
                sorted_cols.sort();
                for c in sorted_cols {
                    if !(!project_date_partition && need_partition_columns)
                        || (c != YEAR_PARTITION_COLUMN_NAME
                            && c != MONTH_PARTITION_COLUMN_NAME
                            && c != DAY_PARTITION_COLUMN_NAME)
                    {
                        outer_select.expr(SimpleExpr::Column(ColumnRef::Column(Rc::new(
                            Name::Column(c.clone()),
                        ))));
                    }
                }
                use_select = outer_select;
            } else {
                use_select = select;
            }

            use_select.and_where(se);

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
                    selects.push(create_query(s, tables, true)?);
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
        TimeSeriesQuery::Grouped(grouped) => create_grouped_query(
            &grouped.tsq,
            &grouped.by,
            &grouped.aggregations,
            tables,
            project_date_partition,
        ),
        TimeSeriesQuery::GroupedBasic(_, _, _) => {}
        TimeSeriesQuery::ExpressionAs(_, _, _) => {}
    }
}

fn inner_join_selects(
    mut selects_and_timestamp_cols: Vec<(SelectStatement, HashSet<String>)>,
    timestamp_col: &String,
) -> (SelectStatement, HashSet<String>) {
    let (mut first_select, mut first_columns) = selects_and_timestamp_cols.remove(0);
    let mut new_first_select = Query::select();
    let first_select_name = "first_query";
    new_first_select.from_subquery(first_select, Alias::new(first_select_name));
    let mut sorted_cols: Vec<&String> = first_columns.iter().collect();
    sorted_cols.sort();
    for c in sorted_cols {
        new_first_select.expr_as(
            SimpleExpr::Column(ColumnRef::TableColumn(
                Rc::new(Name::Table(first_select_name.to_string())),
                Rc::new(Name::Column(c.to_string())),
            )),
            Alias::new(c),
        );
    }
    first_select = new_first_select;

    for (i, (s, cols)) in selects_and_timestamp_cols.into_iter().enumerate() {
        let select_name = format!("other_{}", i);
        let mut conditions = vec![];
        let col_conditions = [
            timestamp_col.clone(),
            YEAR_PARTITION_COLUMN_NAME.to_string(),
            MONTH_PARTITION_COLUMN_NAME.to_string(),
            DAY_PARTITION_COLUMN_NAME.to_string(),
        ];
        for c in col_conditions {
            conditions.push(
                SimpleExpr::Column(ColumnRef::TableColumn(
                    Rc::new(Name::Table(first_select_name.to_string())),
                    Rc::new(Name::Column(c.clone())),
                ))
                .equals(SimpleExpr::Column(ColumnRef::TableColumn(
                    Rc::new(Name::Table(select_name.clone())),
                    Rc::new(Name::Column(c)),
                ))),
            );
        }
        let mut first_condition = conditions.remove(0);
        for c in conditions {
            first_condition =
                SimpleExpr::Binary(Box::new(first_condition), BinOper::And, Box::new(c));
        }

        first_select.join(
            JoinType::InnerJoin,
            TableRef::SubQuery(s, Rc::new(Alias::new(&select_name))),
            first_condition,
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
    let mut transformer = create_transformer(partitioning_support, None);
    let mut se = transformer.sparql_expression_to_sql_expression(expression)?;
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
        partitioned = part_status || transformer.used_partitioning;
    }
    Ok((se, partitioned))
}

fn create_grouped_query(
    inner_tsq: &TimeSeriesQuery,
    by: &Vec<Variable>,
    aggregations: &Vec<(Variable, AggregateExpression)>,
    tables: &Vec<TimeSeriesTable>,
    project_date_partition: bool,
) -> Result<(SelectStatement, HashSet<String>), TimeSeriesQueryToSQLError> {
    let partitioning_support = check_partitioning_support(tables);

    //Inner query timeseries functions:
    let inner_query_str = "inner_query";
    let inner_query_name = Name::Table(inner_query_str.to_string());
    let mut expr_transformer = create_transformer(partitioning_support, Some(&inner_query_name));
    let mut ses = vec![];


    //Outer query aggregations:
    let outer_query_str = "outer_query";
    let outer_query_name = Name::Table(outer_query_str.to_string());
    let mut new_columns = HashSet::new();
    let mut agg_transformer = create_transformer(partitioning_support, Some(&outer_query_name));
    let mut aggs = vec![];
    for (_, agg) in aggregations {
        aggs.push(
            agg_transformer
                .sparql_aggregate_expression_to_sql_expression(agg)?,
        );
    }

    let (query, columns) = create_query(
        &inner_tsq,
        tables,
        expr_transformer.used_partitioning
            || agg_transformer.used_partitioning
            || project_date_partition,
    )?;
    let mut inner_query = Query::select();

    inner_query.from_subquery(query, inner_query_name.clone());
    let mut sorted_cols: Vec<&String> = columns.iter().collect();
    sorted_cols.sort();
    for c in &sorted_cols {
        inner_query.expr_as(
            SimpleExpr::Column(ColumnRef::TableColumn(
                Rc::new(inner_query_name.clone()),
                Rc::new(Name::Column(c.to_string())),
            )),
            Alias::new(c),
        );
    }

    let mut outer_query = Query::select();
    outer_query.from_subquery(inner_query, Alias::new(outer_query_str));

    for (v, _) in aggregations {
        let agg_trans = aggs.remove(0);
        outer_query.expr_as(agg_trans, Alias::new(v.as_str()));
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


fn create_transformer(
    partitioning_support: bool,
    table_name: Option<&Name>,
) -> SPARQLToSQLExpressionTransformer {
    if partitioning_support {
        SPARQLToSQLExpressionTransformer::new(
            table_name,
            Some(YEAR_PARTITION_COLUMN_NAME),
            Some(MONTH_PARTITION_COLUMN_NAME),
            Some(DAY_PARTITION_COLUMN_NAME),
        )
    } else {
        SPARQLToSQLExpressionTransformer::new(table_name, None, None, None)
    }
}

/*
#[cfg(test)]
mod tests {
    use crate::query_context::{
        AggregateExpressionInContext, Context, ExpressionInContext, VariableInContext,
    };
    use crate::timeseries_database::timeseries_sql_rewrite::{create_query, TimeSeriesTable};
    use crate::timeseries_query::{
        BasicTimeSeriesQuery, GroupedTimeSeriesQuery, Synchronizer, TimeSeriesQuery,
    };
    use oxrdf::vocab::xsd;
    use oxrdf::{Literal, NamedNode, Variable};
    use sea_query::PostgresQueryBuilder;
    use spargebra::algebra::{AggregateExpression, Expression, Function};
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
            Expression::LessOrEqual(
                Box::new(Expression::Variable(Variable::new_unchecked("t"))),
                Box::new(Expression::Literal(Literal::new_typed_literal(
                    "2022-06-01T08:46:53",
                    xsd::DATE_TIME,
                ))),
            ),
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
            r#"SELECT "id", "t", "v" FROM (SELECT "dir2" AS "day_partition_column_name", "dir3" AS "id", "dir1" AS "month_partition_column_name", "timestamp" AS "t", "value" AS "v", "dir0" AS "year_partition_column_name" FROM "s3.otit-benchmark"."timeseries_double" WHERE "dir3" IN ('A', 'B')) AS "filtering_query" WHERE ("year_partition_column_name" < 2022) OR (("year_partition_column_name" = 2022) AND ("month_partition_column_name" < 6)) OR ("year_partition_column_name" = 2022) AND ("month_partition_column_name" = 6) AND ("day_partition_column_name" < 1) OR ("year_partition_column_name" = 2022) AND ("month_partition_column_name" = 6) AND ("day_partition_column_name" = 1) AND ("t" <= '2022-06-01 08:46:53')"#
        );
    }

    #[test]
    fn test_synchronized_grouped() {
        let tsq = TimeSeriesQuery::Grouped(GroupedTimeSeriesQuery {
            tsq: Box::new(TimeSeriesQuery::Filtered(
                Box::new(TimeSeriesQuery::InnerSynchronized(
                    vec![
                        Box::new(TimeSeriesQuery::Basic(BasicTimeSeriesQuery {
                            identifier_variable: Some(Variable::new_unchecked("ts_external_id_1")),
                            timeseries_variable: Some(VariableInContext::new(
                                Variable::new_unchecked("ts_speed"),
                                Context::new(),
                            )),
                            data_point_variable: Some(VariableInContext::new(
                                Variable::new_unchecked("dp_speed"),
                                Context::new(),
                            )),
                            value_variable: Some(VariableInContext::new(
                                Variable::new_unchecked("val_speed"),
                                Context::new(),
                            )),
                            datatype_variable: Some(Variable::new_unchecked("ts_datatype_1")),
                            datatype: Some(xsd::DOUBLE.into_owned()),
                            timestamp_variable: Some(VariableInContext::new(
                                Variable::new_unchecked("t"),
                                Context::new(),
                            )),
                            ids: Some(vec!["id1".to_string()]),
                        })),
                        Box::new(TimeSeriesQuery::Basic(BasicTimeSeriesQuery {
                            identifier_variable: Some(Variable::new_unchecked("ts_external_id_2")),
                            timeseries_variable: Some(VariableInContext::new(
                                Variable::new_unchecked("ts_dir"),
                                Context::new(),
                            )),
                            data_point_variable: Some(VariableInContext::new(
                                Variable::new_unchecked("dp_dir"),
                                Context::new(),
                            )),
                            value_variable: Some(VariableInContext::new(
                                Variable::new_unchecked("val_dir"),
                                Context::new(),
                            )),
                            datatype_variable: Some(Variable::new_unchecked("ts_datatype_2")),
                            datatype: Some(xsd::DOUBLE.into_owned()),
                            timestamp_variable: Some(VariableInContext::new(
                                Variable::new_unchecked("t"),
                                Context::new(),
                            )),
                            ids: Some(vec!["id2".to_string()]),
                        })),
                    ],
                    vec![Synchronizer::Identity("t".to_string())],
                )),
                Expression::And(
                    Box::new(Expression::GreaterOrEqual(
                        Box::new(Expression::Variable(Variable::new_unchecked("t"))),
                        Box::new(Expression::Literal(Literal::new_typed_literal(
                            "2022-08-30T08:46:53",
                            xsd::DATE_TIME,
                        ))),
                    )),
                    Box::new(Expression::LessOrEqual(
                        Box::new(Expression::Variable(Variable::new_unchecked("t"))),
                        Box::new(Expression::Literal(Literal::new_typed_literal(
                            "2022-08-30T21:46:53",
                            xsd::DATE_TIME,
                        ))),
                    )),
                )),
            ),
            graph_pattern_context: Context::new(),
            by: vec![
                Variable::new_unchecked("year".to_string()),
                Variable::new_unchecked("month".to_string()),
                Variable::new_unchecked("day".to_string()),
                Variable::new_unchecked("hour".to_string()),
                Variable::new_unchecked("minute_10"),
                Variable::new_unchecked("ts_external_id_1"),
                Variable::new_unchecked("ts_external_id_0"),
            ],
            aggregations: vec![
                (
                    Variable::new_unchecked("f7ca5ee9058effba8691ac9c642fbe95"),
                        AggregateExpression::Avg {
                            expr: Box::new(Expression::Variable(Variable::new_unchecked(
                                "val_dir",
                            ))),
                            distinct: false,
                        }
                ),
                (
                    Variable::new_unchecked("990362f372e4019bc151c13baf0b50d5"),

                        AggregateExpression::Avg {
                            expr: Box::new(Expression::Variable(Variable::new_unchecked(
                                "val_speed",
                            ))),
                            distinct: false,
                        },
                ),
            ],
            timeseries_funcs: vec![
                (
                    Variable::new_unchecked("minute_10"),
                    ExpressionInContext::new(
                        Expression::FunctionCall(
                            Function::Custom(xsd::INTEGER.into_owned()),
                            vec![Expression::FunctionCall(
                                Function::Floor,
                                vec![Expression::Divide(
                                    Box::new(Expression::FunctionCall(
                                        Function::Minutes,
                                        vec![Expression::Variable(Variable::new_unchecked("t"))],
                                    )),
                                    Box::new(Expression::Literal(Literal::new_typed_literal(
                                        "10.0",
                                        xsd::DECIMAL,
                                    ))),
                                )],
                            )],
                        ),
                        Context::new(),
                    ),
                ),
                (
                    Variable::new_unchecked("hour"),
                    ExpressionInContext::new(
                        Expression::FunctionCall(
                            Function::Hours,
                            vec![Expression::Variable(Variable::new_unchecked("t"))],
                        ),
                        Context::new(),
                    ),
                ),
                (
                    Variable::new_unchecked("day"),
                    ExpressionInContext::new(
                        Expression::FunctionCall(
                            Function::Day,
                            vec![Expression::Variable(Variable::new_unchecked("t"))],
                        ),
                        Context::new(),
                    ),
                ),
                (
                    Variable::new_unchecked("month"),
                    ExpressionInContext::new(
                        Expression::FunctionCall(
                            Function::Month,
                            vec![Expression::Variable(Variable::new_unchecked("t"))],
                        ),
                        Context::new(),
                    ),
                ),
                (
                    Variable::new_unchecked("year"),
                    ExpressionInContext::new(
                        Expression::FunctionCall(
                            Function::Year,
                            vec![Expression::Variable(Variable::new_unchecked("t"))],
                        ),
                        Context::new(),
                    ),
                ),
            ],
        });

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

        let expected_str = r#"SELECT AVG("outer_query"."val_dir") AS "f7ca5ee9058effba8691ac9c642fbe95", AVG("outer_query"."val_speed") AS "990362f372e4019bc151c13baf0b50d5", "outer_query"."year" AS "year", "outer_query"."month" AS "month", "outer_query"."day" AS "day", "outer_query"."hour" AS "hour", "outer_query"."minute_10" AS "minute_10", "outer_query"."ts_external_id_1" AS "ts_external_id_1", "outer_query"."ts_external_id_0" AS "ts_external_id_0" FROM (SELECT "inner_query"."day_partition_column_name" AS "day_partition_column_name", "inner_query"."month_partition_column_name" AS "month_partition_column_name", "inner_query"."t" AS "t", "inner_query"."ts_external_id_1" AS "ts_external_id_1", "inner_query"."ts_external_id_2" AS "ts_external_id_2", "inner_query"."val_dir" AS "val_dir", "inner_query"."val_speed" AS "val_speed", "inner_query"."year_partition_column_name" AS "year_partition_column_name", "inner_query"."year_partition_column_name" AS "year", "inner_query"."month_partition_column_name" AS "month", "inner_query"."day_partition_column_name" AS "day", date_part('hour', "inner_query"."t") AS "hour", CAST(FLOOR(date_part('minute', "inner_query"."t") / 10), 'BIGINT') AS "minute_10" FROM (SELECT "first_query"."day_partition_column_name" AS "day_partition_column_name", "first_query"."month_partition_column_name" AS "month_partition_column_name", "first_query"."t" AS "t", "first_query"."ts_external_id_1" AS "ts_external_id_1", "first_query"."val_speed" AS "val_speed", "first_query"."year_partition_column_name" AS "year_partition_column_name", "other_0"."day_partition_column_name" AS "day_partition_column_name", "other_0"."month_partition_column_name" AS "month_partition_column_name", "other_0"."ts_external_id_2" AS "ts_external_id_2", "other_0"."val_dir" AS "val_dir", "other_0"."year_partition_column_name" AS "year_partition_column_name" FROM (SELECT "dir2" AS "day_partition_column_name", "dir1" AS "month_partition_column_name", "timestamp" AS "t", "dir3" AS "ts_external_id_1", "value" AS "val_speed", "dir0" AS "year_partition_column_name" FROM "s3.otit-benchmark"."timeseries_double" WHERE "dir3" IN ('id1')) AS "first_query" INNER JOIN (SELECT "dir2" AS "day_partition_column_name", "dir1" AS "month_partition_column_name", "timestamp" AS "t", "dir3" AS "ts_external_id_2", "value" AS "val_dir", "dir0" AS "year_partition_column_name" FROM "s3.otit-benchmark"."timeseries_double" WHERE "dir3" IN ('id2')) AS "other_0" ON ("first_query"."t" = "other_0"."t") AND ("first_query"."year_partition_column_name" = "other_0"."year_partition_column_name") AND ("first_query"."month_partition_column_name" = "other_0"."month_partition_column_name") AND ("first_query"."day_partition_column_name" = "other_0"."day_partition_column_name") WHERE ("year_partition_column_name" > 2022) OR (("year_partition_column_name" = 2022) AND ("month_partition_column_name" > 8)) OR ("year_partition_column_name" = 2022) AND ("month_partition_column_name" = 8) AND ("day_partition_column_name" > 30) OR ("year_partition_column_name" = 2022) AND ("month_partition_column_name" = 8) AND ("day_partition_column_name" = 30) AND ("t" >= '2022-08-30 08:46:53') AND (("year_partition_column_name" < 2022) OR (("year_partition_column_name" = 2022) AND ("month_partition_column_name" < 8)) OR ("year_partition_column_name" = 2022) AND ("month_partition_column_name" = 8) AND ("day_partition_column_name" < 30) OR ("year_partition_column_name" = 2022) AND ("month_partition_column_name" = 8) AND ("day_partition_column_name" = 30) AND ("t" <= '2022-08-30 21:46:53'))) AS "inner_query") AS "outer_query" GROUP BY "outer_query"."year", "outer_query"."month", "outer_query"."day", "outer_query"."hour", "outer_query"."minute_10", "outer_query"."ts_external_id_1", "outer_query"."ts_external_id_0""#;
        assert_eq!(expected_str, sql_query.to_string(PostgresQueryBuilder));
    }
}
*/

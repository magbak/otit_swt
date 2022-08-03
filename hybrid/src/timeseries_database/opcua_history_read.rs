use std::collections::HashMap;
use crate::constants::DATETIME_AS_SECONDS;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::TimeSeriesQuery;
use opcua_client::prelude::{DateTime, AggregateConfiguration, AttributeService, ByteString, Client, ClientBuilder, EndpointDescription, ExtensionObject, HistoryReadAction, HistoryReadResult, HistoryReadValueId, Identifier, IdentityToken, MessageSecurityMode, NodeId, QualifiedName, ReadProcessedDetails, Session, TimestampsToReturn, UAString, UserTokenPolicy, ReadRawModifiedDetails, HistoryData, BinaryEncoder, Variant};
use oxrdf::vocab::xsd;
use oxrdf::{Literal, Variable};
use polars::export::chrono::{DateTime as ChronoDateTime, NaiveDateTime, TimeZone, Utc};
use polars_core::frame::DataFrame;
use spargebra::algebra::{AggregateExpression, Expression, Function};
use std::error::Error;
use std::process::id;
use std::sync::{Arc, RwLock};
use polars_core::prelude::{AnyValue, DataType, NamedFrom};
use polars_core::series::Series;
use async_trait::async_trait;
use polars::prelude::{concat, IntoLazy};

const OPCUA_AGG_FUNC_AVERAGE: u32 = 2342;
const OPCUA_AGG_FUNC_COUNT: u32 = 2352;
const OPCUA_AGG_FUNC_MINIMUM: u32 = 2346;
const OPCUA_AGG_FUNC_MAXIMUM: u32 = 2347;
const OPCUA_AGG_FUNC_TOTAL: u32 = 2344;

pub struct OPCUAHistoryRead {
    client: Client,
    session: Arc<RwLock<Session>>,
    namespace: u16,
}

impl OPCUAHistoryRead {
    pub fn new(endpoint: &str, namespace: u16) -> OPCUAHistoryRead {
        //From: https://github.com/locka99/opcua/blob/master/docs/client.md
        let mut client = ClientBuilder::new()
            .application_name("My First Client")
            .application_uri("urn:MyFirstClient")
            .create_sample_keypair(true)
            .trust_server_certs(true)
            .session_retry_limit(3)
            .client()
            .unwrap();

        let endpoint: EndpointDescription = (
            endpoint,
            "None",
            MessageSecurityMode::None,
            UserTokenPolicy::anonymous(),
        )
            .into();

        let session = client
            .connect_to_endpoint(endpoint, IdentityToken::Anonymous)
            .unwrap();

        OPCUAHistoryRead {
            client,
            session,
            namespace
        }
    }
}

#[async_trait]
impl TimeSeriesQueryable for OPCUAHistoryRead {
    async fn execute(&mut self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        let session = self.session.write().unwrap();
        let start_time = find_time(tsq, &FindTime::Start);
        let end_time = find_time(tsq, &FindTime::End);

        let mut processed_details = None;
        let mut raw_modified_details = None;

        let mut colnames_identifiers = vec![];
        if let Some(grouping) = &tsq.grouping {
            processed_details = Some(create_read_processed_details(tsq, start_time, end_time));
            for c in tsq.ids.as_ref().unwrap() {
                for (v,_) in &grouping.aggregations {
                    colnames_identifiers.push((v.as_str().to_string(), c.clone()));
                }
            }

        } else {
            raw_modified_details = Some(create_raw_details(start_time, end_time));
            for c in tsq.ids.as_ref().unwrap() {
                colnames_identifiers.push((tsq.value_variable.as_ref().unwrap().variable.as_str().to_string(), c.clone()))
            }
        }

        let mut nodes_to_read_vec = vec![];
        for (_, id) in &colnames_identifiers {
            let hrvi = HistoryReadValueId{
                node_id: NodeId::new(self.namespace, Identifier::String(UAString::from(id.to_string()))),
                index_range: UAString::null(),
                data_encoding: QualifiedName::null(),
                continuation_point: ByteString::null(),
            };
            nodes_to_read_vec.push(hrvi);
        }
        //let series = vec![];
        let mut stopped = false;
        let mut dfs = vec![];
        while !stopped {
            let action = if let Some(d) = &processed_details {
                HistoryReadAction::ReadProcessedDetails(d.clone())
            } else if let Some(d) = &raw_modified_details {
                HistoryReadAction::ReadRawModifiedDetails(d.clone())
            } else {
                panic!("");
            };
            let resp = session.history_read(action, TimestampsToReturn::Source, false, nodes_to_read_vec.as_slice()).expect("");
            //First we set the new continuation points:
            for (i,h) in resp.iter().enumerate() {
                if h.continuation_point.is_null() {
                    if stopped {
                        panic!("Should not happen")
                    }
                    stopped = true;
                } else {
                    nodes_to_read_vec.get_mut(i).unwrap().continuation_point = h.continuation_point.clone();
                    todo!("Continuation points are just halfway implemented...");
                }
            }

            let mut series_map: HashMap<String, Vec<(Series, Series)>> = HashMap::new();

            //Now we process the data
            for (i, h) in resp.into_iter().enumerate() {
                let HistoryReadResult { status_code, continuation_point:_, history_data } = h;
                let (mut ts, mut val) = history_data_to_series_tuple(history_data.decode_inner::<HistoryData>(&Default::default()).unwrap());
                let (colname, id) = colnames_identifiers.get(i).unwrap();
                ts.rename(tsq.timestamp_variable.as_ref().unwrap().variable.as_str());
                val.rename(colname);
                if let Some(v) = series_map.get_mut(id) {
                    v.push((ts, val));
                } else {
                    series_map.insert(id.clone(), vec![(ts,val)]);
                }
            }
            let mut keys: Vec<String> = series_map.keys().map(|x|x.clone()).collect();
            keys.sort();
            for k in keys {
                let series_vec = series_map.remove(&k).unwrap();
                let mut first_ts = None;
                let mut value_vec = vec![];
                for (i, (ts, val)) in series_vec.into_iter().enumerate() {
                    if let Some(_) = &first_ts {
                    } else {
                        first_ts = Some(ts);
                    }
                    value_vec.push(val);
                };
                let mut identifier_series = Series::new_empty(tsq.identifier_variable.as_ref().unwrap().as_str(), &DataType::Utf8);
                identifier_series = identifier_series.extend_constant(AnyValue::Utf8(&k), first_ts.as_ref().unwrap().len()).unwrap();
                value_vec.push(identifier_series);
                value_vec.push(first_ts.unwrap());
                value_vec.sort_by_key(|x|x.name().to_string());
                dfs.push(DataFrame::new(value_vec).unwrap().lazy())
            }
        }

        Ok(concat(dfs, true).unwrap().collect().unwrap())
    }
}

fn create_raw_details(start_time:Option<DateTime>, end_time:Option<DateTime>) -> ReadRawModifiedDetails {
    ReadRawModifiedDetails {
        is_read_modified: false,
        start_time: start_time.unwrap(),
        end_time: end_time.unwrap(),
        num_values_per_node: 0,
        return_bounds: false
    }
}

fn create_read_processed_details(tsq: &TimeSeriesQuery, start_time:Option<DateTime>, end_time:Option<DateTime>) -> ReadProcessedDetails {
    let aggregate_type = find_aggregate_types(tsq);

    let config = AggregateConfiguration {
        use_server_capabilities_defaults: false,
        treat_uncertain_as_bad: false,
        percent_data_bad: 0,
        percent_data_good: 0,
        use_sloped_extrapolation: false,
    };
let interval_opt = find_grouping_interval(tsq);
        let processing_interval = if let Some(interval) = interval_opt {
            interval
        } else {
            0.0
        };

    let details = ReadProcessedDetails {
        start_time: start_time.unwrap_or(Default::default()),
        end_time: end_time.unwrap_or(Default::default()),
        processing_interval,
        aggregate_type,
        aggregate_configuration: config,
    };
    details
}

fn history_data_to_series_tuple(hd: HistoryData) -> (Series, Series) {
    let HistoryData { data_values } = hd;
    let data_values_vec = data_values.unwrap();
    let mut any_value_vec = vec![];
    let mut ts_value_vec = vec![];
    for data_value in data_values_vec {
        if let Some(ts) = data_value.source_timestamp {
            let polars_datetime = NaiveDateTime::from_timestamp(ts.as_chrono().timestamp(), 0);
            ts_value_vec.push(polars_datetime);
        }
        if let Some(val) = data_value.value {
            let any_value = match val {
                Variant::Double(d) => {AnyValue::Float64(d)},
                Variant::Int64(i) => {AnyValue::Int64(i)},
                _ => {todo!("Implement: {}", val)}
            };
            any_value_vec.push(any_value);
        }
    }
    let timestamps = Series::new("timestamp", ts_value_vec.as_slice());
    let values = Series::from_any_values("value", any_value_vec.as_slice()).unwrap();
    (timestamps, values)
}

fn find_aggregate_types(tsq: &TimeSeriesQuery) -> Option<Vec<NodeId>> {
    if let Some(grouping) = &tsq.grouping {
        let mut nodes = vec![];
        for (v, a) in &grouping.aggregations {
            let agg = &a.aggregate_expression;
            let value_var_str = tsq.value_variable.as_ref().unwrap().variable.as_str();
            let expr_is_ok = |expr: &Expression| -> bool {
                if let Expression::Variable(v) = expr {
                    v.as_str() == value_var_str
                } else {
                    false
                }
            };
            let aggfunc = match agg {
                AggregateExpression::Count { expr, distinct } => {
                    assert!(!distinct);
                    if let Some(e) = expr {
                        assert!(expr_is_ok(e));
                    }
                    NodeId {
                        namespace: 0,
                        identifier: Identifier::Numeric(OPCUA_AGG_FUNC_COUNT),
                    }
                }
                AggregateExpression::Sum { expr, distinct } => {
                    assert!(!distinct);
                    assert!(expr_is_ok(expr));
                    NodeId {
                        namespace: 0,
                        identifier: Identifier::Numeric(OPCUA_AGG_FUNC_TOTAL),
                    }
                }
                AggregateExpression::Avg { expr, distinct } => {
                    assert!(!distinct);
                    assert!(expr_is_ok(expr));
                    NodeId {
                        namespace: 0,
                        identifier: Identifier::Numeric(OPCUA_AGG_FUNC_AVERAGE),
                    }
                }
                AggregateExpression::Min { expr, distinct } => {
                    assert!(!distinct);
                    assert!(expr_is_ok(expr));
                    NodeId {
                        namespace: 0,
                        identifier: Identifier::Numeric(OPCUA_AGG_FUNC_MINIMUM),
                    }
                }
                AggregateExpression::Max { expr, distinct } => {
                    assert!(!distinct);
                    assert!(expr_is_ok(expr));
                    NodeId {
                        namespace: 0,
                        identifier: Identifier::Numeric(OPCUA_AGG_FUNC_MAXIMUM),
                    }
                }
                _ => {
                    panic!("Not supported {:?}, should not happen", agg)
                }
            };
            nodes.push(aggfunc);
        }
        Some(nodes)
    } else {
        None
    }
}

enum FindTime {
    Start,
    End,
}

fn find_time(tsq: &TimeSeriesQuery, find_time: &FindTime) -> Option<DateTime> {
    let mut found_time = None;
    for c in &tsq.conditions {
        let e = &c.expression;
        let found_time_opt = find_time_condition(
            &tsq.timestamp_variable.as_ref().unwrap().variable,
            e,
            find_time,
        );
        if found_time_opt.is_some() {
            if found_time.is_some() {
                panic!("Two duplicate conditions??");
            }
            found_time = found_time_opt;
        }
    }
    found_time
}

fn find_time_condition(
    timestamp_variable: &Variable,
    expr: &Expression,
    find_time: &FindTime,
) -> Option<DateTime> {
    match expr {
        Expression::And(left, right) => {
            let left_cond = find_time_condition(timestamp_variable, left, find_time);
            let right_cond = find_time_condition(timestamp_variable, right, find_time);
            if left_cond.is_some() && right_cond.is_some() {
                panic!("Not allowed");
            } else if let Some(cond) = left_cond {
                Some(cond)
            } else if let Some(cond) = right_cond {
                Some(cond)
            } else {
                None
            }
        }
        Expression::Greater(_, _) => {
            todo!("No support for strictly greater yet")
        }
        Expression::GreaterOrEqual(left, right) => {
            match find_time {
                FindTime::Start => {
                    //Must have form variable >= literal_date
                    if let Expression::Variable(v) = left.as_ref() {
                        if v == timestamp_variable {
                            datetime_from_expression(right)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                FindTime::End => {
                    //Must have form literal_date >= variable
                    if let Expression::Variable(v) = right.as_ref() {
                        if v == timestamp_variable {
                            datetime_from_expression(left)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            }
        }
        Expression::Less(_, _) => {
            todo!("No support for strictly less yet")
        }
        Expression::LessOrEqual(left, right) => {
            match find_time {
                FindTime::Start => {
                    //Must have form literal_date <= variable
                    if let Expression::Variable(v) = right.as_ref() {
                        if v == timestamp_variable {
                            datetime_from_expression(left)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                FindTime::End => {
                    //Must have form variable <= literal_date
                    if let Expression::Variable(v) = left.as_ref() {
                        if v == timestamp_variable {
                            datetime_from_expression(right)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            }
        }
        _ => None,
    }
}

fn datetime_from_expression(expr: &Expression) -> Option<DateTime> {
    if let Expression::Literal(lit) = expr {
        if lit.datatype() == xsd::DATE_TIME {
            if let Ok(dt) = lit.value().parse::<NaiveDateTime>() {
                let dt_with_tz_utc: ChronoDateTime<Utc> = Utc.from_utc_datetime(&dt);
                Some(DateTime::from(dt_with_tz_utc))
            } else if let Ok(dt) = lit.value().parse::<ChronoDateTime<Utc>>() {
                Some(DateTime::from(dt))
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}

fn find_grouping_interval(tsq: &TimeSeriesQuery) -> Option<f64> {
    if let Some(grouping) = &tsq.grouping {
        let mut tsf = None;
        for v in &grouping.by {
            for (t, e) in &grouping.timeseries_funcs {
                if t == v {
                    tsf = Some((t, &e.expression));
                }
            }
        }
        if let Some((_, e)) = tsf {
            if let Expression::Multiply(left, right) = e {
                if let (Expression::FunctionCall(f, args), Expression::Literal(_)) = (left.as_ref(),right.as_ref()) {
                    if f == &Function::Floor && args.len() == 1 {
                        if let Expression::Divide(left, right) = args.get(0).unwrap() {
                            if let (Expression::FunctionCall(f, args), Expression::Literal(lit)) =
                                (left.as_ref(), right.as_ref())
                            {
                                if let Function::Custom(nn) = f {
                                    if nn.as_str() == DATETIME_AS_SECONDS {
                                        return from_numeric_datatype(lit);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

fn from_numeric_datatype(lit: &Literal) -> Option<f64> {
    let dt = lit.datatype();
    if dt == xsd::UNSIGNED_INT
        || dt == xsd::UNSIGNED_LONG
        || dt == xsd::INT
        || dt == xsd::INTEGER
        || dt == xsd::LONG
    {
        let i: i32 = lit.value().parse().unwrap();
        Some(f64::from(i))
    } else if dt == xsd::FLOAT || dt == xsd::DOUBLE || dt == xsd::DECIMAL {
        let f: f64 = lit.value().parse().unwrap();
        Some(f)
    } else {
        None
    }
}

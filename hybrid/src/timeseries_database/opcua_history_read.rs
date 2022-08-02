use crate::constants::DATETIME_AS_SECONDS;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::TimeSeriesQuery;
use opcua_client::prelude::{DateTime, AggregateConfiguration, AttributeService, ByteString, Client, ClientBuilder, EndpointDescription, ExtensionObject, HistoryReadAction, HistoryReadResult, HistoryReadValueId, Identifier, IdentityToken, MessageSecurityMode, NodeId, QualifiedName, ReadProcessedDetails, Session, TimestampsToReturn, UAString, UserTokenPolicy};
use oxrdf::vocab::xsd;
use oxrdf::{Literal, Variable};
use polars::export::chrono::{DateTime as ChronoDateTime, NaiveDateTime, TimeZone, Utc};
use polars_core::frame::DataFrame;
use spargebra::algebra::{AggregateExpression, Expression, Function};
use std::error::Error;
use std::sync::{Arc, RwLock};
use polars_core::prelude::DataType;
use polars_core::series::Series;
use async_trait::async_trait;

const AVERAGE: u32 = 2342;
const COUNT: u32 = 2352;
const MINIMUM: u32 = 2346;
const MAXIMUM: u32 = 2347;
const TOTAL: u32 = 2344;

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

        let interval_opt = find_grouping_interval(tsq);
        let processing_interval = if let Some(interval) = interval_opt {
            interval
        } else {
            0.0
        };

        let aggregate_type = find_aggregate_types(tsq);

        let config = AggregateConfiguration {
            use_server_capabilities_defaults: false,
            treat_uncertain_as_bad: false,
            percent_data_bad: 0,
            percent_data_good: 0,
            use_sloped_extrapolation: false,
        };

        let details = ReadProcessedDetails {
            start_time: start_time.unwrap_or(Default::default()),
            end_time: end_time.unwrap_or(Default::default()),
            processing_interval,
            aggregate_type,
            aggregate_configuration: config,
        };
        let mut nodes_to_read_vec = vec![];
        for id in tsq.ids.as_ref().unwrap() {
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
        while !stopped {
            let resp = session.history_read(HistoryReadAction::ReadProcessedDetails(details.clone()), TimestampsToReturn::Source, false, nodes_to_read_vec.as_slice()).expect("");
            //First we set the new continuation points:
            for (i,h) in resp.iter().enumerate() {
                if h.continuation_point.is_null() {
                    if stopped {
                        panic!("Should not happen")
                    }
                    stopped = true;
                } else {
                    nodes_to_read_vec.get_mut(i).unwrap().continuation_point = h.continuation_point.clone();
                }
            }

            //Now we process the data
            for (i, h) in resp.into_iter().enumerate() {
                let HistoryReadResult { status_code, continuation_point:_, history_data } = h;
                println!("Status code: {}", status_code);
                let series = decode_history_data(history_data);
            }
        }
        Ok(DataFrame::new(vec![Series::new_empty("hello", &DataType::Float64)]).unwrap())
    }
}

fn decode_history_data(ex: ExtensionObject) -> Series {
    let ExtensionObject { node_id, body } = ex;
    println!("Exobj: {}", node_id);
    Series::new_empty("MySeries", &DataType::Float64)
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
                        identifier: Identifier::Numeric(COUNT),
                    }
                }
                AggregateExpression::Sum { expr, distinct } => {
                    assert!(!distinct);
                    assert!(expr_is_ok(expr));
                    NodeId {
                        namespace: 0,
                        identifier: Identifier::Numeric(TOTAL),
                    }
                }
                AggregateExpression::Avg { expr, distinct } => {
                    assert!(!distinct);
                    assert!(expr_is_ok(expr));
                    NodeId {
                        namespace: 0,
                        identifier: Identifier::Numeric(AVERAGE),
                    }
                }
                AggregateExpression::Min { expr, distinct } => {
                    assert!(!distinct);
                    assert!(expr_is_ok(expr));
                    NodeId {
                        namespace: 0,
                        identifier: Identifier::Numeric(MINIMUM),
                    }
                }
                AggregateExpression::Max { expr, distinct } => {
                    assert!(!distinct);
                    assert!(expr_is_ok(expr));
                    NodeId {
                        namespace: 0,
                        identifier: Identifier::Numeric(MAXIMUM),
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
                FindTime::End => {
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
            }
        }
        Expression::Less(_, _) => {
            todo!("No support for strictly less yet")
        }
        Expression::LessOrEqual(left, right) => {
            match find_time {
                FindTime::Start => {
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
                FindTime::End => {
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

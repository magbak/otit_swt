use opcua_server::prelude::*;
use polars::prelude::{col, lit, DataType as PolarsDataType, IntoLazy, Expr, AggExpr};
use polars::export::chrono::{DateTime as PolarsDateTime, Utc as PolarsUtc};
use polars_core::frame::DataFrame;
use polars_core::prelude::TimeUnit;
use std::collections::HashMap;
use std::ops::{Div, Mul};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use polars_core::datatypes::AnyValue;

const OPCUA_AGG_FUNC_AVERAGE: u32 = 2342;
const OPCUA_AGG_FUNC_COUNT: u32 = 2352;
const OPCUA_AGG_FUNC_MINIMUM: u32 = 2346;
const OPCUA_AGG_FUNC_MAXIMUM: u32 = 2347;
const OPCUA_AGG_FUNC_TOTAL: u32 = 2344;

pub struct OPCUADataProvider {
    pub frames: HashMap<String, DataFrame>,
}

impl OPCUADataProvider {
    pub fn new(frames: HashMap<String, DataFrame>) -> OPCUADataProvider {
        OPCUADataProvider { frames }
    }

    fn read(&self, nodes_to_read: &[HistoryReadValueId], aggregation_types:Option<Vec<NodeId>>, start_time:&DateTime, end_time:&DateTime, interval:Option<f64>) -> Result<Vec<HistoryReadResult>, StatusCode>{
        let mut results = vec![];
        for (i, n) in nodes_to_read.iter().enumerate() {
            let NodeId { namespace, identifier } = &n.node_id;
            let idstring= if let Identifier::String(uas) = identifier {
                uas.to_string()
            } else {panic!("")};
            let mut df = self.frames.get(&idstring).unwrap().clone();
            let mut lf = df.lazy();
            lf = lf.filter(col("timestamp").gt_eq(lit(start_time.as_chrono().to_string().parse::<PolarsDateTime<PolarsUtc>>().unwrap().naive_utc())));
            lf = lf.filter(col("timestamp").lt_eq(lit(end_time.as_chrono().to_string().parse::<PolarsDateTime<PolarsUtc>>().unwrap().naive_utc())));
            if let Some(aggregation_types) = &aggregation_types {
            lf = lf.with_column(
                col("timestamp")
                    .cast(PolarsDataType::Datetime(TimeUnit::Milliseconds, None))
                    .cast(PolarsDataType::UInt64)
                    .mul(lit(1000))
                    .alias("timestamp")
                    .div(lit(interval.unwrap()))
                    .floor()
                    .mul(lit(interval.unwrap()))
                    .cast(PolarsDataType::UInt64),
            );
            let lfgr = lf.groupby(&["timestamp"]);
                let agg_func = aggregation_types.get(i).unwrap();
                assert_eq!(agg_func.namespace, 0);
                let mut agg_col = None;
                if let Identifier::Numeric(agg_func_i) = &agg_func.identifier {
                    agg_col = Some(match agg_func_i {
                        &OPCUA_AGG_FUNC_AVERAGE => {
                            col("value").mean()
                        }
                        _ => {
                            unimplemented!("We do not support this aggregation function: {}", agg_func)
                        }
                    });
                }
                lf = lfgr.agg([agg_col.unwrap().alias("value")]);
            }
            df = lf.collect().unwrap();
            let mut ts_iter = df.column("timestamp").unwrap().iter();
            let mut v_iter = df.column("value").unwrap().iter();
            let mut data_values = vec![];
            for _ in 0..df.height() {
                let value_variant = match v_iter.next().unwrap() {
                    AnyValue::Float64(f) => {Variant::Double(f)}
                    _ => {todo!("Very rudimentary value type support!")}
                };

                let timestamp = match ts_iter.next().unwrap() {
                    AnyValue::Datetime(number, timeunit, _) => {
                        match timeunit {
                            TimeUnit::Nanoseconds => {
                                DateTime::from(number / 1_000_000_000)
                            }
                            TimeUnit::Microseconds => {
                                DateTime::from(number / 1_000_000)
                            }
                            TimeUnit::Milliseconds => {
                                DateTime::from(number / 1_000_000_000)
                            }
                        }
                    },
                    _ => {panic!("Something is not right!")}
                };

                let dv = DataValue{
                    value: Some(value_variant),
                    status: None,
                    source_timestamp: Some(timestamp),
                    source_picoseconds: None,
                    server_timestamp: None,
                    server_picoseconds: None
                };
                data_values.push(dv);
            }
            let h = HistoryData { data_values: Some(data_values) };
            let r = HistoryReadResult {
            status_code: StatusCode::Good,
            continuation_point: Default::default(),
            history_data: ExtensionObject::from_encodable(h.object_id(), &h),
            };
            results.push(r);
        }
        Ok(results)
    }
}

impl HistoricalDataProvider for OPCUADataProvider {
    fn read_raw_modified_details(&self, _address_space: Arc<RwLock<AddressSpace>>, request: ReadRawModifiedDetails, _timestamps_to_return: TimestampsToReturn, _release_continuation_points: bool, nodes_to_read: &[HistoryReadValueId]) -> Result<Vec<HistoryReadResult>, StatusCode> {
        self.read(nodes_to_read, None, &request.start_time, &request.end_time, None)
    }
    
    fn read_processed_details(
        &self,
        _address_space: Arc<RwLock<AddressSpace>>,
        request: ReadProcessedDetails,
        timestamps_to_return: TimestampsToReturn,
        _release_continuation_points: bool,
        nodes_to_read: &[HistoryReadValueId],
    ) -> Result<Vec<HistoryReadResult>, StatusCode> {
        self.read(nodes_to_read, Some(request.aggregate_type.unwrap()), &request.start_time, &request.end_time, Some(request.processing_interval))
    }

}

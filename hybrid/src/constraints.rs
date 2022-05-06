#[derive(PartialEq, Debug)]
pub enum Constraint {
    ExternalTimeseries,
    ExternalDataPoint,
    ExternalDataValue,
    ExternalTimestamp,
}
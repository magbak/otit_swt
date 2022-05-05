#[derive(PartialEq)]
pub enum Constraint {
    ExternalTimeseries,
    ExternalDataPoint,
    ExternalDataValue,
    ExternalTimestamp,
}
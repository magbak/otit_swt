#[derive(PartialEq, Debug, Clone)]
pub enum Constraint {
    ExternalTimeseries,
    ExternalDataPoint,
    ExternalDataValue,
    ExternalTimestamp,
    ExternallyDerived
}
use spargebra::term::NamedNode;

pub const HAS_TIMESTAMP: NamedNode =
    NamedNode::new("https://github.com/magbak/quarry-rs#hasTimestamp").unwrap();
pub const HAS_TIMESERIES: NamedNode =
    NamedNode::new("https://github.com/magbak/quarry-rs#hasTimeseries").unwrap();
pub const HAS_DATA_POINT: NamedNode =
    NamedNode::new("https://github.com/magbak/quarry-rs#hasDataPoint").unwrap();
pub const HAS_VALUE: NamedNode =
    NamedNode::new("https://github.com/magbak/quarry-rs#hasValue").unwrap();
pub const HAS_EXTERNAL_ID: NamedNode =
    NamedNode::new("https://github.com/magbak/quarry-rs#hasExternalId").unwrap();

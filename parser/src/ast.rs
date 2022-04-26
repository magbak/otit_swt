use std::time::Duration;
use chrono::{DateTime, Utc};

#[derive(PartialEq, Debug)]
pub enum ConnectiveType {
    Colon,
    Period,
    Semicolon,
    Dash,
    Slash,
    Backslash,
}

impl ConnectiveType {
    pub fn new(ctype: &char) -> ConnectiveType {
        match ctype {
            ':' => ConnectiveType::Colon,
            '.' => ConnectiveType::Period,
            ';' => ConnectiveType::Semicolon,
            '-' => ConnectiveType::Dash,
            '/' => ConnectiveType::Slash,
            '\\' => ConnectiveType::Backslash,
            _ => {
                panic!("Should only be called with valid connective type")
            }
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Connective {
    pub(crate) connective_type: ConnectiveType,
    pub(crate) number_of: usize,
}

impl Connective {
    pub fn new(ctype: &char, number_of: usize) -> Connective {
        Connective {
            connective_type: ConnectiveType::new(ctype),
            number_of,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum PathElementOrConnective {
    PathElement(PathElement),
    Connective(Connective),
}

#[derive(PartialEq, Debug)]
pub struct PathElement {
    pub glue: Option<Glue>,
    pub element: Option<ElementConstraint>,
}

impl PathElement {
    pub fn new(glue: Option<Glue>, element: Option<ElementConstraint>) -> PathElement {
        PathElement { glue, element }
    }
}

#[derive(PartialEq, Debug)]
pub struct Path {
    path: Vec<PathElementOrConnective>,
}

impl Path {
    pub fn new(p: Vec<PathElementOrConnective>) -> Path {
        Path { path: p }
    }

    pub fn prepend(&mut self, pe: PathElementOrConnective) {
        self.path.insert(0, pe);
    }
}

#[derive(PartialEq, Debug)]
pub enum BooleanOperator {
    NEQ,
    EQ,
    LTEQ,
    GTEQ,
    LT,
    GT,
    LIKE,
}

impl BooleanOperator {
    pub fn new(o: &str) -> BooleanOperator {
        match o {
            "!=" => BooleanOperator::NEQ,
            "=" => BooleanOperator::EQ,
            "<=" => BooleanOperator::LTEQ,
            ">=" => BooleanOperator::GTEQ,
            "<" => BooleanOperator::LT,
            ">" => BooleanOperator::GT,
            "LIKE" => BooleanOperator::LIKE,
            _ => {
                panic!("Unknown operator {:}", o)
            }
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum Literal {
    Real(f64),
    Integer(i32),
    String(String),
}

#[derive(PartialEq, Debug)]
pub enum PathOrLiteral {
    Path(Path),
    Literal(Literal),
}

#[derive(PartialEq, Debug)]
pub struct ConditionedPath {
    lhs_path: Path,
    boolean_operator: Option<BooleanOperator>,
    rhs_path_or_literal: Option<PathOrLiteral>,
}

impl ConditionedPath {
    pub fn new(
        lhs_path: Path,
        boolean_operator: BooleanOperator,
        rhs_path_or_literal: PathOrLiteral,
    ) -> ConditionedPath {
        ConditionedPath {
            lhs_path,
            boolean_operator:Some(boolean_operator),
            rhs_path_or_literal:Some(rhs_path_or_literal),
        }
    }

    pub fn from_path(lhs_path:Path) -> ConditionedPath {
        ConditionedPath{
            lhs_path,
            boolean_operator:None,
            rhs_path_or_literal:None
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Glue {
    id: String,
}

impl Glue {
    pub fn new(g: &str) -> Glue {
        Glue { id: g.to_string() }
    }
}

#[derive(PartialEq, Debug)]
pub enum ElementConstraint {
    Name(String),
    TypeName(String),
}

#[derive(PartialEq, Debug)]
pub struct GraphPattern {
    conditioned_paths: Vec<ConditionedPath>,
}

impl GraphPattern {
    pub fn new(conditioned_paths: Vec<ConditionedPath>) -> GraphPattern {
        GraphPattern { conditioned_paths }
    }
}

#[derive(PartialEq, Debug)]
pub struct Aggregation {
    function_name: String,
    duration: Duration,
}

impl Aggregation {
    pub fn new(function_name: &str, duration: Duration) -> Aggregation {
        Aggregation {
            function_name: function_name.to_string(),
            duration,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct TsQuery {
    graph_pattern: GraphPattern,
    from_datetime: DateTime<Utc>,
    to_datetime: DateTime<Utc>,
    aggregation: Aggregation,
}

impl TsQuery {
    pub fn new(
        graph_pattern: GraphPattern,
        from_datetime: DateTime<Utc>,
        to_datetime: DateTime<Utc>,
        aggregation: Aggregation,
    ) -> TsQuery {
        TsQuery {
            graph_pattern,
            from_datetime,
            to_datetime,
            aggregation,
        }
    }
}

pub enum DataType {
    String,
    Real,
    Integer,
}

impl DataType {
    pub fn new(dt:&str) -> DataType{
        match dt {
            "String" => DataType::String,
            "Real" => DataType::String,
            "Integer" => DataType::Integer,
            p => panic!("Invalid data type {:}", p)
        }
    }
}

pub struct Output {
    path: Vec<PathElement>,
    data_type: DataType
}

impl Output {
    pub fn new(path:Vec<PathElement>, data_type:DataType) -> Output {
        Output{path, data_type:data_type}
    }
}

pub struct Outputs {
    outputs: Vec<Output>
}

impl Outputs {
    pub fn new(outputs:Vec<Output>) -> Outputs {
        Outputs{outputs}
    }
}

pub struct TsApi {
    input: GraphPattern,
    outputs: Outputs
}

impl TsApi {
    pub fn new(input:GraphPattern, outputs:Outputs) -> TsApi {
        TsApi{input, outputs}
    }
}
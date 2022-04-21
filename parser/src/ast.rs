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
    Connective(Connective)
}

#[derive(PartialEq, Debug)]
pub struct PathElement {
    element: String,
}

impl PathElement {
    pub fn new(element: &str) -> PathElement {
        PathElement {
            element: element.to_string(),
        }
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
    String(String)
}

#[derive(PartialEq, Debug)]
pub enum PathOrLiteral {
    Path(Path),
    Literal(Literal)
}

#[derive(PartialEq, Debug)]
pub struct ConditionedPath {
    lhs_path:Path,
    boolean_operator: BooleanOperator,
    rhs_path_or_literal: PathOrLiteral
}

impl ConditionedPath {
    pub fn new(lhs_path:Path, boolean_operator:BooleanOperator, rhs_path_or_literal:PathOrLiteral) -> ConditionedPath {
        ConditionedPath{
            lhs_path,
            boolean_operator,
            rhs_path_or_literal
        }
    }
}
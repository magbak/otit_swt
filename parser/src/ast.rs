#[derive(PartialEq, Debug)]
pub enum ConnectiveType {
    COLON,
    PERIOD,
    SEMICOLON,
    DASH,
    SLASH,
    BACKSLASH
}

impl ConnectiveType {
    pub fn new(ctype:&char) -> ConnectiveType {
        match ctype {
            ':' => {ConnectiveType::COLON}
            '.' => {ConnectiveType::PERIOD}
            ';' => {ConnectiveType::SEMICOLON}
            '-' => {ConnectiveType::DASH}
            '/' => {ConnectiveType::SLASH}
            '\\' => {ConnectiveType::BACKSLASH}
            _ => {panic!("Should only be called with valid connective type")}
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Connective {
    connective_type:ConnectiveType,
    number_of:usize
}

impl Connective {
    pub fn new(ctype:&char, number_of:usize) -> Connective {
        Connective{ connective_type: ConnectiveType::new(ctype), number_of }
    }
}

#[derive(PartialEq, Debug)]
pub struct PathElementOrConnective {
    path_element:Option<PathElement>,
    connective:Option<Connective>
}

impl PathElementOrConnective {
    pub fn from_path_element(p:PathElement) -> PathElementOrConnective{
        PathElementOrConnective{ path_element: Some(p), connective: None }
    }
    pub fn from_connective(c:Connective) -> PathElementOrConnective {
        PathElementOrConnective{ path_element: None, connective: Some(c) }
    }
}

#[derive(PartialEq, Debug)]
pub struct PathElement {
    element:String
}

impl PathElement {
    pub fn new(element:&str) -> PathElement{
        PathElement{element:element.to_string()}
    }
}

#[derive(PartialEq, Debug)]
pub struct Path {
    path:Vec<PathElementOrConnective>
}

impl Path {
    pub fn new(p:Vec<PathElementOrConnective>) -> Path {
        Path{path:p}
    }

    pub fn prepend(&mut self, pe:PathElementOrConnective) {
        self.path.insert(0, pe);
    }
}


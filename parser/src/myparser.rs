extern crate nom;

use crate::ast::{Connective, Path, PathElement, PathElementOrConnective};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while};
use nom::character::complete::{alpha1, char};
use nom::character::is_alphabetic;
use nom::multi::many1;
use nom::sequence::{tuple};
use nom::IResult;

fn connective(c: &str) -> IResult<&str, Connective> {
    let connres:IResult<&str, Vec<(char)>> = alt((
        many1(char('.')),
        many1(char('-')),
        many1(char(':')),
        many1(char(';')),
        many1(char('/')),
        many1(char('\\')),
    ))(c);
    match connres {
        Ok((_,conns)) => {
            assert!(conns.len()>0);
            Ok((c,Connective::new(conns.get(0).unwrap(), conns.len())))
        }
        Err(e) => {Err(e)}
    }
}

fn path_element(p: &str) -> IResult<&str, PathElement> {
    let taken = alpha1(p);
    return match taken {
        Ok((_, name)) => {Ok((p, PathElement::new(name)))}
        Err(e) => {Err(e)}
    }
}

fn singleton_path(p: &str) -> IResult<&str, Path> {
    let el = path_element(p);
    match el {
        Ok((_,pe)) => {
            Ok((p,Path::new(vec![PathElementOrConnective::from_path_element(pe)])))
        }
        Err(e) => {Err(e)}
    }

}

fn path_triple(p: &str) -> IResult<&str, Path> {
    let tripl = tuple((path, connective, path_element))(p);
    match tripl {
        Ok((_, (mut pa,conn, pe))) => {
            let conn_or = PathElementOrConnective::from_connective(conn);
            let pe_or = PathElementOrConnective::from_path_element(pe);
            pa.push(conn_or);
            pa.push(pe_or);
            Ok((p, pa))
        }
        Err(e) => {Err(e)}
    }
}

fn path(p: &str) -> IResult<&str, Path> {
    alt((singleton_path, path_triple))(p)
}



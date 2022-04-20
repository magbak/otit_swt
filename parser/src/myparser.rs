extern crate nom;

use crate::ast::{Connective, Path, PathElement, PathElementOrConnective};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, take_while1};
use nom::character::complete::{alpha1, char};
use nom::character::is_alphabetic;
use nom::combinator::{complete, recognize};
use nom::multi::many1;
use nom::sequence::tuple;
use nom::IResult;

fn connective(c: &str) -> IResult<&str, Connective> {
    let (c, conns) = alt((
        many1(char('.')),
        many1(char('-')),
        many1(char(':')),
        many1(char(';')),
        many1(char('/')),
        many1(char('\\'))))(c)?;
   assert!(conns.len() > 0);
    Ok((c, Connective::new(conns.get(0).unwrap(), conns.len())))
}

fn path_element(p: &str) -> IResult<&str, PathElement> {
    let (p, name) = alpha1(p)?;
    Ok((p, PathElement::new(name)))
}

fn singleton_path(p: &str) -> IResult<&str, Path> {
    let el = path_element(p);
    match el {
        Ok((_, pe)) => Ok((
            p,
            Path::new(vec![PathElementOrConnective::from_path_element(pe)]),
        )),
        Err(e) => Err(e),
    }
}

fn path_triple(p: &str) -> IResult<&str, Path> {
    let (p,(pe, conn, mut pa)) = tuple((path_element, connective, path))(p)?;
    let conn_or = PathElementOrConnective::from_connective(conn);
    let pe_or = PathElementOrConnective::from_path_element(pe);
    pa.prepend(conn_or);
    pa.prepend(pe_or);
    Ok((p, pa))
}

fn path(p: &str) -> IResult<&str, Path> {
    alt((path_triple, singleton_path))(p)
}

#[test]
fn test_parse_path() {
    assert_eq!(connective("-"), Ok(("", Connective::new(&'-', 1))));
    assert_eq!(path_triple("Abc.cda"), Ok(("", Path::new(vec![]))));
}

extern crate nom;

use crate::ast::{BooleanOperator, ConditionedPath, Connective, ConnectiveType, Literal, Path, PathElement, PathElementOrConnective, PathOrLiteral};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, take_while1};
use nom::character::complete::{alpha1, char};
use nom::character::is_alphabetic;
use nom::combinator::{complete, recognize};
use nom::multi::many1;
use nom::sequence::{pair, tuple};
use nom::IResult;

fn connective(c: &str) -> IResult<&str, Connective> {
    let (c, conns) = alt((
        many1(char('.')),
        many1(char('-')),
        many1(char(':')),
        many1(char(';')),
        many1(char('/')),
        many1(char('\\')),
    ))(c)?;
    assert!(conns.len() > 0);
    Ok((c, Connective::new(conns.get(0).unwrap(), conns.len())))
}

fn path_element(p: &str) -> IResult<&str, PathElement> {
    let (p, name) = alpha1(p)?;
    Ok((p, PathElement::new(name)))
}

fn singleton_path(p: &str) -> IResult<&str, Path> {
    let (p, el) = path_element(p)?;
    Ok((
        p,
        Path::new(vec![PathElementOrConnective::from_path_element(el)]),
    ))
}

fn path_triple(p: &str) -> IResult<&str, Path> {
    let (p, (pe, conn, mut pa)) = tuple((path_element, connective, path))(p)?;
    let conn_or = PathElementOrConnective::from_connective(conn);
    let pe_or = PathElementOrConnective::from_path_element(pe);
    pa.prepend(conn_or);
    pa.prepend(pe_or);
    Ok((p, pa))
}

fn path(p: &str) -> IResult<&str, Path> {
    alt((path_triple, singleton_path))(p)
}

fn path_or_literal(pl: &str) -> IResult<&str, PathOrLiteral> {

}

fn conditionedPath(cp: &str) -> IResult<&str, ConditionedPath> {
    let (cp,(p, bop, pol) ) = tuple((path, boolean_operator, path_or_literal))(cp);
}

fn numeric_literal(l: &str) -> IResult<&str, Literal> {}

fn string_literal(s: &str) -> IResult<&str, Literal> {}

fn literal(l: &str) -> IResult<&str, Literal> {}

fn boolean_operator(o: &str) -> IResult<&str, BooleanOperator> {
    let (o, opstr) = alt((
            tag("="),
            tag("!="),
            tag(">"),
            tag("<"),
            tag(">="),
            tag("<="),
            tag("like")
        ))(o)?;
    Ok((o, BooleanOperator::new(opstr)))
}

fn condition(c: &str) -> IResult<&str, Condition> {
    let (c, cond) = pair(
        ws(boolean_operator),
        alt((literal, path))
    )(c);
}

#[test]
fn test_parse_path() {
    assert_eq!(connective("-"), Ok(("", Connective::new(&'-', 1))));
    assert_eq!(
        path("Abc.cda"),
        Ok((
            "",
            Path::new(vec![
                PathElementOrConnective::from_path_element(PathElement::new("Abc")),
                PathElementOrConnective::from_connective(Connective {
                    connective_type: ConnectiveType::PERIOD,
                    number_of: 1
                }),
                PathElementOrConnective::from_path_element(PathElement::new("cda"))
            ])
        ))
    );
}

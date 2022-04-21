extern crate nom;

use std::str::FromStr;
use crate::ast::{BooleanOperator, ConditionedPath, Connective, ConnectiveType, Literal, Path, PathElement, PathElementOrConnective, PathOrLiteral};
use nom::branch::alt;
use nom::bytes::complete::{tag};
use nom::character::complete::{alpha1, char, digit1, not_line_ending, space0};
use nom::combinator::opt;
use nom::multi::many1;
use nom::sequence::{delimited, pair, tuple};
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
        Path::new(vec![PathElementOrConnective::PathElement(el)]),
    ))
}

fn path_triple(p: &str) -> IResult<&str, Path> {
    let (p, (pe, conn, mut pa)) = tuple((path_element, connective, path))(p)?;
    let conn_or = PathElementOrConnective::Connective(conn);
    let pe_or = PathElementOrConnective::PathElement(pe);
    pa.prepend(conn_or);
    pa.prepend(pe_or);
    Ok((p, pa))
}

fn path(p: &str) -> IResult<&str, Path> {
    alt((path_triple, singleton_path))(p)
}

fn path_as_path_or_literal(p:&str) -> IResult<&str, PathOrLiteral> {
    let (p, path) = path(p)?;
    Ok((p, PathOrLiteral::Path(path)))
}

fn path_or_literal(pl: &str) -> IResult<&str, PathOrLiteral> {
    alt((path_as_path_or_literal, literal_as_path_or_literal))(pl)
}

fn conditioned_path(cp: &str) -> IResult<&str, ConditionedPath> {
    let (cp,(p, _, bop, _, pol) ) = tuple((path, space0, boolean_operator, space0, path_or_literal))(cp)?;
    Ok((cp, ConditionedPath::new(p, bop, pol)))
}

fn numeric_literal(l: &str) -> IResult<&str, Literal> {
    let (l, (num1, opt_num2)) = pair(digit1, opt(pair(tag("."), digit1)))(l)?;
    match opt_num2 {
        Some((dot, num2)) => Ok((l, Literal::Real(f64::from_str(&(num1.to_owned() + dot + num2)).expect("Failed to parse float64")))),
        None => Ok((l, Literal::Integer(i32::from_str(num1).expect("Failed to parse int32"))))
    }
}

fn string_literal(s: &str) -> IResult<&str, Literal> {
    let (s, lit) = delimited(tag("\""), not_line_ending, tag("\""))(s)?;
    Ok((s, Literal::String(lit.to_string())))
}

fn literal(l: &str) -> IResult<&str, Literal> {
    alt((numeric_literal, string_literal))(l)
}

fn literal_as_path_or_literal(l: &str) -> IResult<&str, PathOrLiteral> {
    let (l, lit) = literal(l)?;
    Ok((l, PathOrLiteral::Literal(lit)))
}

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

#[test]
fn test_parse_path() {
    assert_eq!(connective("-"), Ok(("", Connective::new(&'-', 1))));
    assert_eq!(
        path("Abc.cda"),
        Ok((
            "",
            Path::new(vec![
                PathElementOrConnective::PathElement(PathElement::new("Abc")),
                PathElementOrConnective::Connective(Connective {
                    connective_type: ConnectiveType::Period,
                    number_of: 1
                }),
                PathElementOrConnective::PathElement(PathElement::new("cda"))
            ])
        ))
    );
}

#[test]
fn test_parse_conditioned_path() {
    assert_eq!(
        conditioned_path("Abc.cda > 25"),
        Ok((
            "",
            ConditionedPath::new(Path::new(vec![]), BooleanOperator::NEQ, PathOrLiteral::Literal(Literal::Integer(25)))
        ))
    );
}

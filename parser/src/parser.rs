extern crate nom;

use crate::ast::{
    BooleanOperator, ConditionedPath, Connective, ConnectiveType, ElementConstraint, Glue, Literal,
    Path, PathElement, PathElementOrConnective, PathOrLiteral,
};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric0, alphanumeric1, char, digit1, not_line_ending, space0};
use nom::combinator::opt;
use nom::multi::many1;
use nom::sequence::{delimited, pair, tuple};
use nom::IResult;
use std::str::FromStr;

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

fn glue(g: &str) -> IResult<&str, PathElement> {
    let (g, gstr) = delimited(tag("["), alphanumeric1, tag("]"))(g)?;
    Ok((g, PathElement::new(Some(Glue::new(gstr)), None)))
}

fn name_constraint(n: &str) -> IResult<&str, ElementConstraint> {
    let (n, s) = delimited(tag("\""), alphanumeric1, tag("\""))(n)?;
    Ok((n, ElementConstraint::Name(s.to_string())))
}

fn type_constraint(t: &str) -> IResult<&str, ElementConstraint> {
    let (t, (f,s)) = pair(alpha1, alphanumeric0)(t)?;
    Ok((t, ElementConstraint::TypeName(f.to_string() + s)))
}

fn element_constraint(e: &str) -> IResult<&str, PathElement> {
    let (e, c) = alt((name_constraint, type_constraint))(e)?;
    Ok((e, PathElement::new(None, Some(c))))
}

fn glued_element(e: &str) -> IResult<&str, PathElement> {
    let (e, (g, c)) = pair(glue, element_constraint)(e)?;
    Ok((e, PathElement::new(g.glue, c.element)))
}

fn path_element(p: &str) -> IResult<&str, PathElement> {
    alt((glued_element, glue, element_constraint))(p)
}

fn singleton_path(p: &str) -> IResult<&str, Path> {
    let (p, el) = path_element(p)?;
    Ok((p, Path::new(vec![PathElementOrConnective::PathElement(el)])))
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

fn numeric_literal(l: &str) -> IResult<&str, Literal> {
    let (l, (num1, opt_num2)) = pair(digit1, opt(pair(tag("."), digit1)))(l)?;
    match opt_num2 {
        Some((dot, num2)) => Ok((
            l,
            Literal::Real(
                f64::from_str(&(num1.to_owned() + dot + num2)).expect("Failed to parse float64"),
            ),
        )),
        None => Ok((
            l,
            Literal::Integer(i32::from_str(num1).expect("Failed to parse int32")),
        )),
    }
}

fn string_literal(s: &str) -> IResult<&str, Literal> {
    println!("{:}", s);
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
        tag("like"),
    ))(o)?;
    Ok((o, BooleanOperator::new(opstr)))
}

fn path_as_path_or_literal(p: &str) -> IResult<&str, PathOrLiteral> {
    let (p, path) = path(p)?;
    Ok((p, PathOrLiteral::Path(path)))
}

fn path_or_literal(pl: &str) -> IResult<&str, PathOrLiteral> {
    alt((path_as_path_or_literal, literal_as_path_or_literal))(pl)
}

fn conditioned_path(cp: &str) -> IResult<&str, ConditionedPath> {
    let (cp, (p, _, bop, _, pol)) =
        tuple((path, space0, boolean_operator, space0, path_or_literal))(cp)?;
    Ok((cp, ConditionedPath::new(p, bop, pol)))
}

#[test]
fn test_parse_path() {
    assert_eq!(connective("-"), Ok(("", Connective::new(&'-', 1))));
    assert_eq!(
        path("Abc.\"cda\""),
        Ok((
            "",
            Path::new(vec![
                PathElementOrConnective::PathElement(PathElement::new(
                    None,
                    Some(ElementConstraint::TypeName("Abc".to_string()))
                )),
                PathElementOrConnective::Connective(Connective {
                    connective_type: ConnectiveType::Period,
                    number_of: 1
                }),
                PathElementOrConnective::PathElement(PathElement::new(
                    None,
                    Some(ElementConstraint::Name("cda".to_string()))
                ))
            ])
        ))
    );
}

#[test]
fn test_parse_conditioned_path_literal() {
    let lhs = Path::new(vec![
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("Abc".to_string())),
        )),
        PathElementOrConnective::Connective(Connective {
            connective_type: ConnectiveType::Period,
            number_of: 1,
        }),
        PathElementOrConnective::PathElement(PathElement::new(
            Some(Glue::new("mynode")),
            Some(ElementConstraint::Name("cda".to_string())),
        )),
    ]);
    assert_eq!(
        conditioned_path("Abc.[mynode]\"cda\" > 25"),
        Ok((
            "",
            ConditionedPath::new(
                lhs,
                BooleanOperator::GT,
                PathOrLiteral::Literal(Literal::Integer(25))
            )
        ))
    );
}

#[test]
fn test_parse_conditioned_path_other_path() {
    let lhs = Path::new(vec![
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("Abc".to_string())),
        )),
        PathElementOrConnective::Connective(Connective {
            connective_type: ConnectiveType::Period,
            number_of: 1,
        }),
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("cda".to_string())),
        )),
    ]);
    let rhs = Path::new(vec![
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("acadadad".to_string())),
        )),
        PathElementOrConnective::Connective(Connective {
            connective_type: ConnectiveType::Dash,
            number_of: 1,
        }),
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("bca".to_string())),
        )),
    ]);
    assert_eq!(
        conditioned_path("Abc.cda > acadadad-bca"),
        Ok((
            "",
            ConditionedPath::new(lhs, BooleanOperator::GT, PathOrLiteral::Path(rhs))
        ))
    );
}

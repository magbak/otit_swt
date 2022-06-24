extern crate nom;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric0, digit1};
use nom::combinator::opt;
use nom::IResult;
use nom::multi::many0;
use nom::sequence::tuple;
use oxrdf::NamedNode;
use oxrdf::Term::NamedNode;
use crate::ast::{Directive, Prefix};
use crate::ast::Directive::Prefix;

fn directive(d:&str) -> IResult<&str, Directive> {
    let (d, a) = alt((prefix_id_as_directive, base_as_directive, sparql_prefix_as_directive, sparql_base_as_directive))(d)?;
}

fn prefix_id_as_directive(p:&str) -> IResult<&str, Directive> {
    let (p, prefix) = prefix_id(p)?;
    Ok((p, Directive::Prefix(prefix)))
}

fn base_as_directive(b:&str) -> IResult<&str, Directive> {
    let (b, dir) = base(b)?;
    Ok((b, Directive::Base(dir)))
}

fn sparql_prefix_as_directive(s:&str) -> IResult<&str, Directive> {
    let (s, prefix) = sparql_prefix(s)?;
    Ok((s,Directive::SparqlPrefix(prefix)))
}

fn sparql_base_as_directive(s:&str) -> IResult<&str, Directive> {
    let (s, b) = sparql_base(s)?;
    Ok((s, Directive::SparqlBase(b)))
}

fn sparql_base(s:&str) -> IResult<&str, NamedNode> {
    let (s, (_, nn)) = tuple((tag("BASE"), iri_ref))(s)?;
    Ok((b, nn))
}

fn sparql_prefix(s:&str) -> IResult<&str, Prefix> {
    let (s, (_, name, iri)) = tuple((tag("PREFIX"), pname_ns, iri_ref))(s)?;
    Ok((s,Prefix{name, iri}))
}

fn base(b:&str) -> IResult<&str, NamedNode> {
    let (b, (_, nn)) = tuple((tag("BASE"), iri_ref))(b)?;
    Ok((b, nn))
}

fn prefix_id(p:&str) -> IResult<&str, Prefix> {
    let (p, (_, name, iri)) = tuple((tag("@prefix"), pname_ns, iri_ref))(p)?;
    Ok((p, Prefix{ name, iri }))
}

fn iri_ref(i:&str) -> IResult<&str, NamedNode> {
    let (i, (_, iri,_) ) = tuple((tag("<"), alphanumeric0, tag(">")) )(i)?;
    let nn = NamedNode::new(iri)?;
    Ok((i,nn))
}

fn pname_ns(p:&str) -> IResult<&str, String> {
    let (p, (optname, _)) = tuple((opt(pn_prefix), tag(":")))(p)?;
    let out = match optname {
        None => {"".to_string()}
        Some(name) => {name}
    };
    Ok((p, out))
}

fn pn_prefix(p:&str) -> IResult<&str, String> {
    let (p, (pbase, dotnot)) = tuple((pn_chars_base, opt(tuple((many0(alt((pn_chars, tag(".")))), pn_chars)))))(p)?;
    let out = match dotnot {
        None => {pbase.to_string()}
        Some((strvec, end)) => {concat!(pbase, strvec.join(""), end)}
    };
    Ok((p, out))
}

//Incomplete from specification
fn pn_chars(p:&str) -> IResult<&str, &str> {
    let (p, chrs) = alt((pn_chars_u, tag("-"), digit1))(p)?;
    Ok((p,chrs))
}

fn pn_chars_u(p:&str) -> IResult<&str, &str> {
    let (p, chrs) = alt((pn_chars_base, tag("_")))(p)?;
    Ok((p, chrs))
}

//Incomplete from specification
fn pn_chars_base(p:&str) -> IResult<&str, &str> {
    let (p, chrs) = alpha1(p)?;
    Ok((p,chrs))
}

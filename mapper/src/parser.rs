extern crate nom;

use crate::ast::{
    Annotation, Argument, BaseTemplate, ConstantLiteral, ConstantTerm, DefaultValue, Directive,
    Instance, ListExpanderType, PType, Parameter, Prefix, PrefixedName, ResolvesToNamedNode,
    Signature, Statement, StottrDocument, StottrTerm, StottrVariable, Template,
};
use nom::branch::alt;
use nom::bytes::complete::{escaped, is_not, tag};
use nom::character::complete::{alpha1, alphanumeric0, alphanumeric1, anychar, digit1, one_of};
use nom::combinator::opt;
use nom::multi::{count, many0, separated_list0, separated_list1};
use nom::sequence::{pair, tuple};
use nom::IResult;
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, Literal, NamedNode};

enum DirectiveStatement {
    Directive(Directive),
    Statement(Statement),
}

fn stOttrDoc(s: &str) -> IResult<&str, StottrDocument> {
    let (s, parts) = many0(alt((directive_as_union, statement_as_union)))(s)?;
    Ok((s, StottrDocument {}))
}

fn statement_as_union(s: &str) -> IResult<&str, DirectiveStatement> {
    let (s, statement) = statement(s)?;
    Ok((s, DirectiveStatement::Statement(statement)))
}

fn statement(s: &str) -> IResult<&str, Statement> {
    let (s, statement) = alt((
        signature_as_statement,
        template_as_statement,
        base_template_as_statement,
        instance_as_statement,
    ))(s)?;
    Ok((s, statement))
}

fn signature_as_statement(s: &str) -> IResult<&str, Statement> {
    let (s, sign) = signature(s)?;
    Ok((s, Statement::Signature(sign)))
}

fn signature(s: &str) -> IResult<&str, Signature> {
    let (s, (template_name, _, parameter_list, _, annotation_list)) = tuple((
        template_name,
        tag("["),
        separated_list1(tag(","), parameter),
        tag("]"),
        opt(annotation_list),
    ))(s)?;
    Ok((
        s,
        Signature {
            template_name,
            parameter_list,
            annotation_list,
        },
    ))
}

fn annotation_list(a: &str) -> IResult<&str, Vec<Annotation>> {
    let (a, li) = separated_list1(tag(","), annotation)(a)?;
    Ok((a, li))
}

fn annotation(a: &str) -> IResult<&str, Annotation> {
    let (a, (_, instance)) = tuple((tag("@@"), instance))(a)?;
    Ok((a, Annotation { instance }))
}

fn template_as_statement(t: &str) -> IResult<&str, Statement> {
    let (t, template) = template(t)?;
    Ok((t, Statement::Template(template)))
}

fn template(t: &str) -> IResult<&str, Template> {
    let (t, (signature, _, pattern_list)) = tuple((signature, tag("::"), pattern_list))(t)?;
    Ok((
        t,
        Template {
            signature,
            pattern_list,
        },
    ))
}

fn template_name(t: &str) -> IResult<&str, ResolvesToNamedNode> {
    let (t, tn) = iri(t)?;
    Ok((t, tn))
}

fn base_template_as_statement(b: &str) -> IResult<&str, Statement> {
    let (b, t) = base_template(b)?;
    Ok((b, Statement::BaseTemplate(t)))
}

fn base_template(b: &str) -> IResult<&str, BaseTemplate> {
    let (b, (signature, _, _)) = tuple((signature, tag("::"), tag("BASE")))(b)?;
    Ok((b, BaseTemplate { signature }))
}

fn instance_as_statement(i: &str) -> IResult<&str, Statement> {
    let (i, instance) = instance(i)?;
    Ok((i, Statement::Instance(instance)))
}

fn instance(i: &str) -> IResult<&str, Instance> {
    let (i, (expander,)) = tuple(
        (opt(tuple((
            list_expander,
            tag("/"),
            template_name,
            argument_list,
        )))),
    )(i)?;

}

fn list_expander(l: &str) -> IResult<&str, ListExpanderType> {
    let (l, exp) = alt((tag("cross"), tag("zipMin"), tag("zipMax")))(l)?;
    let expander_type = ListExpanderType::from(exp);
    Ok((l, expander_type))
}

fn argument_list(a: &str) -> IResult<&str, Vec<Argument>> {
    let (a, (_, l, _)) = tuple((tag("("), separated_list0(tag(","), argument), tag(")")))(a)?;
    Ok((a, l))
}

fn argument(a: &str) -> IResult<&str, Argument> {
    let (a, ((list_expand, term))) = tuple((opt(list_expand), term))(a)?;
    Ok((a, Argument { list_expand:list_expand.is_some(), term }))
}

fn term(t: &str) -> IResult<&str, StottrTerm> {
    let (t, term) = alt((variable_as_term, constant_term_as_term, list_as_term))(t)?;
    Ok((t, term))
}

fn variable_as_term(v: &str) -> IResult<&str, StottrTerm> {
    let (v, var) = variable(v)?;
    Ok((v, StottrTerm::Variable(var)))
}

fn constant_term_as_term(c: &str) -> IResult<&str, StottrTerm> {
    let (c, con) = constant_term(c)?;
    Ok((c, StottrTerm::ConstantTerm(con)))
}

fn list_as_term(l: &str) -> IResult<&str, StottrTerm> {
    let (l, li) = list(l)?;
    Ok((l, StottrTerm::List(li)))
}

fn list(l: &str) -> IResult<&str, Vec<StottrTerm>> {
    let (l, (_, li, _)) = tuple((tag("("), separated_list0(tag(","), term), tag(")")))(l)?;
    Ok((l, li))
}

fn list_expand(l: &str) -> IResult<&str, &str> {
    let (l, expand) = tag("++")(l)?;
    Ok((l, expand))
}

fn pattern_list(p: &str) -> IResult<&str, Vec<Instance>> {
    let (p, (_, ilist, _)) = tuple((tag("{"), separated_list0(tag(","), instance), tag("}")))(p)?;
    Ok((p, ilist))
}

fn parameter(p: &str) -> IResult<&str, Parameter> {
    let (p, (pmode, ptype, variable, default_value)) = tuple((
        many0(alt((tag("!"), tag("?")))),
        opt(ptype),
        variable,
        opt(default_value),
    ))(p)?;
    //Todo check duplicate modes..
    let mut optional = false;
    let mut non_blank = false;
    if pmode.contains(&"!") {
        non_blank = true;
    }
    if pmode.contains(&"?") {
        optional = true;
    }

    Ok((
        p,
        Parameter {
            optional,
            non_blank,
            ptype,
            stottr_variable: variable,
            default_value,
        },
    ))
}

fn ptype(p: &str) -> IResult<&str, PType> {
    let (p, t) = alt((list_type, ne_list_type, lub_type, basic_type))(p)?;
    Ok((p, t))
}

fn list_type(l: &str) -> IResult<&str, PType> {
    let (l, (_, t, _)) = tuple((tag("List<"), ptype, tag(">")))(l)?;
    Ok((l, PType::ListType(Box::new(t))))
}

fn ne_list_type(l: &str) -> IResult<&str, PType> {
    let (l, (_, t, _)) = tuple((tag("NEList<"), ptype, tag(">")))(l)?;
    Ok((l, PType::NEListType(Box::new(t))))
}

fn lub_type(l: &str) -> IResult<&str, PType> {
    let (l, (_, t, _)) = tuple((tag("LUB<"), basic_type, tag(">")))(l)?;
    Ok((l, PType::LUBType(Box::new(t))))
}

fn basic_type(b: &str) -> IResult<&str, PType> {
    let (b, t) = prefixed_name(b)?;
    Ok((b, PType::BasicType(t)))
}

fn variable(v: &str) -> IResult<&str, StottrVariable> {
    let (v, (_, name)) = tuple((tag("?"), b_node_label))(v)?;
    Ok((v, StottrVariable { name }))
}

fn default_value(d: &str) -> IResult<&str, DefaultValue> {
    let (d, (_, constant_term)) = tuple((tag("="), constant_term))(d)?;
    Ok((d, DefaultValue { constant_term }))
}

fn constant_term(c: &str) -> IResult<&str, ConstantTerm> {
    let (c, t) = alt((constant_literal, constant_list))(c)?;
    Ok((c,t))
}

fn constant_list(c: &str) -> IResult<&str, Vec<ConstantTerm>> {
    let (c, (_, li, _)) = tuple((tag("("), separated_list0(tag(","), constant_term), tag(")")))(c)?;
    Ok((c, li))
}

fn constant_literal(c: &str) -> IResult<&str, ConstantTerm> {
    let (c, t) = alt((
        iri_as_constant_literal,
        blank_node_as_constant_literal,
        literal_as_constant_literal,
        none_as_constant_literal,
    ))(c)?;
    Ok((c,t))
}

fn none_as_constant_literal(n: &str) -> IResult<&str, ConstantLiteral> {
    let (n, _) = tag("none")(n)?;
    Ok((n, ConstantLiteral::None))
}

fn literal_as_constant_literal(l: &str) -> IResult<&str, ConstantLiteral> {
    let (l, lit) = literal(l)?;
    Ok((l, ConstantLiteral::Literal(lit)))
}

fn iri_as_constant_literal(i: &str) -> IResult<&str, ConstantLiteral> {
    let (i, iri) = iri(i)?;
    Ok((i, ConstantLiteral::IRI(iri)))
}

fn blank_node_as_constant_literal(b: &str) -> IResult<&str, ConstantLiteral> {
    let (b, blank) = blank_node(b)?;
    Ok((b, ConstantLiteral::BlankNode(blank)))
}

fn blank_node(b: &str) -> IResult<&str, BlankNode> {
    let (b, bn) = alt((blank_node_label, anon))(b)?;
    Ok((b, BlankNode::new(bn).expect("Blank node id problem")))
}

fn anon(a: &str) -> IResult<&str, ()> {
    let (a, _) = tuple((tag("["), tag("]")))(a)?;
    Ok((a, ()))
}

fn blank_node_label(b: &str) -> IResult<&str, String> {
    let (b, (_, mid, end)) = tuple((
        tag("_:"),
        alt((pn_chars_u, one_digit)),
        opt(tuple((many0(alt((pn_chars, tag(".")))), pn_chars))),
    ))(b)?;
    let rhs = if let Some((mut a, b)) = end {
        a.push(b);
        a.join("")
    } else {
        "".to_string()
    };
    let out = mid.to_string() + &rhs;
    Ok((b, out))
}

fn literal(l: &str) -> IResult<&str, Literal> {
    let (l, lit) = alt((rdf_literal, numeric_literal, boolean_literal))(l)?;
    Ok((l, lit))
}

fn boolean_literal(b: &str) -> IResult<&str, Literal> {
    let (b, lit) = alt((tag("true"), tag("false")))(b)?;
    Ok((b, Literal::new_typed_literal(lit, xsd::BOOLEAN)))
}

fn numeric_literal(n: &str) -> IResult<&str, Literal> {
    let (n, numeric) = alt((turtle_integer, turtle_decimal, turtle_double))(n)?;
    Ok((n, numeric))
}

fn turtle_integer(i: &str) -> IResult<&str, Literal> {
    todo!()
}

fn turtle_decimal(i: &str) -> IResult<&str, Literal> {
    todo!()
}
fn turtle_double(i: &str) -> IResult<&str, Literal> {
    todo!()
}

fn rdf_literal(r: &str) -> IResult<&str, Literal> {
    let (r, lit) = alt((rdf_literal_lang_tag, rdf_literal_iri))(r)?;
    Ok((r, lit))
}

fn rdf_literal_lang_tag(r: &str) -> IResult<&str, Literal> {
    let (r, (value, language)) = tuple((string, lang_tag))(r)?;
    Ok((
        r,
        Literal::new_language_tagged_literal_unchecked(value, language),
    ))
}

fn rdf_literal_iri(r: &str) -> IResult<&str, Literal> {
    let (r, (value, _, language)) = tuple((string, tag("^^"), iri))(r)?;
    Ok((
        r,
        Literal::new_language_tagged_literal_unchecked(value, language),
    ))
}

fn lang_tag(l: &str) -> IResult<&str, String> {
    let (l, (_, language, dashthings)) =
        tuple((tag("@"), alpha1, many0(tuple((tag("-"), alphanumeric1)))))(l)?;
    let mut out = vec![language];
    for (dash, al) in dashthings {
        out.push(dash);
        out.push(al);
    }
    Ok((l, out.join("")))
}

fn string(s: &str) -> IResult<&str, &str> {
    let (s, sl) = alt((
        string_literal_quote,
        string_literal_single_quote,
        string_literal_long_single_quote,
        string_literal_long_quote,
    ))(s)?;
    Ok((s, sl))
}

fn string_literal_quote(s: &str) -> IResult<&str, &str> {
    let (s, (_, esc, _)) = tuple((
        tag("\""),
        escaped(many0(is_not("\"\\\n\r")), '\\', escapable_echar),
        tag("\""),
    ))(s)?;
    Ok((s, esc))
}

fn string_literal_single_quote(s: &str) -> IResult<&str, &str> {
    let (s, (_, esc, _)) = tuple((
        tag("'"),
        escaped(many0(is_not("\'\\\n\r")), '\\', escapable_echar),
        tag("\""),
    ))(s)?;
    Ok((s, esc))
}
fn string_literal_long_quote(s: &str) -> IResult<&str, &str> {
    let (s, (_, esc, _)) = tuple((
        tag("\"\"\""),
        escaped(
            many0(tuple((alt((opt(tag("\"")), opt(tag("\"\"")))), is_not("\"\\")))),
            '\\',
            escapable_echar,
        ),
        tag("'''"),
    ))(s)?;
    Ok((s, esc))
}
fn string_literal_long_single_quote(s: &str) -> IResult<&str, &str> {
    let (s, (_, esc, _)) = tuple((
        tag("'''"),
        escaped(
            many0(tuple((alt((opt(tag("'")), opt(tag("''")))), is_not("'\\")))),
            '\\',
            escapable_echar,
        ),
        tag("'''"),
    ))(s)?;
    Ok((s, esc))
}

fn not_(u: &str) -> IResult<&str, &str> {
    let (u, c) = anychar(u)?;
}

fn escapable_echar(e: &str) -> IResult<&str, String> {
    let (e, c) = one_of(r#"tbnrf"'\"#)(e)?;
    Ok((e, c.to_string()))
}

fn b_node_label(b: &str) -> IResult<&str, String> {
    let (b, (first, mut list)) = pair(pn_chars_u, separated_list0(tag("."), pn_chars))(b)?;
    let first_string = first.to_string();

    Ok((b, first_string + &list.join(".")))
}

fn directive_as_union(d: &str) -> IResult<&str, DirectiveStatement> {
    let (d, directive) = directive(d)?;
    Ok((d, DirectiveStatement::Directive(directive)))
}

fn directive(d: &str) -> IResult<&str, Directive> {
    let (d, a) = alt((
        prefix_id_as_directive,
        base_as_directive,
        sparql_prefix_as_directive,
        sparql_base_as_directive,
    ))(d)?;
    Ok((d,a))
}

fn prefix_id_as_directive(p: &str) -> IResult<&str, Directive> {
    let (p, prefix) = prefix_id(p)?;
    Ok((p, Directive::Prefix(prefix)))
}

fn base_as_directive(b: &str) -> IResult<&str, Directive> {
    let (b, dir) = base(b)?;
    Ok((b, Directive::Base(dir)))
}

fn sparql_prefix_as_directive(s: &str) -> IResult<&str, Directive> {
    let (s, prefix) = sparql_prefix(s)?;
    Ok((s, Directive::SparqlPrefix(prefix)))
}

fn sparql_base_as_directive(s: &str) -> IResult<&str, Directive> {
    let (s, b) = sparql_base(s)?;
    Ok((s, Directive::SparqlBase(b)))
}

fn sparql_base(s: &str) -> IResult<&str, NamedNode> {
    let (s, (_, nn)) = tuple((tag("BASE"), iri_ref))(s)?;
    Ok((s, nn))
}

fn sparql_prefix(s: &str) -> IResult<&str, Prefix> {
    let (s, (_, name, iri)) = tuple((tag("PREFIX"), pname_ns, iri_ref))(s)?;
    Ok((s, Prefix { name, iri }))
}

fn base(b: &str) -> IResult<&str, NamedNode> {
    let (b, (_, nn)) = tuple((tag("BASE"), iri_ref))(b)?;
    Ok((b, nn))
}

fn prefix_id(p: &str) -> IResult<&str, Prefix> {
    let (p, (_, name, iri)) = tuple((tag("@prefix"), pname_ns, iri_ref))(p)?;
    Ok((p, Prefix { name, iri }))
}

fn iri(i: &str) -> IResult<&str, ResolvesToNamedNode> {
    let (i, rtnn) = alt((iri_ref_as_resolves, prefixed_name_as_resolves))(i)?;
    Ok((i, rtnn))
}

fn prefixed_name_as_resolves(p: &str) -> IResult<&str, ResolvesToNamedNode> {
    let (p, pn) = prefixed_name(p)?;
    Ok((p, ResolvesToNamedNode::PrefixedName(pn)))
}

fn prefixed_name(p: &str) -> IResult<&str, PrefixedName> {
    let (p, pn) = alt((pname_ln, pname_ns))(p)?;
    Ok((p, PrefixedName{ prefix: pn, name: "".to_string() }))
}

fn iri_ref_as_resolves(i: &str) -> IResult<&str, ResolvesToNamedNode> {
    let (i, nn) = iri_ref(i)?;
    Ok((i, ResolvesToNamedNode::NamedNode(nn)))
}

fn iri_ref(i: &str) -> IResult<&str, NamedNode> {
    let (i, (_, iri, _)) = tuple((tag("<"), alphanumeric0, tag(">")))(i)?;
    let nn = NamedNode::new(iri).expect("Invalid IRI");
    Ok((i, nn))
}

fn pname_ns(p: &str) -> IResult<&str, String> {
    let (p, (optname, _)) = tuple((opt(pn_prefix), tag(":")))(p)?;
    let out = match optname {
        None => "".to_string(),
        Some(name) => name,
    };
    Ok((p, out))
}

fn pname_ln(p: &str) -> IResult<&str, Prefix> {
    let (p, (name, local)) = tuple((pname_ns, pn_local))(p)?;
    Ok((p, Prefix{name, iri: NamedNode::new(local).expect("IRI problem") }))
}

//Incomplete from specification
fn pn_chars_base(p: &str) -> IResult<&str, &str> {
    let (p, chrs) = alpha1(p)?;
    Ok((p, chrs))
}

fn pn_chars_u(p: &str) -> IResult<&str, &str> {
    let (p, chrs) = alt((pn_chars_base, tag("_")))(p)?;
    Ok((p, chrs))
}

//Incomplete from specification
fn pn_chars(p: &str) -> IResult<&str, &str> {
    let (p, chrs) = alt((pn_chars_u, tag("-"), digit1))(p)?;
    Ok((p, chrs))
}

fn pn_prefix(p: &str) -> IResult<&str, String> {
    let (p, (pbase, dotnot)) = tuple((
        pn_chars_base,
        opt(tuple((many0(alt((pn_chars, tag(".")))), pn_chars))),
    ))(p)?;
    let out = match dotnot {
        None => pbase.to_string(),
        Some((mut strvec, end)) => {
            strvec.insert(0, pbase);
            strvec.push(end);
            strvec.join("")
        }
    };
    Ok((p, out))
}

//TODO: Big errors here..
fn pn_local(p: &str) -> IResult<&str, String> {
    let (p, s) = tuple((alt((pn_chars_u, tag(":"), one_digit, plx))))(p)?;
    Ok((p, s))
}

fn plx(p: &str) -> IResult<&str, String> {
    let (p, plx) = alt((percent, pn_local_esc))(p)?;
    Ok((p,plx))
}

fn percent(p: &str) -> IResult<&str, String> {
    let (p, (_, h)) = tuple((tag("%"), count(one_hex, 2)))(p)?;
    Ok((p, h.join("")))
}

fn one_digit(d: &str) -> IResult<&str, &str> {
    let (d, digit) = one_of("0123456789")(d)?;
    Ok((d, &digit.to_string()))
}

fn pn_local_esc(s: &str) -> IResult<&str, String> {
    let esc = r#"\(_~.-!$&\()*+,;=/?#@%"#;
    let (s, c) = one_of(esc)(s)?;
    Ok((s,c.to_string()))
}

fn one_hex(h: &str) -> IResult<&str, &str> {
    let (h, hex) = alt((one_digit, one_of("abcdefABCDEF")))(h)?;
    Ok((h, hex))
}

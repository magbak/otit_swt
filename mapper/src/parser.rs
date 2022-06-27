extern crate nom;

use crate::ast::{
    Annotation, Argument, BaseTemplate, ConstantLiteral, ConstantTerm, DefaultValue, Directive,
    Instance, ListExpanderType, PType, Parameter, Prefix, PrefixedName, ResolvesToNamedNode,
    Signature, Statement, StottrDocument, StottrLiteral, StottrTerm, StottrVariable, Template,
};
use nom::branch::alt;
use nom::bytes::complete::{escaped, is_not, tag};
use nom::character::complete::{alpha1, alphanumeric1, char, multispace0, multispace1, one_of};
use nom::combinator::{opt, peek};
use nom::multi::{count, many0, many1, separated_list0, separated_list1};
use nom::sequence::{pair, tuple};
use nom::{Finish, IResult};
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, NamedNode};

enum DirectiveStatement {
    Directive(Directive),
    Statement(Statement),
}

pub fn stottr_doc(s: &str) -> IResult<&str, StottrDocument> {
    let (s, parts) = many0(tuple((
        multispace0,
        alt((directive_as_union, statement_as_union)),
        multispace0,
    )))(s)?;
    let mut directives = vec![];
    let mut statements = vec![];
    for (_, p, _) in parts {
        match p {
            DirectiveStatement::Directive(d) => {
                directives.push(d);
            }
            DirectiveStatement::Statement(s) => {
                statements.push(s);
            }
        }
    }
    Ok((
        s,
        StottrDocument {
            directives,
            statements,
        },
    ))
}

fn statement_as_union(s: &str) -> IResult<&str, DirectiveStatement> {
    let (s, statement) = statement(s)?;
    Ok((s, DirectiveStatement::Statement(statement)))
}

fn statement(s: &str) -> IResult<&str, Statement> {
    let (s, (statement, _,_)) = tuple((alt((
        template_as_statement,
        instance_as_statement,
        signature_as_statement,
        base_template_as_statement,
    )), multispace0, tag(".")))(s)?;
    Ok((s, statement))
}

fn signature_as_statement(s: &str) -> IResult<&str, Statement> {
    let (s, sign) = signature(s)?;
    Ok((s, Statement::Signature(sign)))
}

fn signature(s: &str) -> IResult<&str, Signature> {
    let (s, (template_name, _, _, _, parameter_list, _, _, _, annotation_list)) = tuple((
        template_name,
        multispace0,
        tag("["),
        multispace0,
        separated_list1(tag(","), parameter),
        multispace0,
        tag("]"),
        multispace0,
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
    let (a, (_, _, instance, _)) = tuple((multispace0, tag("@@"), instance, multispace0))(a)?;
    Ok((a, Annotation { instance }))
}

fn template_as_statement(t: &str) -> IResult<&str, Statement> {
    let (t, template) = template(t)?;
    Ok((t, Statement::Template(template)))
}

fn template(t: &str) -> IResult<&str, Template> {
    let (t, (signature, _, _, _, pattern_list)) =
        tuple((signature, multispace0, tag("::"), multispace0, pattern_list))(t)?;
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
    let (b, (signature, _, _, _, _)) =
        tuple((signature, multispace0, tag("::"), multispace0, tag("BASE")))(b)?;
    Ok((b, BaseTemplate { signature }))
}

fn instance_as_statement(i: &str) -> IResult<&str, Statement> {
    let (i, instance) = instance(i)?;
    Ok((i, Statement::Instance(instance)))
}

fn instance(i: &str) -> IResult<&str, Instance> {
    let (i, (_, expander, template_name, _, argument_list, _)) = tuple((
        multispace0,
        opt(tuple((list_expander, tag("/")))),
        template_name,
        multispace0,
        argument_list,
        multispace0,
    ))(i)?;
    let mut exp = None;
    if let Some((some_exp, _)) = expander {
        exp = Some(some_exp)
    }
    Ok((
        i,
        Instance {
            list_expander: exp,
            template_name,
            argument_list,
        },
    ))
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
    let (a, (_, list_expand, term, _)) =
        tuple((multispace0, opt(list_expand), term, multispace0))(a)?;
    Ok((
        a,
        Argument {
            list_expand: list_expand.is_some(),
            term,
        },
    ))
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
    let only_variable_and_opt_default = || { tuple((
        multispace0,
        variable,
        multispace0,
        opt(default_value),
        multispace0,
    ))};
    if let Ok(_) = peek(only_variable_and_opt_default())(p).finish() {
        let (p, (_, stottr_variable, _, default_value, _)) = only_variable_and_opt_default()(p)?;
        Ok((
            p,
            Parameter {
                optional: false,
                non_blank: false,
                ptype: None,
                stottr_variable,
                default_value,
            },
        ))
    } else {
        let (p, (_, pmode, _, ptype, _, variable, _, default_value, _)) = tuple((
            multispace0,
            alt((tag("?!"), tag("!?"), tag("?"), tag("!"))),
            multispace1,
            opt(ptype),
            multispace1,
            variable,
            multispace0,
            opt(default_value),
            multispace0,
        ))(p)?;
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
    let (d, (_, _, constant_term)) = tuple((tag("="), multispace0, constant_term))(d)?;
    Ok((d, DefaultValue { constant_term }))
}

fn constant_term(c: &str) -> IResult<&str, ConstantTerm> {
    let (c, t) = alt((constant_literal_as_term, constant_term_list))(c)?;
    Ok((c, t))
}

fn constant_term_list(c: &str) -> IResult<&str, ConstantTerm> {
    let (c, (_, li, _)) = tuple((tag("("), separated_list0(tag(","), constant_term), tag(")")))(c)?;
    Ok((c, ConstantTerm::ConstantList(li)))
}

fn constant_literal_as_term(c: &str) -> IResult<&str, ConstantTerm> {
    let (c, lit) = constant_literal(c)?;
    Ok((c, ConstantTerm::Constant(lit)))
}

fn constant_literal(c: &str) -> IResult<&str, ConstantLiteral> {
    let (c, t) = alt((
        iri_as_constant_literal,
        blank_node_as_constant_literal,
        literal_as_constant_literal,
        none_as_constant_literal,
    ))(c)?;
    Ok((c, t))
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

fn anon(a: &str) -> IResult<&str, String> {
    let (a, _) = tuple((tag("["), tag("]")))(a)?;
    Ok((a, "".to_string()))
}

fn blank_node_label(b: &str) -> IResult<&str, String> {
    let (b, (_, startchar, opt_period, period_sep_list)) = tuple((
        tag("_:"),
        alt((pn_chars_u, one_digit)),
        opt(tag(".")),
        separated_list0(tag("."), many1(pn_chars)),
    ))(b)?;
    let mut out = startchar.to_string();
    if let Some(period) = opt_period {
        out += &period.to_string();
    }
    let stringvec: Vec<String> = period_sep_list
        .iter()
        .map(|x| x.iter().collect::<String>())
        .collect();
    out += &stringvec.join(".");
    Ok((b, out))
}

fn literal(l: &str) -> IResult<&str, StottrLiteral> {
    let (l, lit) = alt((rdf_literal, numeric_literal, boolean_literal))(l)?;
    Ok((l, lit))
}

fn boolean_literal(b: &str) -> IResult<&str, StottrLiteral> {
    let (b, value) = alt((tag("true"), tag("false")))(b)?;
    Ok((
        b,
        StottrLiteral {
            value: value.to_string(),
            language: None,
            data_type_iri: Some(ResolvesToNamedNode::NamedNode(xsd::BOOLEAN.into_owned())),
        },
    ))
}

fn numeric_literal(n: &str) -> IResult<&str, StottrLiteral> {
    let (n, numeric) = alt((turtle_integer, turtle_decimal, turtle_double))(n)?;
    Ok((n, numeric))
}

fn turtle_integer(i: &str) -> IResult<&str, StottrLiteral> {
    todo!()
}

fn turtle_decimal(i: &str) -> IResult<&str, StottrLiteral> {
    todo!()
}
fn turtle_double(i: &str) -> IResult<&str, StottrLiteral> {
    todo!()
}

fn rdf_literal(r: &str) -> IResult<&str, StottrLiteral> {
    let (r, lit) = alt((rdf_literal_lang_tag, rdf_literal_iri))(r)?;
    Ok((r, lit))
}

fn rdf_literal_lang_tag(r: &str) -> IResult<&str, StottrLiteral> {
    let (r, (value, language)) = tuple((string, lang_tag))(r)?;
    Ok((
        r,
        StottrLiteral {
            value: value.to_string(),
            language: Some(language),
            data_type_iri: None,
        },
    ))
}

fn rdf_literal_iri(r: &str) -> IResult<&str, StottrLiteral> {
    let (r, (value, _, datatype_iri)) = tuple((string, tag("^^"), iri))(r)?;
    Ok((
        r,
        StottrLiteral {
            value: value.to_string(),
            language: None,
            data_type_iri: Some(datatype_iri),
        },
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
            many0(tuple((
                alt((opt(tag("\"")), opt(tag("\"\"")))),
                is_not("\"\\"),
            ))),
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

fn escapable_echar(e: &str) -> IResult<&str, String> {
    let (e, c) = one_of(r#"tbnrf"'\"#)(e)?;
    Ok((e, c.to_string()))
}

fn b_node_label(b: &str) -> IResult<&str, String> {
    let (b, (first, opt_period, list)) = tuple((
        pn_chars_u_as_string,
        opt(tag(".")),
        separated_list0(tag("."), many1(pn_chars_as_string)),
    ))(b)?;
    let mut first_string = first.to_string();
    if let Some(period) = opt_period {
        first_string += period;
    }
    let list_strings: Vec<String> = list.iter().map(|x| x.join("")).collect();
    first_string += &list_strings.join(".");
    Ok((b, first_string))
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
    Ok((d, a))
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
    let (p, (_, _, _, name, _, iri, _)) = tuple((
        multispace0,
        tag("@prefix"),
        multispace0,
        pname_ns,
        multispace0,
        iri_ref,
        tag("."),
    ))(p)?;
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
    let (p, pn) = alt((pname_ln, pname_ns_as_prefixed_name))(p)?;
    Ok((p, pn))
}

fn iri_ref_as_resolves(i: &str) -> IResult<&str, ResolvesToNamedNode> {
    let (i, nn) = iri_ref(i)?;
    Ok((i, ResolvesToNamedNode::NamedNode(nn)))
}

fn iri_ref(i: &str) -> IResult<&str, NamedNode> {
    let mut notin: String = chars!('\u{0000}'..='\u{0020}').iter().collect();
    let rest = "<>\"{}|^`\\";
    notin += rest;
    let (i, (_, iri, _)) = tuple((tag("<"), many0(is_not(notin.as_str())), tag(">")))(i)?;
    let nn = NamedNode::new(
        iri.iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(""),
    )
    .expect("Invalid IRI");
    Ok((i, nn))
}

fn pname_ns_as_prefixed_name(p: &str) -> IResult<&str, PrefixedName> {
    let (p, prefix) = pname_ns(p)?;
    let out = PrefixedName {
        prefix: prefix,
        name: "".to_string(),
    };
    Ok((p, out))
}

fn pname_ns(p: &str) -> IResult<&str, String> {
    let (p, (optname, _)) = tuple((opt(pn_prefix), tag(":")))(p)?;
    let out = match optname {
        None => "".to_string(),
        Some(name) => name,
    };
    Ok((p, out))
}

fn pname_ln(p: &str) -> IResult<&str, PrefixedName> {
    let (p, (prefix, name)) = tuple((pname_ns, pn_local))(p)?;
    Ok((p, PrefixedName { prefix, name }))
}

fn pn_chars_base_as_string(p: &str) -> IResult<&str, String> {
    let (p, chrs) = pn_chars_base(p)?;
    Ok((p, chrs.to_string()))
}

//Incomplete from specification
fn pn_chars_base(p: &str) -> IResult<&str, char> {
    let range_a = chars!('A'..'Z').iter();
    let range_b = chars!('a'..'z').iter();
    let range_c = chars!('\u{00C0}'..'\u{00D6}').iter();
    let range_d = chars!('\u{00D8}'..'\u{00F6}').iter();
    let range_e = chars!('\u{00F8}'..'\u{02FF}').iter();
    let range_f = chars!('\u{0370}'..'\u{037D}').iter();
    let range_g = chars!('\u{037F}'..'\u{1FFF}').iter();
    let range_h = chars!('\u{200C}'..'\u{200D}').iter();
    let range_i = chars!('\u{2070}'..'\u{218F}').iter();
    let range_j = chars!('\u{2C00}'..'\u{2FEF}').iter();
    let range_k = chars!('\u{3001}'..'\u{D7FF}').iter();
    let range_l = chars!('\u{F900}'..'\u{FDCF}').iter();
    let range_m = chars!('\u{FDF0}'..'\u{FFFD}').iter();
    let all_chars: String = range_a
        .chain(range_b)
        .chain(range_c)
        .chain(range_d)
        .chain(range_e)
        .chain(range_f)
        .chain(range_g)
        .chain(range_h)
        .chain(range_i)
        .chain(range_j)
        .chain(range_k)
        .chain(range_l)
        .chain(range_m)
        .collect();
    let (p, chrs) = one_of(all_chars.as_str())(p)?;
    Ok((p, chrs))
}

fn pn_chars_u_as_string(p: &str) -> IResult<&str, String> {
    let (p, chrs) = pn_chars_u(p)?;
    Ok((p, chrs.to_string()))
}

fn pn_chars_u(p: &str) -> IResult<&str, char> {
    let (p, chrs) = alt((pn_chars_base, char('_')))(p)?;
    Ok((p, chrs))
}

//Incomplete from specification
fn pn_chars_as_string(p: &str) -> IResult<&str, String> {
    let (p, chrs) = pn_chars(p)?;
    Ok((p, chrs.to_string()))
}

fn pn_chars(p: &str) -> IResult<&str, char> {
    let range_a: String = chars!('\u{0300}'..'\u{036F}').iter().collect();
    let range_b: String = chars!('\u{203F}'..'\u{2040}').iter().collect();
    let (p, chrs) = alt((
        pn_chars_u,
        char('-'),
        one_of("0123456789"),
        one_of(range_a.as_str()),
        one_of(range_b.as_str()),
    ))(p)?;
    Ok((p, chrs))
}

fn pn_prefix(p: &str) -> IResult<&str, String> {
    let (p, (pbase, opt_period, dotnot)) = tuple((
        pn_chars_base,
        opt(tag(".")),
        opt(separated_list0(tag("."), many1(pn_chars))),
    ))(p)?;
    let mut out = pbase.to_string();
    if let Some(period) = opt_period {
        out += period;
    }
    if let Some(v) = dotnot {
        let mut strings: Vec<String> = vec![];
        for chars in v {
            strings.push(chars.iter().collect());
        }
        out += &strings.join(".");
    }
    Ok((p, out))
}

fn pn_local(p: &str) -> IResult<&str, String> {
    let (p, (s1, opt_period, s2)) = tuple((
        alt((
            pn_chars_u_as_string,
            colon_as_string,
            one_digit_as_string,
            plx,
        )),
        opt(tag(".")),
        separated_list0(
            tag("."),
            many1(alt((pn_chars_as_string, colon_as_string, plx))),
        ),
    ))(p)?;
    let mut out = s1.to_string();
    if let Some(period) = opt_period {
        out += &period;
    }
    let liststrings: Vec<String> = s2.into_iter().map(|x| x.join("")).collect();
    out += &liststrings.join(".");
    Ok((p, out))
}

fn plx(p: &str) -> IResult<&str, String> {
    let (p, plx) = alt((percent, pn_local_esc))(p)?;
    Ok((p, plx))
}

fn percent(p: &str) -> IResult<&str, String> {
    let (p, (_, h)) = tuple((tag("%"), count(one_hex, 2)))(p)?;
    Ok((p, h.join("")))
}

fn one_digit_as_string(d: &str) -> IResult<&str, String> {
    let (d, digit) = one_digit(d)?;
    Ok((d, digit.to_string()))
}

fn one_digit(d: &str) -> IResult<&str, char> {
    let (d, digit) = one_of("0123456789")(d)?;
    Ok((d, digit))
}

fn pn_local_esc(s: &str) -> IResult<&str, String> {
    let esc = r#"\(_~.-!$&\()*+,;=/?#@%"#;
    let (s, (_, c)) = tuple((tag("\\"), one_of(esc)))(s)?;
    Ok((s, c.to_string()))
}

fn one_hex(h: &str) -> IResult<&str, String> {
    let (h, hex) = one_of("0123456789abcdefABCDEF")(h)?;
    Ok((h, hex.to_string()))
}

fn comma(c: &str) -> IResult<&str, String> {
    let (c, comma) = tag(",")(c)?;
    Ok((c, comma.to_string()))
}

fn colon_as_string(c: &str) -> IResult<&str, String> {
    let (c, colon) = tag(":")(c)?;
    Ok((c, colon.to_string()))
}

fn period_as_string(c: &str) -> IResult<&str, String> {
    let (c, period) = tag(".")(c)?;
    Ok((c, period.to_string()))
}

fn underscore(c: &str) -> IResult<&str, String> {
    let (c, underscore) = tag("_")(c)?;
    Ok((c, underscore.to_string()))
}

fn dash(c: &str) -> IResult<&str, String> {
    let (c, dash) = tag("-")(c)?;
    Ok((c, dash.to_string()))
}

#[test]
fn test_iri_ref() {
    let s = "<http://example.org#>";
    let (r, nn) = iri_ref(s).finish().expect("Ok");
    assert_eq!(nn, NamedNode::new_unchecked("http://example.org#"));
    println!("{:?}", nn);
}

#[test]
fn test_pn_prefix() {
    let s = "o-.rd.f.";
    let (r, p) = pn_prefix(s).finish().expect("Ok");
    assert_eq!(r, ".");
    assert_eq!(&p, "o-.rd.f")
}

#[test]
fn test_prefixed_name() {
    let s = "o-rdf:Type";
    let (r, p) = prefixed_name(s).finish().expect("Ok");
    let expected = PrefixedName {
        prefix: "o-rdf".to_string(),
        name: "Type".to_string(),
    };
    assert_eq!(p, expected);
    assert_eq!(r, "");
}

#[test]
fn test_argument_bad_escape_behavior() {
    let s = "foaf:Person,";
    let (r, i) = argument(s).finish().expect("Ok");
    assert_eq!(r, ",");
}

#[test]
fn test_instance() {
    let s = "ottr:Triple (_:person, foaf:Person, ?var)";
    let (r, i) = instance(s).finish().expect("Ok");
    let expected = Instance {
        list_expander: None,
        template_name: ResolvesToNamedNode::PrefixedName(PrefixedName {
            prefix: "ottr".to_string(),
            name: "Triple".to_string(),
        }),
        argument_list: vec![
            Argument {
                list_expand: false,
                term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::BlankNode(
                    BlankNode::new_unchecked("person"),
                ))),
            },
            Argument {
                list_expand: false,
                term: StottrTerm::ConstantTerm(ConstantTerm::Constant(ConstantLiteral::IRI(
                    ResolvesToNamedNode::PrefixedName(PrefixedName {
                        prefix: "foaf".to_string(),
                        name: "Person".to_string(),
                    }),
                ))),
            },
            Argument {
                list_expand: false,
                term: StottrTerm::Variable(StottrVariable {
                    name: "var".to_string(),
                }),
            },
        ],
    };
    assert_eq!(i, expected);
    assert_eq!(r, "");
}
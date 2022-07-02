use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::read_dir;
use std::path::Path;
use crate::ast::{Instance, PType, Parameter, Statement, StottrDocument, StottrTerm, StottrVariable, Template, Signature};
use oxrdf::NamedNode;
use oxrdf::vocab::xsd;
use crate::constants::OTTR_TRIPLE;
use crate::document::stottr_from_file;



#[derive(Debug)]
pub struct TypingError {
    pub kind: TypingErrorType,
}

#[derive(Debug)]
pub enum TypingErrorType {
    InconsistentNumberOfArguments(String, String, usize, usize),
    IncompatibleTypes(String, StottrVariable, String, String),
}

impl Display for TypingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            TypingErrorType::InconsistentNumberOfArguments(calling, template, given, expected) => {
                write!(f, "Template {} called {} with {} arguments, but expected {}", calling, template, given, expected)
            }
            TypingErrorType::IncompatibleTypes(nn, var, given, expected) => {
                write!(f, "Template {} variable {} was given argument of type {:?} but expected {:?}", nn, var.name, given, expected)
            }
        }
    }
}

impl Error for TypingError {

}

#[derive(Clone)]
pub struct TemplateDataset {
    pub templates: Vec<Template>,
    pub ground_instances: Vec<Instance>,
}

impl TemplateDataset {
    pub fn new(mut documents: Vec<StottrDocument>) -> Result<TemplateDataset,TypingError> {
        let mut templates = vec![];
        let mut ground_instances = vec![];
        for d in &mut documents {
            for i in d.statements.drain(0..d.statements.len()) {
                match i {
                    Statement::Template(t) => {
                        templates.push(t);
                    }
                    Statement::Instance(i) => {
                        ground_instances.push(i);
                    }
                }
            }
        }
        let mut td = TemplateDataset {
            templates,
            ground_instances,
        };
        //TODO: Put in function, check not exists and consistent...
        let ottr_triple_subject =Parameter{
                optional: false,
                non_blank: false,
                ptype: Some(PType::BasicType(xsd::ANY_URI.into_owned())),
                stottr_variable: StottrVariable { name: "subject".to_string() },
                default_value: None
            };
        let ottr_triple_verb =Parameter{
                optional: false,
                non_blank: false,
                ptype: Some(PType::BasicType(xsd::ANY_URI.into_owned())),
                stottr_variable: StottrVariable { name: "verb".to_string() },
                default_value: None
            };
        let ottr_triple_object =Parameter{
                optional: false,
                non_blank: false,
                ptype: None,
                stottr_variable: StottrVariable { name: "object".to_string() },
                default_value: None
            };


        let ottr_template = Template { signature: Signature {
            template_name: NamedNode::new_unchecked(OTTR_TRIPLE),
            parameter_list: vec![ottr_triple_subject, ottr_triple_verb, ottr_triple_object],
            annotation_list: None
        }, pattern_list: vec![] };
        td.templates.push(ottr_template);
        //Todo: variable safe, no cycles, referential integrity, no duplicates, well founded
        //Check ground instances also!!
        td.infer_types()?;
        Ok(td)
    }

    pub fn from_folder<P: AsRef<Path>>(path: P) -> Result<TemplateDataset, Box<dyn Error + 'static>> {
        let mut docs = vec![];
        let files_result = read_dir(path)?;
        for f in files_result {
            let f = f?;
            if let Some(e) = f.path().extension() {
                if let Some(s) = e.to_str() {
                    let extension = s.to_lowercase();
                    if "stottr" == &extension {
                        let doc = stottr_from_file(f.path())?;
                        docs.push(doc);
                    }
                }
            }
        }
        Ok(TemplateDataset::new(docs)?)
    }

    pub fn get(&self, named_node: &NamedNode) -> Option<&Template> {
        for t in &self.templates {
            if &t.signature.template_name == named_node {
                return Some(t);
            }
        }
        None
    }

    fn infer_types(&mut self) -> Result<(), TypingError> {
        for i in 0..self.templates.len() {
            let (left, right) = self.templates.split_at_mut(i);
            let (element, right) = right.split_at_mut(1);
            let mut changed = true;
            while changed {
                changed = infer_template_types(
                    element.first_mut().unwrap(),
                    (&left).iter().chain((&right).iter()).collect(),
                )?;
            }
        }
        Ok(())
        }
}

fn infer_template_types(
    template: &mut Template,
    templates: Vec<&Template>,
) -> Result<bool, TypingError> {
    let mut changed = false;
    for i in &mut template.pattern_list {
        let other = *templates
            .iter()
            .find(|t| &t.signature.template_name == &i.template_name)
            .unwrap();
        if i.argument_list.len() != other.signature.parameter_list.len() {
            return Err(TypingError {
                kind: TypingErrorType::InconsistentNumberOfArguments(
                    template.signature.template_name.as_str().to_string(),
                    other.signature.template_name.as_str().to_string(),
                    i.argument_list.len(),
                    other.signature.parameter_list.len(),
                ),
            });
        }
        for (argument, other_parameter) in i
            .argument_list
            .iter()
            .zip(other.signature.parameter_list.iter())
        {
            match &argument.term {
                StottrTerm::Variable(v) => {
                    for my_parameter in &mut template.signature.parameter_list {
                        if &my_parameter.stottr_variable == v {
                            if let Some(other_ptype) = &other_parameter.ptype {
                                if argument.list_expand {
                                    if !other_parameter.optional {
                                        changed = lub_update(&template.signature.template_name, v,
                                            my_parameter,
                                            &PType::NEListType(Box::new(other_ptype.clone())),
                                        )?;
                                    } else {
                                        changed = lub_update(
                                            &template.signature.template_name, v,
                                            my_parameter,
                                            &PType::ListType(Box::new(other_ptype.clone())),
                                        )?;
                                    }
                                } else {
                                    changed = lub_update(&template.signature.template_name, v,  my_parameter, other_ptype)?;
                                }
                            }
                        }
                    }
                }
                StottrTerm::ConstantTerm(c) => {}
                StottrTerm::List(l) => {}
            }
        }
    }
    Ok(changed)
}

fn lub_update(template_name: &NamedNode, variable: &StottrVariable, my_parameter: &mut Parameter, right: &PType) -> Result<bool, TypingError> {
    if my_parameter.ptype.is_none() {
        my_parameter.ptype = Some(right.clone());
        Ok(true)
    } else {
        if my_parameter.ptype.as_ref().unwrap() != right {
            let ptype = lub(template_name, variable, my_parameter.ptype.as_ref().unwrap(), right)?;
            if my_parameter.ptype.as_ref().unwrap() != &ptype {
                my_parameter.ptype = Some(ptype);
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }
}

//TODO: LUB ptype...
fn lub(template_name: &NamedNode, variable: &StottrVariable, left:&PType, right:&PType) -> Result<PType, TypingError> {
    if left == right {
        return Ok(left.clone());
    } else {
        if let PType::NEListType(left_inner) = left {
            if let PType::ListType(right_inner) = right {
                return Ok(PType::NEListType(Box::new(lub(template_name, variable,left_inner, right_inner)?)));
            } else if let PType::NEListType(right_inner) = right {
                return Ok(PType::NEListType(Box::new(lub(template_name, variable,left_inner, right_inner)?)));
            }
        } else if let PType::ListType(left_inner) = left {
            if let PType::NEListType(right_inner) = right {
                return Ok(PType::NEListType(Box::new(lub(template_name, variable,left_inner, right_inner)?)));
            } else if let PType::ListType(right_inner) = right {
                return Ok(PType::ListType(Box::new(lub(template_name, variable,left_inner, right_inner)?)));
            }
        }
    }
    Err(TypingError{kind:TypingErrorType::IncompatibleTypes(template_name.as_str().to_string(), variable.clone(), left.to_string(), right.to_string())})
}

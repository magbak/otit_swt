use crate::ast::{
    Instance, PType, Parameter, Statement, StottrDocument, StottrTerm, StottrVariable, Template,
};
use oxrdf::NamedNode;

pub struct TemplateDataset {
    pub templates: Vec<Template>,
    pub ground_instances: Vec<Instance>,
}

pub struct TypingError {
    pub kind: TypingErrorType,
}

pub enum TypingErrorType {
    InconsistentNumberOfArguments(NamedNode, NamedNode, usize, usize),
    IncompatibleTypes(NamedNode, StottrVariable, PType, PType),
}

impl TemplateDataset {
    pub fn new(mut documents: Vec<StottrDocument>) -> TemplateDataset {
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
        TemplateDataset {
            templates,
            ground_instances,
        }
    }

    pub fn get(&self, named_node: &NamedNode) -> Option<Template> {
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
                    template.signature.template_name.clone(),
                    other.signature.template_name.clone(),
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
                                        changed = lub_update(
                                            my_parameter,
                                            &PType::NEListType(Box::new(other_ptype.clone())),
                                        )?;
                                    } else {
                                        changed = lub_update(
                                            my_parameter,
                                            &PType::ListType(Box::new(other_ptype.clone())),
                                        )?;
                                    }
                                } else {
                                    changed = lub_update(my_parameter, other_ptype)?;
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

fn lub_update(my_parameter: &mut Parameter, right: &PType) -> Result<bool, TypingError> {
    Ok(true)
}

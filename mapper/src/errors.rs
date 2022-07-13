use crate::mapping::errors::MappingError;
use crate::parsing::errors::ParsingError;
use crate::resolver::ResolutionError;
use crate::templates::TypingError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MapperError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    ParsingError(#[from] ParsingError),
    #[error(transparent)]
    ResolutionError(#[from] ResolutionError),
    #[error(transparent)]
    TypingError(#[from] TypingError),
    #[error(transparent)]
    MappingError(#[from] MappingError),
}

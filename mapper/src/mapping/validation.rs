use super::Mapping;
use crate::mapping::errors::MappingError;
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;

impl Mapping {
    pub(crate) fn validate_dataframe(&mut self, df: &mut DataFrame) -> Result<(), MappingError> {
        if !df.get_column_names().contains(&"Key") {
            return Err(MappingError::MissingKeyColumn);
        }
        if self
            .object_property_triples
            .as_ref()
            .unwrap()
            .get_column_names()
            .contains(&"Key")
        {
            let key_datatype = df.column("Key").unwrap().dtype().clone();
            if key_datatype != DataType::Utf8 {
                return Err(MappingError::InvalidKeyColumnDataType(key_datatype.clone()));
            }
            if df.column("Key").unwrap().is_duplicated().unwrap().any() {
                let is_duplicated = df.column("Key").unwrap().is_duplicated().unwrap();
                let dupes = df.filter(&is_duplicated).unwrap().clone();
                return Err(MappingError::KeyColumnContainsDuplicates(
                    dupes.column("Key").unwrap().clone(),
                ));
            }
        }
        Ok(())
    }
}

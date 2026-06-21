use super::*;
use polars::prelude::*;

impl<'a> IterFromColumnRaw<'a> for Option<&'a str> {
    fn create_iter(column: &'a Column) -> PolarsResult<impl Iterator<Item = Self> + 'a> {
        let iter = match column.dtype() {
            DataType::String => create_str_iter(column)?,
            #[cfg(feature = "dtype-categorical")]
            DataType::Categorical(_, _) => create_cat_iter(column)?,
            #[cfg(feature = "dtype-categorical")]
            DataType::Enum(_, _) => create_enum_iter(column)?,
            dtype => {
                let column_name = column.name().as_str();
                return Err(
                    polars_err!(SchemaMismatch: "Cannot get &str from column '{column_name}' with dtype '{dtype}'.\
                                             Make sure to enable 'dtype-categorical' feature for 'Categorical' and 'Enum' dtypes."),
                );
            }
        };

        Ok(iter)
    }
}

fn create_str_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    Ok(Box::new(column.str()?.iter()))
}

#[cfg(feature = "dtype-categorical")]
fn create_cat_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    Ok(Box::new(column.cat32()?.iter_str()))
}

#[cfg(feature = "dtype-categorical")]
fn create_enum_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    Ok(Box::new(column.cat8()?.iter_str()))
}

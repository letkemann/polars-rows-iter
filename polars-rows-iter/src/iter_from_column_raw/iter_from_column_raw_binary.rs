use super::*;
use polars::prelude::*;

impl<'a> IterFromColumnRaw<'a> for Option<&'a [u8]> {
    fn create_iter(column: &'a Column) -> PolarsResult<impl Iterator<Item = Self> + 'a> {
        let column_name = column.name().as_str();
        let iter: Box<dyn Iterator<Item = Option<&[u8]>>> = match column.dtype() {
            DataType::Binary => Box::new(column.binary()?.iter()),
            DataType::BinaryOffset => Box::new(column.binary_offset()?.iter()),
            dtype => {
                return Err(
                    polars_err!(SchemaMismatch: "Cannot get &[u8] from column '{column_name}' with dtype : {dtype}"),
                )
            }
        };

        Ok(iter)
    }
}

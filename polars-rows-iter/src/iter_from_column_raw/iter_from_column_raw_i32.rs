use crate::IterFromColumnRaw;
use polars::prelude::*;

impl<'a> IterFromColumnRaw<'a> for Option<i32> {
    fn create_iter(
        column: &'a polars::prelude::Column,
    ) -> polars::prelude::PolarsResult<impl Iterator<Item = Self> + 'a>
    where
        Self: Sized,
    {
        let column_name = column.name().as_str();
        match column.dtype() {
            DataType::Int32 => Ok(column.i32()?.iter()),
            DataType::Date => Ok(column.date()?.phys.iter()),
            dtype => {
                Err(polars_err!(SchemaMismatch: "Cannot get i32 from column '{column_name}' with dtype : {dtype}"))
            }
        }
    }
}

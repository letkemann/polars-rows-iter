use crate::IterFromColumnRaw;
use polars::prelude::*;

impl<'a> IterFromColumnRaw<'a> for Option<i64> {
    fn create_iter(
        column: &'a polars::prelude::Column,
    ) -> polars::prelude::PolarsResult<impl Iterator<Item = Self> + 'a>
    where
        Self: Sized,
    {
        let column_name = column.name().as_str();
        let iter = match column.dtype() {
            DataType::Int64 => column.i64()?.iter(),
            DataType::Time => column.as_materialized_series().time()?.phys.iter(),
            DataType::Datetime(_, _) => column.datetime()?.phys.iter(),
            DataType::Duration(_) => column.duration()?.phys.iter(),
            dtype => {
                return Err(
                    polars_err!(SchemaMismatch: "Cannot get i64 from column '{column_name}' with dtype : {dtype}"),
                )
            }
        };

        Ok(iter)
    }
}

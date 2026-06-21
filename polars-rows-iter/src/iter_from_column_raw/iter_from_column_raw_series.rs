use crate::IterFromColumnRaw;
use polars::prelude::*;

impl<'a> IterFromColumnRaw<'a> for Option<Series> {
    fn create_iter(
        column: &'a polars::prelude::Column,
    ) -> polars::prelude::PolarsResult<impl Iterator<Item = Self> + 'a>
    where
        Self: Sized,
    {
        let column_name = column.name().as_str();
        let iter: Box<dyn Iterator<Item = Option<Series>> + 'a> = match column.dtype() {
            DataType::List(_) => Box::new(
                column
                    .list()?
                    .amortized_iter()
                    .map(|opt| opt.map(|series| series.deep_clone())),
            ),
            dtype => {
                return Err(
                    polars_err!(SchemaMismatch: "Cannot get Series from column '{column_name}' with dtype: {dtype}"),
                )
            }
        };

        Ok(iter)
    }
}

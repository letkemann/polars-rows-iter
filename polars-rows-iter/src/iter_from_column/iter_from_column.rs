use polars::prelude::*;

pub trait IterFromColumn<'a> {
    fn create_iter(
        dataframe: &'a DataFrame,
        column_name: &'a str,
    ) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
    where
        Self: Sized;
}

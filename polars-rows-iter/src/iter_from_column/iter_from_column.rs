use polars::prelude::*;

pub trait IterFromColumn<'a, T> {
    fn create_iter(
        dataframe: &'a DataFrame,
        column_name: &'a str,
    ) -> PolarsResult<Box<dyn Iterator<Item = Option<T>> + 'a>>
    where
        Self: Sized;
}

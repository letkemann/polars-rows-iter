use polars::prelude::*;

pub trait IterFromColumn<'a, T> {
    fn create_iter(
        dataframe: &'a DataFrame,
        column_name: &'a str,
    ) -> PolarsResult<Box<dyn Iterator<Item = Option<T>> + 'a>>
    where
        Self: Sized;

    fn get_value(_polars_value: Option<T>, _column_name: &'a str) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        // You must override this for not optional values.
        // Make sure to add the #[inline] attribute to your implementation.
        panic!("This should never be called")
    }
}

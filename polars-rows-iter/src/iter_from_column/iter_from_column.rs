use polars::prelude::*;

pub trait IterFromColumn<'a> {
    type RawInner;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<Self::RawInner>> + 'a>>
    where
        Self: Sized;

    fn get_value(polars_value: Option<Self::RawInner>, column_name: &str, dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized;

    #[inline]
    fn unexpected_null_value_error(column_name: &str) -> PolarsError {
        polars_err!(SchemaMismatch: "Found unexpected None/null value in column {column_name} with mandatory values!")
    }
}

mod iter_from_column_binary;
#[cfg(feature = "chrono")]
mod iter_from_column_chrono;
mod iter_from_column_i32;
mod iter_from_column_i64;
mod iter_from_column_primitives;
mod iter_from_column_series;
mod iter_from_column_str;
mod iter_from_column_string;
mod iter_from_column_vec;

use polars::prelude::*;

pub trait IterFromColumn<'a> {
    type RawInner: 'a;

    fn get_value(polars_value: Option<Self::RawInner>, column_name: &str, dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized;

    #[inline]
    fn unexpected_null_value_error(column_name: &str) -> PolarsError {
        polars_err!(SchemaMismatch: "Found unexpected None/null value in column '{column_name}' with mandatory values!")
    }
}

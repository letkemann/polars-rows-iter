mod iter_from_column_raw_binary;
mod iter_from_column_raw_i32;
mod iter_from_column_raw_i64;
mod iter_from_column_raw_series;
mod iter_from_column_raw_str;

use polars::prelude::*;

pub trait IterFromColumnRaw<'a> {
    fn create_iter(column: &'a Column) -> PolarsResult<impl Iterator<Item = Self> + 'a>
    where
        Self: Sized;
}

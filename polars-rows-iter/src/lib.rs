use polars::prelude::*;
pub use polars_rows_iter_derive::*;

// iter_from_column_for_type!(i8);

pub trait FromDataFrameRow<'a> {
    fn from_dataframe(dataframe: &'a DataFrame) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
    where
        Self: Sized;
}

pub trait IterFromColumn<'a> {
    fn create_iter(
        dataframe: &'a DataFrame,
        column_name: &'a str,
    ) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
    where
        Self: Sized;
}

pub trait DataframeRowsIterExt<'a> {
    fn rows_iter<T>(&'a self) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>;
}

impl<'a> DataframeRowsIterExt<'a> for DataFrame {
    fn rows_iter<T>(&'a self) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>,
    {
        Ok(T::from_dataframe(self)?)
    }
}

fn mandatory_value<T>(polars_value: Option<T>) -> PolarsResult<T> {
    match polars_value {
        Some(value) => Ok(value),
        None => Err(polars_err!(SchemaMismatch: "Found unexpected None/null value in columns with mandatory values!")),
    }
}

fn optional_value<T>(polars_value: Option<T>) -> PolarsResult<Option<T>> {
    Ok(polars_value)
}

impl<'a> IterFromColumn<'a> for &'a str {
    fn create_iter(
        dataframe: &'a DataFrame,
        column_name: &'a str,
    ) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>> {
        let iter = Box::new(dataframe.column(column_name)?.str()?.into_iter().map(mandatory_value));
        Ok(iter)
    }
}

iter_from_column_for_type!(bool);
iter_from_column_for_type!(i8);
iter_from_column_for_type!(i16);
iter_from_column_for_type!(i32);
iter_from_column_for_type!(i64);
iter_from_column_for_type!(u8);
iter_from_column_for_type!(u16);
iter_from_column_for_type!(u32);
iter_from_column_for_type!(u64);
iter_from_column_for_type!(f32);
iter_from_column_for_type!(f64);

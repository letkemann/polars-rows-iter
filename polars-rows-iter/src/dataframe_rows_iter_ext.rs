use polars::prelude::*;

use crate::FromDataFrameRow;

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

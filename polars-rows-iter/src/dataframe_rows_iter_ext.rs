use std::collections::HashMap;

use polars::prelude::*;

use crate::{ColumnNameBuilder, FromDataFrameRow};

pub trait DataframeRowsIterExt<'a> {
    fn rows_iter<T>(&'a self) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>;

    fn rows_iter_for_columns<T>(
        &'a self,
        build_fn: impl FnOnce(&mut T::Builder),
    ) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>;
}

impl<'a> DataframeRowsIterExt<'a> for DataFrame {
    fn rows_iter<T>(&'a self) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>,
    {
        T::from_dataframe(self, HashMap::new())
    }

    fn rows_iter_for_columns<T>(
        &'a self,
        build_fn: impl FnOnce(&mut T::Builder),
    ) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>,
    {
        let mut builder = T::create_builder();

        build_fn(&mut builder);

        let columns = builder.build();

        T::from_dataframe(self, columns)
    }
}

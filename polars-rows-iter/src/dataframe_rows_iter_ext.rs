use std::collections::HashMap;

use polars::prelude::*;

use crate::{ColumnNameBuilder, FromDataFrameRow};

pub trait DataframeRowsIterExt<'a> {
    fn rows_iter<T>(&'a self) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>;

    fn rows_iter_for_columns<T>(
        &'a self,
        build_fn: impl FnOnce(&mut T::Builder) -> &mut T::Builder,
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
        build_fn: impl FnOnce(&mut T::Builder) -> &mut T::Builder,
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

#[cfg(test)]
mod tests {
    #![allow(dead_code)]

    use polars::df;

    use crate::*;

    #[derive(FromDataFrameRow)]
    struct TestStruct {
        x1: i32,
        x2: i32,
    }

    #[test]
    fn rows_iter_should_return_error_when_given_column_not_available() {
        let df = df!(
            "y1" => [1i32, 2, 3],
            "x2" => [1i32, 2, 3]
        )
        .unwrap();

        let result = df.rows_iter::<TestStruct>();

        assert!(result.is_err());
    }

    #[test]
    fn builder_should_build_hashmap_with_correct_entries() {
        let mut builder = TestStruct::create_builder();
        builder.x1("column_1").x2("column_2");
        let columns = builder.build();

        assert_eq!("column_1", *columns.get("x1").unwrap());
        assert_eq!("column_2", *columns.get("x2").unwrap());
    }

    #[test]
    fn rows_iter_for_columns_should_return_error_when_given_column_not_available() {
        let df = df!(
            "x1" => [1i32, 2, 3],
            "x2" => [1i32, 2, 3]
        )
        .unwrap();

        let result = df.rows_iter_for_columns::<TestStruct>(|b| b.x1("y1"));

        assert!(result.is_err());
    }

    #[test]
    fn rows_iter_for_columns_should_return_valid_iter() {
        let df = df!(
            "x_1" => [1i32, 2, 3],
            "x_2" => [1i32, 2, 3]
        )
        .unwrap();

        let result = df.rows_iter_for_columns::<TestStruct>(|b| b.x1("x_1").x2("x_2"));

        assert!(result.is_ok());
    }
}

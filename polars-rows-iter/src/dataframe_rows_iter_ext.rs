use std::collections::HashMap;

use polars::prelude::*;

use crate::{ColumnNameBuilder, FromDataFrameRow, IterFromColumn};

pub trait DataframeRowsIterExt<'a> {
    fn rows_iter<T>(&'a self) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>;

    fn rows_iter_with_columns<T>(
        &'a self,
        build_fn: impl FnOnce(&mut T::Builder) -> &mut T::Builder,
    ) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>;

    fn scalar_iter<T>(&'a self, column_name: &'a str) -> PolarsResult<impl Iterator<Item = PolarsResult<T>> + 'a>
    where
        T: IterFromColumn<'a> + 'a;
}

impl<'a> DataframeRowsIterExt<'a> for DataFrame {
    /// Creates a row iterator for this DataFrame with static column names defined in row struct
    /// ```rust
    /// use polars::prelude::*;
    /// use polars_rows_iter::*;
    ///
    ///    #[derive(Debug, FromDataFrameRow)]
    ///    #[derive(PartialEq)] // for assert_eq
    ///    struct MyRow<'a>
    ///    {
    ///        #[column("col_a")]
    ///        a: i32,
    ///        // the column name defaults to the field name if no explicit name given
    ///        col_b: &'a str,
    ///        col_c: String,
    ///        #[column("col_d")]
    ///        optional: Option<f64>
    ///    }
    ///   
    ///    let df = df!(
    ///            "col_a" => [1i32, 2, 3, 4, 5],
    ///            "col_b" => ["a", "b", "c", "d", "e"],
    ///            "col_c" => ["A", "B", "C", "D", "E"],
    ///            "col_d" => [Some(1.0f64), None, None, Some(2.0), Some(3.0)]
    ///        ).unwrap();
    ///   
    ///    let rows_iter = df.rows_iter::<MyRow>().unwrap(); // ready to use row iterator
    ///    // collect to vector for assert_eq
    ///    let rows_vec = rows_iter.collect::<PolarsResult<Vec<MyRow>>>().unwrap();
    ///   
    ///    assert_eq!(
    ///        rows_vec,
    ///        [
    ///            MyRow { a: 1, col_b: "a", col_c: "A".to_string(), optional: Some(1.0) },
    ///            MyRow { a: 2, col_b: "b", col_c: "B".to_string(), optional: None },
    ///            MyRow { a: 3, col_b: "c", col_c: "C".to_string(), optional: None },
    ///            MyRow { a: 4, col_b: "d", col_c: "D".to_string(), optional: Some(2.0) },
    ///            MyRow { a: 5, col_b: "e", col_c: "E".to_string(), optional: Some(3.0) },
    ///        ]
    ///    );
    /// ```
    fn rows_iter<T>(&'a self) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>,
    {
        T::from_dataframe(self, HashMap::new())
    }

    /// Creates a row iterator for this DataFrame with custom column names, which can be defined over the lambda function
    /// for every struct field. If no custom column name for a field is given, the column name falls back to
    /// the statically defined one.
    ///```rust
    ///use polars::prelude::*;
    ///use polars_rows_iter::*;
    ///
    ///const ID: &str = "id";
    ///
    ///#[derive(Debug, FromDataFrameRow)]
    ///#[derive(PartialEq)] // for assert_eq
    ///struct MyRow<'a> {
    ///    #[column(ID)]
    ///    id: i32,
    ///    value_b: &'a str,
    ///    value_c: String,
    ///    optional: Option<f64>,
    ///}
    ///
    ///    let df = df!(
    ///        "id" => [1i32, 2, 3, 4, 5],
    ///        "col_b" => ["a", "b", "c", "d", "e"],
    ///        "col_c" => ["A", "B", "C", "D", "E"],
    ///        "col_d" => [Some(1.0f64), None, None, Some(2.0), Some(3.0)]
    ///    ).unwrap();
    ///
    ///    let value_b_column_name = "col_b".to_string();
    ///    let value_c_column_name = "col_c";
    ///
    ///    let rows_iter = df.rows_iter_with_columns::<MyRow>(|columns| {
    ///        columns
    ///            .value_b(&value_b_column_name)
    ///            .value_c(value_c_column_name)
    ///            .optional("col_d")
    ///    }).unwrap();
    ///
    ///    // collect to vector for assert_eq
    ///    let rows_vec = rows_iter.collect::<PolarsResult<Vec<MyRow>>>().unwrap();
    ///   
    ///    assert_eq!(
    ///        rows_vec,
    ///        [
    ///            MyRow { id: 1, value_b: "a", value_c: "A".to_string(), optional: Some(1.0) },
    ///            MyRow { id: 2, value_b: "b", value_c: "B".to_string(), optional: None },
    ///            MyRow { id: 3, value_b: "c", value_c: "C".to_string(), optional: None },
    ///            MyRow { id: 4, value_b: "d", value_c: "D".to_string(), optional: Some(2.0) },
    ///            MyRow { id: 5, value_b: "e", value_c: "E".to_string(), optional: Some(3.0) },
    ///        ]
    ///    );
    ///```
    fn rows_iter_with_columns<T>(
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

    fn scalar_iter<T>(&'a self, column_name: &'a str) -> PolarsResult<impl Iterator<Item = PolarsResult<T>> + 'a>
    where
        T: IterFromColumn<'a> + 'a,
    {
        let column = self.column(column_name)?;
        let column_dtype = column.dtype();

        let iter = <T as IterFromColumn<'a>>::create_iter(column)?;
        let iter = iter.map(|v| <T as IterFromColumn<'a>>::get_value(v, column_name, column_dtype));

        Ok(iter)
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
    fn rows_iter_with_columns_should_return_error_when_given_column_not_available() {
        let df = df!(
            "x1" => [1i32, 2, 3],
            "x2" => [1i32, 2, 3]
        )
        .unwrap();

        let result = df.rows_iter_with_columns::<TestStruct>(|b| b.x1("y1"));

        assert!(result.is_err());
    }

    #[test]
    fn rows_iter_with_columns_should_return_valid_iter() {
        let df = df!(
            "x_1" => [1i32, 2, 3],
            "x_2" => [1i32, 2, 3]
        )
        .unwrap();

        let result = df.rows_iter_with_columns::<TestStruct>(|b| b.x1("x_1").x2("x_2"));

        assert!(result.is_ok());
    }
}

#![feature(prelude_import)]
#[prelude_import]
use std::prelude::rust_2021::*;
#[macro_use]
extern crate std;
use polars::df;
use polars_rows_iter::*;

struct DataRow0<'a> {
    _col_x: i32,
    _col_y: &'a str,
}
#[automatically_derived]
impl<'a> ::core::fmt::Debug for DataRow0<'a> {
    #[inline]
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        ::core::fmt::Formatter::debug_struct_field2_finish(
            f,
            "DataRow0",
            "_col_x",
            &self._col_x,
            "_col_y",
            &&self._col_y,
        )
    }
}
#[automatically_derived]
impl<'a> FromDataFrameRow<'a> for DataRow0<'a> {
    fn from_dataframe(
        dataframe: &'a polars::prelude::DataFrame,
    ) -> polars::prelude::PolarsResult<Box<dyn Iterator<Item = polars::prelude::PolarsResult<Self>> + 'a>>
    where
        Self: Sized,
    {
        let _col_x_iter = IterFromColumn::create_iter(dataframe, "_col_x")?;
        let _col_y_iter = IterFromColumn::create_iter(dataframe, "_col_y")?;
        Ok(Box::new(DataRow0RowsIterator::<'a> {
            _col_x_iter,
            _col_y_iter,
        }))
    }
}
#[automatically_derived]
struct DataRow0RowsIterator<'a> {
    _col_x_iter: Box<dyn Iterator<Item = polars::prelude::PolarsResult<i32>> + 'a>,
    _col_y_iter: Box<dyn Iterator<Item = polars::prelude::PolarsResult<&'a str>> + 'a>,
}
#[automatically_derived]
impl<'a> DataRow0RowsIterator<'a> {
    fn create(
        _col_x: polars::prelude::PolarsResult<i32>,
        _col_y: polars::prelude::PolarsResult<&'a str>,
    ) -> polars::prelude::PolarsResult<DataRow0<'a>> {
        Ok(DataRow0 {
            _col_x: _col_x?,
            _col_y: _col_y?,
        })
    }
}
impl<'a> Iterator for DataRow0RowsIterator<'a> {
    type Item = polars::prelude::PolarsResult<DataRow0<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        let _col_x_value = self._col_x_iter.next()?;
        let _col_y_value = self._col_y_iter.next()?;
        Some(Self::create(_col_x_value, _col_y_value))
    }
}

struct DataRow1 {
    _col_x: i32,
}
#[automatically_derived]
impl ::core::fmt::Debug for DataRow1 {
    #[inline]
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        ::core::fmt::Formatter::debug_struct_field1_finish(f, "DataRow1", "_col_x", &&self._col_x)
    }
}
#[automatically_derived]
impl<'a> FromDataFrameRow<'a> for DataRow1 {
    fn from_dataframe(
        dataframe: &'a polars::prelude::DataFrame,
    ) -> polars::prelude::PolarsResult<Box<dyn Iterator<Item = polars::prelude::PolarsResult<Self>> + 'a>>
    where
        Self: Sized,
    {
        let _col_x_iter = IterFromColumn::create_iter(dataframe, "_col_x")?;
        Ok(Box::new(DataRow1RowsIterator { _col_x_iter }))
    }
}
#[automatically_derived]
struct DataRow1RowsIterator<'a> {
    _col_x_iter: Box<dyn Iterator<Item = polars::prelude::PolarsResult<i32>> + 'a>,
}
#[automatically_derived]
impl<'a> DataRow1RowsIterator<'a> {
    fn create(_col_x: polars::prelude::PolarsResult<i32>) -> polars::prelude::PolarsResult<DataRow1> {
        Ok(DataRow1 { _col_x: _col_x? })
    }
}
impl<'a> Iterator for DataRow1RowsIterator<'a> {
    type Item = polars::prelude::PolarsResult<DataRow1>;
    fn next(&mut self) -> Option<Self::Item> {
        let _col_x_value = self._col_x_iter.next()?;
        Some(Self::create(_col_x_value))
    }
}

extern crate test;
#[cfg(test)]
#[rustc_test_marker = "test"]
pub const test: test::TestDescAndFn = test::TestDescAndFn {
    desc: test::TestDesc {
        name: test::StaticTestName("test"),
        ignore: false,
        ignore_message: ::core::option::Option::None,
        source_file: "polars-rows-iter/tests/impl.rs",
        start_line: 16usize,
        start_col: 4usize,
        end_line: 16usize,
        end_col: 8usize,
        compile_fail: false,
        no_run: false,
        should_panic: test::ShouldPanic::No,
        test_type: test::TestType::IntegrationTest,
    },
    testfn: test::StaticTestFn(
        #[coverage(off)]
        || test::assert_test_result(test()),
    ),
};
fn test() {
    let df =


            // struct RowRowsIterator<'a> {
            //     col_a_iter: Box<dyn Iterator<Item = PolarsResult<i32>> + 'a>,
            //     col_b_iter: Box<dyn Iterator<Item = PolarsResult<&'a str>> + 'a>,
            // }

            // impl<'a> RowRowsIterator<'a> {
            //     #[inline]
            //     fn create(col_a: PolarsResult<i32>, col_b: Result<&'a str, PolarsError>) -> PolarsResult<Row<'a>> {
            //         Ok(Row {
            //             col_a: col_a?,
            //             col_b: col_b?,
            //         })
            //     }
            // }

            // impl<'a> Iterator for RowRowsIterator<'a> {
            //     type Item = PolarsResult<Row<'a>>;

            //     fn next(&mut self) -> Option<Self::Item> {
            //         let col_a_value = self.col_a_iter.next()?;
            //         let col_b_value = self.col_b_iter.next()?;

            //         Some(Self::create(col_a_value, col_b_value))
            //     }
            // }

            // impl<'a> FromDataFrameRow<'a> for Row<'a> {
            //     fn from_dataframe(dataframe: &'a DataFrame) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
            //     where
            //         Self: Sized,
            //     {
            //         let col_a_iter = IterFromColumn::create_iter(dataframe.column("col_a")?)?;
            //         let col_b_iter = IterFromColumn::create_iter(dataframe.column("col_b")?)?;

            //         let iter: Box<dyn Iterator<Item = PolarsResult<Self>> + 'a> =
            //             Box::new(RowRowsIterator::<'a> { col_a_iter, col_b_iter });

            //         Ok(iter)
            //     }
            // }
            ::polars_core::prelude::DataFrame::new(<[_]>::into_vec(#[rustc_box] ::alloc::boxed::Box::new([::polars_core::prelude::Column::from(<::polars_core::prelude::Series
                                            as
                                            ::polars_core::prelude::NamedFrom<_,
                                            _>>::new("_col_x".into(), [1i32, 2, 3, 4])),
                                ::polars_core::prelude::Column::from(<::polars_core::prelude::Series
                                            as
                                            ::polars_core::prelude::NamedFrom<_,
                                            _>>::new("_col_y".into(),
                                        ["a", "b", "c", "d"]))]))).unwrap();
    {
        ::std::io::_print(format_args!("{0:?}\n", df));
    };
    let iter = df.rows_iter::<DataRow1>().unwrap();
    for row in iter {
        let row = row.unwrap();
        {
            ::std::io::_print(format_args!("{0:?}\n", row));
        };
    }
}
#[rustc_main]
#[coverage(off)]
pub fn main() -> () {
    extern crate test;
    test::test_main_static(&[&test])
}

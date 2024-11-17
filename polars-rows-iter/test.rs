#![feature(prelude_import)]
#[prelude_import]
use std::prelude::rust_2021::*;
#[macro_use]
extern crate std;
use polars::prelude::*;
use polars_rows_iter::*;

struct DataRow0<'a> {
    _col_x: i32,
    _col_y: &'a str,
}
#[automatically_derived]
impl<'a> ::core::fmt::Debug for DataRow0<'a> {
    #[inline]
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        ::core::fmt::Formatter::debug_struct_field2_finish(f, "DataRow0",
            "_col_x", &self._col_x, "_col_y", &&self._col_y)
    }
}

struct DataRow1 {
    _col_x: i32,
}
#[automatically_derived]
impl ::core::fmt::Debug for DataRow1 {
    #[inline]
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        ::core::fmt::Formatter::debug_struct_field1_finish(f, "DataRow1",
            "_col_x", &&self._col_x)
    }
}

extern crate test;
#[cfg(test)]
#[rustc_test_marker = "test"]
pub const test: test::TestDescAndFn =
    test::TestDescAndFn {



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
        testfn: test::StaticTestFn(#[coverage(off)] ||
                test::assert_test_result(test())),
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
                                            _>>::new("col_x".into(), [1i32, 2, 3, 4])),
                                ::polars_core::prelude::Column::from(<::polars_core::prelude::Series
                                            as
                                            ::polars_core::prelude::NamedFrom<_,
                                            _>>::new("col_y".into(),
                                        ["a", "b", "c", "d"]))]))).unwrap();
    { ::std::io::_print(format_args!("{0:?}\n", df)); };
    let iter = df.rows_iter::<DataRow0>().unwrap();
    for row in iter {
        let row = row.unwrap();
        { ::std::io::_print(format_args!("{0:?}\n", row)); };
    }
}
#[rustc_main]
#[coverage(off)]
pub fn main() -> () {
    extern crate test;
    test::test_main_static(&[&test])
}

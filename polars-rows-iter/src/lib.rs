//! # Polars rows iterator
//!
//! Simple and convenient iteration of polars dataframe rows.
//!
//! This crate provides two main approaches for iterating over DataFrame rows:
//! - **Struct-based iteration** using `#[derive(FromDataFrameRow)]` - best for complex rows with many columns
//! - **Tuple-based iteration** using the [`df_rows_iter!`] macro - best for quick, simple iterations
//!
//! ## Tuple-based iteration with `df_rows_iter!`
//!
//! For simple use cases where you don't need a dedicated struct, use the [`df_rows_iter!`] macro
//! to iterate over rows as tuples:
//!
//! ```rust
//! use polars::prelude::*;
//! use polars_rows_iter::*;
//!
//! fn main() {
//!     let df = df!(
//!         "name" => ["Alice", "Bob", "Charlie"],
//!         "age" => [25i32, 30, 35],
//!         "score" => [Some(95.5f64), None, Some(87.0)]
//!     ).unwrap();
//!
//!     let score_col = format!("sco{}", "re"); // dynamic column name
//!
//!     let iter = df_rows_iter!(
//!         &df,
//!         "name" => &str,       // string literal
//!         "age" => i32,
//!         score_col => Option<f64>  // variable
//!     ).unwrap();
//!
//!     for row in iter {
//!         let (name, age, score) = row.unwrap();
//!         println!("{name}: age {age}, score {score:?}");
//!     }
//! }
//! ```
//!
//! The macro supports tuples of up to 10 elements. Each element is specified as `column_name => Type`.
//! Column names can be string literals or any expression that implements `AsRef<str>`.
//!
//! ## Struct-based iteration with `FromDataFrameRow`
//!
//! For more complex use cases, derive `FromDataFrameRow` on a struct:
//! ```rust
//!use polars::prelude::*;
//!use polars_rows_iter::*;
//!
//!fn main() {
//!    #[derive(Debug, FromDataFrameRow)]
//!    #[derive(PartialEq)] // for assert_eq
//!    struct MyRow<'a>
//!    {
//!        #[column("col_a")]
//!        a: i32,
//!        // the column name defaults to the field name if no explicit name given
//!        col_b: &'a str,
//!        col_c: String,
//!        #[column("col_d")]
//!        optional: Option<f64>
//!    }
//!   
//!    let df = df!(
//!            "col_a" => [1i32, 2, 3, 4, 5],
//!            "col_b" => ["a", "b", "c", "d", "e"],
//!            "col_c" => ["A", "B", "C", "D", "E"],
//!            "col_d" => [Some(1.0f64), None, None, Some(2.0), Some(3.0)]
//!        ).unwrap();
//!   
//!    let rows_iter = df.rows_iter::<MyRow>().unwrap(); // ready to use row iterator
//!    // collect to vector for assert_eq
//!    let rows_vec = rows_iter.collect::<PolarsResult<Vec<MyRow>>>().unwrap();
//!   
//!    assert_eq!(
//!        rows_vec,
//!        [
//!            MyRow { a: 1, col_b: "a", col_c: "A".to_string(), optional: Some(1.0) },
//!            MyRow { a: 2, col_b: "b", col_c: "B".to_string(), optional: None },
//!            MyRow { a: 3, col_b: "c", col_c: "C".to_string(), optional: None },
//!            MyRow { a: 4, col_b: "d", col_c: "D".to_string(), optional: Some(2.0) },
//!            MyRow { a: 5, col_b: "e", col_c: "E".to_string(), optional: Some(3.0) },
//!        ]
//!    );
//!}
//! ```
//! Every row is wrapped with a PolarsError, in case of an unexpected null value the row creation fails and the iterator
//! returns an Err(...) for the row. One can decide to cancel the iteration or to skip the affected row.
//!
//! ## Column Name Transformations
//!
//! The `#[from_dataframe(...)]` attribute allows automatic transformation of field names to column names:
//!
//! ```rust
//! use polars::prelude::*;
//! use polars_rows_iter::*;
//!
//! #[derive(Debug, FromDataFrameRow)]
//! #[from_dataframe(convert_case(Pascal), prefix("col_"))]
//! struct MyRow {
//!     user_name: String,  // maps to column "col_UserName"
//!     age: i32,           // maps to column "col_Age"
//! }
//!
//! fn main() {
//!     let df = df!(
//!         "col_UserName" => ["Alice", "Bob"],
//!         "col_Age" => [25i32, 30]
//!     ).unwrap();
//!
//!     let rows: Vec<MyRow> = df.rows_iter::<MyRow>()
//!         .unwrap()
//!         .collect::<PolarsResult<Vec<_>>>()
//!         .unwrap();
//!
//!     assert_eq!(rows[0].user_name, "Alice");
//!     assert_eq!(rows[0].age, 25);
//! }
//! ```
//!
//! ### Available options:
//!
//! - `convert_case(Case)` - Convert field names using a case style. Supported cases:
//!   `Upper`, `Lower`, `Title`, `Toggle`, `Camel`, `Pascal`, `UpperCamel`, `Snake`,
//!   `UpperSnake`, `ScreamingSnake`, `Kebab`, `Cobol`, `UpperKebab`, `Train`, `Flat`,
//!   `UpperFlat`, `Alternating`
//! - `prefix("str")` - Add a prefix to all column names
//! - `postfix("str")` - Add a postfix/suffix to all column names
//!
//! These can be combined: `#[from_dataframe(convert_case(Snake), prefix("data_"), postfix("_col"))]`
//!
//! Individual fields can still override with `#[column("explicit_name")]`.
//!
//! ## Supported types
//!
//! |State|Rust Type|Supported Polars DataType|Feature Flag|
//! |--|--|--|--|
//! |✓|`bool`|`Boolean`
//! |✓|`u8`|`UInt8`
//! |✓|`u16`|`UInt16`
//! |✓|`u32`|`UInt32`
//! |✓|`u64`|`UInt64`
//! |✓|`i8`|`Int8`
//! |✓|`i16`|`Int16`
//! |✓|`i32`|`Int32`
//! |✓|`i32`|`Date`
//! |✓|`i64`|`Int64`
//! |✓|`i64`|`Datetime(..)`
//! |✓|`i64`|`Duration(..)`
//! |✓|`i64`|`Time`
//! |✓|`f32`|`Float32`
//! |✓|`f64`|`Float64`
//! |✓|`&str`|`String`
//! |✓|`&str`|`Categorical(..)`|`dtype-categorical`
//! |✓|`&str`|`Enum(..)`|`dtype-categorical`
//! |✓|`String`|`String`
//! |✓|`String`|`Categorical(..)`|`dtype-categorical`
//! |✓|`String`|`Enum(..)`|`dtype-categorical`
//! |✓|`&[u8]`|`Binary`
//! |✓|`&[u8]`|`BinaryOffset`
//! |✓|`chrono::NaiveDateTime`|`Datetime(..)`|`chrono`
//! |✓|`chrono::DateTime<Utc>`|`Datetime(..)`|`chrono`
//! |✓|`chrono::Date`|`Date`|`chrono`|
//! |?|?|`List(..)`
//! |?|?|`Array(..)`|
//! |?|?|`Decimal(..)`|
//! |?|?|`Struct(..)`|
//! |X|X|`Null`
//! |X|X|`Unknown(..)`|
//! |X|X|`Object(..)`|
//!
//! TODO: Support is planned <br>
//! ?: Support not yet certain<br>
//! X: No Support

extern crate self as polars_rows_iter;

mod dataframe_rows_iter_ext;
mod from_dataframe_row;
mod iter_from_column;
#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use convert_case;
pub use dataframe_rows_iter_ext::*;
pub use from_dataframe_row::*;
pub use iter_from_column::*;
use polars_rows_iter_derive::impl_tuple_rows_iter;
pub use polars_rows_iter_derive::FromDataFrameRow;

impl_tuple_rows_iter!(10);

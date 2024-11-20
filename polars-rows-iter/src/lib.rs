//! # Polars rows iterator
//!
//! Simple and convenient iteration of polars dataframe rows.
//!
//! ##### Example: Dataframe without None/null values:
//! ```rust
//! use polars::prelude::*;
//! use polars_rows_iter::*;
//!
//! #[derive(Debug, FromDataFrameRow)]
//! #[derive(PartialEq)] // for assert_eq
//! struct MyRow<'a>
//! {
//!     #[column("col_a")]
//!     a: i32,
//!     // the column name defaults to the field name if no explicit name given
//!     col_b: &'a str
//! }
//!
//! let df = df!(
//!         "col_a" => [1i32, 2, 3, 4, 5],
//!         "col_b" => ["a", "b", "c", "d", "e"]
//!     ).unwrap();
//!
//! let rows_iter = df.rows_iter::<MyRow>().unwrap(); // ready to use row iterator
//! // unwrap rows and collect to vector for assert_eq
//! let rows_vec = rows_iter.map(|row|row.unwrap()).collect::<Vec<MyRow>>();
//!
//! assert_eq!(
//!     rows_vec,
//!     [
//!         MyRow { a: 1, col_b: "a" },
//!         MyRow { a: 2, col_b: "b" },
//!         MyRow { a: 3, col_b: "c" },
//!         MyRow { a: 4, col_b: "d" },
//!         MyRow { a: 5, col_b: "e" },
//!     ]
//! );
//! ```
//! Every row is wrapped with a PolarsError, in case of an unexpected null value the row creation fails and the iterator
//! returns an Err(...) for the row. One can decide to cancel the iteration or to skip the affected row.
//!
//! ##### Example: Dataframe with valid None/null values:
//! ```rust
//! use polars::prelude::*;
//! use polars_rows_iter::*;
//!
//! #[derive(Debug, FromDataFrameRow)]
//! #[derive(PartialEq)] // for assert_eq
//! struct MyRow<'a>
//! {
//!     col_a: i32,
//!     col_b: Option<&'a str>
//! }
//!
//! let df = df!(
//!         "col_a" => [1i32, 2, 3, 4, 5],
//!         "col_b" => [Some("a"), None, Some("c"), None, Some("e")]
//!     ).unwrap();
//!
//! let rows_iter = df.rows_iter::<MyRow>().unwrap(); // ready to use row iterator
//! // unwrap rows and collect to vector for assert_eq
//! let rows_vec = rows_iter.map(|row|row.unwrap()).collect::<Vec<MyRow>>();
//!
//! assert_eq!(
//!     rows_vec,
//!     [
//!         MyRow { col_a: 1, col_b: Some("a") },
//!         MyRow { col_a: 2, col_b: None },
//!         MyRow { col_a: 3, col_b: Some("c") },
//!         MyRow { col_a: 4, col_b: None },
//!         MyRow { col_a: 5, col_b: Some("e") },
//!     ]
//! );
//! ```
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
//! |TODO|?|`Binary`
//! |TODO|?|`BinaryOffset`
//! |✓|`chrono::NaiveDateTime`|`Datetime(..)`|`chrono`
//! |✓|`chrono::DateTime<Utc>`|`Datetime(..)`|`chrono`
//! |✓|`chrono::Date`|`Date`|`chrono`|
//! |?|?|`List(..)`
//! |?|?|`Array(..)`|`dtype-array`
//! |?|?|`Enum(..)`|`dtype-categorical`
//! |?|?|`Decimal(..)`|`dtype-decimal`
//! |?|?|`Struct(..)`|`dtype-struct`
//! |X|X|`Null`
//! |X|X|`Unknown(..)`|
//! |X|X|`Object(..)`|`object`
//!
//! TODO: Support is planned <br>
//! ?: Support not yet certain<br>
//! X: No Support
//!
//! ## Limitations
//! * Currently supports only primitive and string/categorical types
//! * No generics in row structs supported

mod dataframe_rows_iter_ext;
mod from_dataframe_row;
mod iter_from_column;

pub use dataframe_rows_iter_ext::*;
pub use from_dataframe_row::*;
pub use iter_from_column::*;
pub use polars_rows_iter_derive::FromDataFrameRow;

#[cfg(test)]
pub mod shared_test_helpers;

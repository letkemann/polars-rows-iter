//! # Polars rows iterator
//!
//! Simple and convenient iteration of polars dataframe rows.
//!
//! ##### Example: Dataframe without None/null values:
//! ```rust
//! use polars::prelude::*;
//! use polars_rows_iter::*;
//!
//! #[derive(Debug, FromDataFrameRow, PartialEq)]
//! struct MyRow<'a>
//! {
//!     col_a: i32,
//!     col_b: &'a str
//! }
//!
//! let df = df!(
//!         "col_a" => [1i32, 2, 3, 4, 5],
//!         "col_b" => ["a", "b", "c", "d", "e"]
//!     ).unwrap();
//!
//! let rows_iter = df.rows_iter::<MyRow>().unwrap();
//! // unwrap rows and collect to vector
//! let rows_vec = rows_iter.map(|row|row.unwrap()).collect::<Vec<MyRow>>();
//!
//! assert_eq!(
//!     rows_vec,
//!     [
//!         MyRow { col_a: 1, col_b: "a" },
//!         MyRow { col_a: 2, col_b: "b" },
//!         MyRow { col_a: 3, col_b: "c" },
//!         MyRow { col_a: 4, col_b: "d" },
//!         MyRow { col_a: 5, col_b: "e" },
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
//! #[derive(Debug, FromDataFrameRow, PartialEq)]
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
//! let rows_iter = df.rows_iter::<MyRow>().unwrap();
//! // unwrap rows and collect to vector
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
//! ## Limitations
//! Currently supports only primitive and string/categorical types

mod dataframe_rows_iter_ext;
mod from_dataframe_row;
mod iter_from_column;

pub use dataframe_rows_iter_ext::*;
pub use from_dataframe_row::*;
pub use iter_from_column::*;
pub use polars_rows_iter_derive::FromDataFrameRow;

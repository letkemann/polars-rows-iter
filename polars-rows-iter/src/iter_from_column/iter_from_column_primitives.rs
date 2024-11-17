use super::{value_functions::*, *};
use iter_from_column::IterFromColumn;
use polars_rows_iter_derive::iter_from_column_for_type;

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

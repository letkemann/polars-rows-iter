use super::*;
use iter_from_column_trait::IterFromColumn;
use polars_rows_iter_derive::iter_from_column_for_type;

iter_from_column_for_type!(bool);
iter_from_column_for_type!(i8);
iter_from_column_for_type!(i16);
// i32 has specific definition
// i64 has specific definition
iter_from_column_for_type!(u8);
iter_from_column_for_type!(u16);
iter_from_column_for_type!(u32);
iter_from_column_for_type!(u64);
iter_from_column_for_type!(f32);
iter_from_column_for_type!(f64);

#[cfg(test)]
mod tests {

    use crate::*;
    use itertools::{izip, Itertools};
    use polars::prelude::*;
    use rand::{rngs::StdRng, SeedableRng};
    use shared_test_helpers::*;

    const ROW_COUNT: usize = 64;

    create_rows_iter_test_for_chunked_type!(bool_test, bool, bool, DataType::Boolean, ROW_COUNT);
    create_rows_iter_test_for_chunked_type!(i8_test, i8, i8, DataType::Int8, ROW_COUNT);
    create_rows_iter_test_for_chunked_type!(i16_test, i16, i16, DataType::Int16, ROW_COUNT);
    create_rows_iter_test_for_chunked_type!(i32_test, i32, i32, DataType::Int32, ROW_COUNT);
    create_rows_iter_test_for_chunked_type!(u8_test, u8, u8, DataType::UInt8, ROW_COUNT);
    create_rows_iter_test_for_chunked_type!(u16_test, u16, u16, DataType::UInt16, ROW_COUNT);
    create_rows_iter_test_for_chunked_type!(u32_test, u32, u32, DataType::UInt32, ROW_COUNT);
    create_rows_iter_test_for_chunked_type!(u64_test, u64, u64, DataType::UInt64, ROW_COUNT);
    create_rows_iter_test_for_chunked_type!(f32_test, f32, f32, DataType::Float32, ROW_COUNT);
    create_rows_iter_test_for_chunked_type!(f64_test, f64, f64, DataType::Float64, ROW_COUNT);
}

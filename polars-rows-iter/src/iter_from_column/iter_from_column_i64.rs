use super::*;
use polars::prelude::*;

impl<'a> IterFromColumn<'a> for i64 {
    type RawInner = i64;

    #[inline]
    fn get_value(polars_value: Option<i64>, column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        polars_value.ok_or_else(|| <i64 as IterFromColumn<'a>>::unexpected_null_value_error(column_name))
    }
}

impl<'a> IterFromColumn<'a> for Option<i64> {
    type RawInner = i64;

    #[inline]
    fn get_value(polars_value: Option<i64>, _column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        Ok(polars_value)
    }
}

#[cfg(test)]
mod tests {

    const ROW_COUNT: usize = 64;

    use crate::*;
    use itertools::{izip, Itertools};
    use polars::prelude::*;
    use rand::{rngs::StdRng, SeedableRng};
    use testing::*;

    create_rows_iter_test_for_chunked_type!(i64_test, i64, i64, DataType::Int64, ROW_COUNT);

    create_rows_iter_test_for_logical_type!(
        i64_as_datetime_milliseconds_test,
        i64,
        datetime,
        DataType::Datetime(TimeUnit::Milliseconds, None),
        ROW_COUNT
    );

    create_rows_iter_test_for_logical_type!(
        i64_as_datetime_microseconds_test,
        i64,
        datetime,
        DataType::Datetime(TimeUnit::Microseconds, None),
        ROW_COUNT
    );

    create_rows_iter_test_for_logical_type!(
        i64_as_datetime_nanoseconds_test,
        i64,
        datetime,
        DataType::Datetime(TimeUnit::Nanoseconds, None),
        ROW_COUNT
    );

    #[cfg(feature = "dtype-time")]
    create_rows_iter_test_for_logical_type!(i64_as_time_test, i64, time, DataType::Time, ROW_COUNT);

    create_rows_iter_test_for_logical_type!(
        i64_as_duration_milliseconds_test,
        i64,
        duration,
        DataType::Duration(TimeUnit::Milliseconds),
        ROW_COUNT
    );

    create_rows_iter_test_for_logical_type!(
        i64_as_duration_microseconds_test,
        i64,
        duration,
        DataType::Duration(TimeUnit::Microseconds),
        ROW_COUNT
    );

    create_rows_iter_test_for_logical_type!(
        i64_as_duration_nanoseconds_test,
        i64,
        duration,
        DataType::Duration(TimeUnit::Nanoseconds),
        ROW_COUNT
    );
}

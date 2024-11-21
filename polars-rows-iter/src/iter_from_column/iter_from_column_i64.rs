use super::*;
use polars::prelude::*;

impl<'a> IterFromColumn<'a> for i64 {
    type RawInner = i64;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<i64>> + 'a>> {
        create_iter(column)
    }

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
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<i64>> + 'a>> {
        create_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<i64>, _column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        Ok(polars_value)
    }
}

fn create_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<i64>> + 'a>> {
    let column_name = column.name().as_str();
    let iter = match column.dtype() {
        DataType::Int64 => Box::new(column.i64()?.iter()),
        DataType::Time => Box::new(column.as_materialized_series().time()?.iter()),
        DataType::Datetime(_, _) => Box::new(column.datetime()?.iter()),
        DataType::Duration(_) => Box::new(column.duration()?.iter()),
        dtype => {
            return Err(polars_err!(SchemaMismatch: "Cannot get i64 from column '{column_name}' with dtype : {dtype}"))
        }
    };

    Ok(iter)
}

#[cfg(test)]
mod tests {

    const ROW_COUNT: usize = 64;

    use crate::*;
    use itertools::{izip, Itertools};
    use polars::prelude::*;
    use rand::{rngs::StdRng, SeedableRng};
    use shared_test_helpers::*;

    create_test_for_type!(i64_test, i64, i64, DataType::Int64, ROW_COUNT);

    create_test_for_type!(
        i64_as_datetime_milliseconds_test,
        i64,
        datetime,
        DataType::Datetime(TimeUnit::Milliseconds, None),
        ROW_COUNT
    );

    create_test_for_type!(
        i64_as_datetime_microseconds_test,
        i64,
        datetime,
        DataType::Datetime(TimeUnit::Microseconds, None),
        ROW_COUNT
    );

    create_test_for_type!(
        i64_as_datetime_nanoseconds_test,
        i64,
        datetime,
        DataType::Datetime(TimeUnit::Nanoseconds, None),
        ROW_COUNT
    );

    #[cfg(feature = "dtype-time")]
    create_test_for_type!(i64_as_time_test, i64, time, DataType::Time, ROW_COUNT);

    create_test_for_type!(
        i64_as_duration_milliseconds_test,
        i64,
        duration,
        DataType::Duration(TimeUnit::Milliseconds),
        ROW_COUNT
    );

    create_test_for_type!(
        i64_as_duration_microseconds_test,
        i64,
        duration,
        DataType::Duration(TimeUnit::Microseconds),
        ROW_COUNT
    );

    create_test_for_type!(
        i64_as_duration_nanoseconds_test,
        i64,
        duration,
        DataType::Duration(TimeUnit::Nanoseconds),
        ROW_COUNT
    );
}

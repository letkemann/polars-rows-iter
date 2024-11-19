use super::*;
use polars::prelude::*;

impl<'a> IterFromColumn<'a, i64> for i64 {
    fn create_iter(
        dataframe: &'a DataFrame,
        column_name: &'a str,
    ) -> PolarsResult<Box<dyn Iterator<Item = Option<i64>> + 'a>> {
        create_iter(dataframe, column_name)
    }

    #[inline]
    fn get_value(polars_value: Option<i64>, column_name: &'a str) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        polars_value.ok_or_else(|| polars::prelude::polars_err!(SchemaMismatch: "Found unexpected None/null value in column {column_name} with mandatory values!"))
    }
}

impl<'a> IterFromColumn<'a, i64> for Option<i64> {
    fn create_iter(
        dataframe: &'a DataFrame,
        column_name: &'a str,
    ) -> PolarsResult<Box<dyn Iterator<Item = Option<i64>> + 'a>> {
        create_iter(dataframe, column_name)
    }
}
fn create_iter<'a>(
    dataframe: &'a DataFrame,
    column_name: &'a str,
) -> PolarsResult<Box<dyn Iterator<Item = Option<i64>> + 'a>> {
    let column = dataframe.column(column_name)?;

    let iter = match column.dtype() {
        DataType::Int64 => Box::new(column.i64()?.into_iter()),
        DataType::Datetime(_, _) => Box::new(column.datetime()?.into_iter()),
        dtype => {
            return Err(polars_err!(SchemaMismatch: "Cannot get &str from column '{column_name}' with dtype : {dtype}"))
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
        datetime_milliseconds_as_i64_test,
        i64,
        datetime,
        DataType::Datetime(TimeUnit::Milliseconds, None),
        ROW_COUNT
    );

    create_test_for_type!(
        datetime_microseconds_as_i64_test,
        i64,
        datetime,
        DataType::Datetime(TimeUnit::Microseconds, None),
        ROW_COUNT
    );

    create_test_for_type!(
        datetime_nanoseconds_as_i64_test,
        i64,
        datetime,
        DataType::Datetime(TimeUnit::Nanoseconds, None),
        ROW_COUNT
    );
}

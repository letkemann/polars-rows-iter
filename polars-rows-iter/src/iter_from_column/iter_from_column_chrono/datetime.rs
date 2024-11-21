use crate::*;
use chrono::{DateTime, Utc};
use polars::prelude::*;

impl<'a> IterFromColumn<'a> for DateTime<Utc> {
    type RawInner = i64;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<i64>> + 'a>>
    where
        Self: Sized,
    {
        create_datetime_iter(column)
    }

    fn get_value(polars_value: Option<i64>, column_name: &str, dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        let timestamp = polars_value
            .ok_or_else(|| <DateTime<Utc> as IterFromColumn<'a>>::unexpected_null_value_error(column_name))?;

        create_datetime(timestamp, column_name, dtype)
    }
}

impl<'a> IterFromColumn<'a> for Option<DateTime<Utc>> {
    type RawInner = i64;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<i64>> + 'a>>
    where
        Self: Sized,
    {
        create_datetime_iter(column)
    }

    fn get_value(polars_value: Option<i64>, column_name: &str, dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        polars_value
            .map(|timestamp| create_datetime(timestamp, column_name, dtype))
            .transpose()
    }
}

fn create_datetime_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<i64>> + 'a>> {
    let iter = column.datetime()?.iter();
    Ok(Box::new(iter))
}

fn create_datetime(timestamp: i64, column_name: &str, dtype: &DataType) -> PolarsResult<DateTime<Utc>> {
    let (time_unit, _time_zone) = if let DataType::Datetime(tu, tz) = dtype {
        (tu, tz)
    } else {
        return Err(polars_err!(SchemaMismatch: "Unable to create chrono::DateTime from DataType: {dtype}"));
    };

    match time_unit {
        TimeUnit::Nanoseconds => Ok(DateTime::from_timestamp_nanos(timestamp)),
        TimeUnit::Microseconds => DateTime::from_timestamp_micros(timestamp)
            .ok_or_else(|| polars_err!(OutOfBounds: "Value {timestamp} in column {column_name} is not a valid microseconds timestamp")),
        TimeUnit::Milliseconds => DateTime::from_timestamp_millis(timestamp)
            .ok_or_else(|| polars_err!(OutOfBounds: "Value {timestamp} in column {column_name} is not a valid nanoseconds timestamp")),
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;

    use super::*;

    #[test]
    fn datetime_should_be_read_from_datetime_millis() {
        let column_id = Column::new("id".into(), vec![1i32, 2, 3, 4, 5]);
        let column_datetime = Column::new(
            "dt".into(),
            vec![
                1732122821000i64,
                1700500421000,
                1668964421000,
                1637428421000,
                1605892421000,
            ],
        )
        .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
        .unwrap();
        let column_datetime_opt = Column::new(
            "dt_opt".into(),
            vec![
                Some(1605892421000i64),
                None,
                Some(1700500421000),
                None,
                Some(1637428421000),
            ],
        )
        .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
        .unwrap();

        let df = DataFrame::new(vec![column_id, column_datetime, column_datetime_opt]).unwrap();

        #[derive(Debug, PartialEq, FromDataFrameRow)]
        struct TestRow {
            id: i32,
            dt: DateTime<Utc>,
            dt_opt: Option<DateTime<Utc>>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|row| row.unwrap()).collect_vec();

        assert_eq!(
            rows,
            vec![
                TestRow {
                    id: 1,
                    dt: DateTime::<Utc>::from_timestamp(1732122821, 0).unwrap(),
                    dt_opt: Some(DateTime::<Utc>::from_timestamp(1605892421, 0).unwrap())
                },
                TestRow {
                    id: 2,
                    dt: DateTime::<Utc>::from_timestamp(1700500421, 0).unwrap(),
                    dt_opt: None
                },
                TestRow {
                    id: 3,
                    dt: DateTime::<Utc>::from_timestamp(1668964421, 0).unwrap(),
                    dt_opt: Some(DateTime::<Utc>::from_timestamp(1700500421, 0).unwrap())
                },
                TestRow {
                    id: 4,
                    dt: DateTime::<Utc>::from_timestamp(1637428421, 0).unwrap(),
                    dt_opt: None
                },
                TestRow {
                    id: 5,
                    dt: DateTime::<Utc>::from_timestamp(1605892421, 0).unwrap(),
                    dt_opt: Some(DateTime::<Utc>::from_timestamp(1637428421, 0).unwrap())
                }
            ]
        )
    }
}

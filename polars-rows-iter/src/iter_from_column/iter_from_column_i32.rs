use super::*;
use polars::prelude::*;

impl<'a> IterFromColumn<'a> for i32 {
    type RawInner = i32;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<i32>> + 'a>> {
        create_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<i32>, column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        polars_value.ok_or_else(|| <i32 as IterFromColumn<'a>>::unexpected_null_value_error(column_name))
    }
}

impl<'a> IterFromColumn<'a> for Option<i32> {
    type RawInner = i32;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<i32>> + 'a>> {
        create_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<i32>, _column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        Ok(polars_value)
    }
}

fn create_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<i32>> + 'a>> {
    let column_name = column.name().as_str();
    let iter = match column.dtype() {
        DataType::Int32 => Box::new(column.i32()?.into_iter()),
        DataType::Date => Box::new(column.date()?.into_iter()),
        dtype => {
            return Err(polars_err!(SchemaMismatch: "Cannot get i32 from column '{column_name}' with dtype : {dtype}"))
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

    create_test_for_type!(i32_test, i32, i32, DataType::Int32, ROW_COUNT);

    create_test_for_type!(i32_as_date_test, i32, date, DataType::Date, ROW_COUNT);

    #[test]
    fn my_test<'a>() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = 64;
        let dtype = DataType::Time;

        let col = create_column("col", dtype.clone(), false, height, &mut rng);
        let col_opt = create_column("col_opt", dtype, true, height, &mut rng);

        let col_values = col
            .as_series()
            .unwrap()
            .time()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect_vec();

        let col_opt_values = col_opt.as_series().unwrap().time().unwrap().into_iter().collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_iter = col_values.into_iter();
        let col_opt_iter = col_opt_values.into_iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(col, col_opt)| TestRow { col, col_opt })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow {
            col: i64,
            col_opt: Option<i64>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }
}

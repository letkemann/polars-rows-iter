use crate::*;
use polars::prelude::*;

impl<'a> IterFromColumn<'a> for Series {
    type RawInner = Series;
    fn create_iter(column: &'a Column) -> PolarsResult<impl Iterator<Item = Option<Series>> + 'a> {
        create_series_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<Series>, column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        polars_value.ok_or_else(|| <Series as IterFromColumn<'a>>::unexpected_null_value_error(column_name))
    }
}

impl<'a> IterFromColumn<'a> for Option<Series> {
    type RawInner = Series;
    fn create_iter(column: &'a Column) -> PolarsResult<impl Iterator<Item = Option<Series>> + 'a> {
        create_series_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<Series>, _column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        Ok(polars_value)
    }
}

pub(crate) fn create_series_iter<'a>(column: &'a Column) -> PolarsResult<impl Iterator<Item = Option<Series>> + 'a> {
    let column_name = column.name().as_str();
    let iter: Box<dyn Iterator<Item = Option<Series>>> = match column.dtype() {
        DataType::List(_) => Box::new(column.list()?.into_iter()),
        dtype => {
            return Err(
                polars_err!(SchemaMismatch: "Cannot get Series from column '{column_name}' with dtype: {dtype}"),
            )
        }
    };

    Ok(iter)
}

#[cfg(test)]
mod tests {
    use crate::*;
    use itertools::{izip, Itertools};
    use polars::prelude::*;
    use rand::{rngs::StdRng, SeedableRng};
    use testing::*;

    const ROW_COUNT: usize = 64;

    #[test]
    fn series_rows_iter_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::List(Box::new(DataType::Int32));

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col.list().unwrap().into_iter().map(|v| v.unwrap()).collect_vec();
        let col_opt_values = col_opt.list().unwrap().into_iter().collect_vec();

        let df = DataFrame::new(height, vec![col, col_opt]).unwrap();

        let col_iter = col_values.into_iter();
        let col_opt_iter = col_opt_values.into_iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(col, col_opt)| TestRow { col, col_opt })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow {
            col: Series,
            col_opt: Option<Series>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }

    #[test]
    fn series_scalar_iter_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::List(Box::new(DataType::Int32));

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col.list().unwrap().into_iter().map(|v| v.unwrap()).collect_vec();

        let df = DataFrame::new(height, vec![col, col_opt]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<Series>>>()
            .unwrap();

        assert_eq!(values, col_values)
    }

    #[test]
    fn series_scalar_iter_opt_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::List(Box::new(DataType::Int32));

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_opt_values = col_opt.list().unwrap().into_iter().collect_vec();

        let df = DataFrame::new(height, vec![col, col_opt]).unwrap();

        let values = df
            .scalar_iter("col_opt")
            .unwrap()
            .collect::<PolarsResult<Vec<Option<Series>>>>()
            .unwrap();

        assert_eq!(values, col_opt_values)
    }
}

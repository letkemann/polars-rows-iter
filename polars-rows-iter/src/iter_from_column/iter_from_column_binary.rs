use crate::*;
use polars::prelude::*;

impl<'a> IterFromColumn<'a> for &'a [u8] {
    type RawInner = &'a [u8];
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a [u8]>> + 'a>> {
        create_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<&'a [u8]>, column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        polars_value.ok_or_else(|| <&[u8] as IterFromColumn<'a>>::unexpected_null_value_error(column_name))
    }
}

impl<'a> IterFromColumn<'a> for Option<&'a [u8]> {
    type RawInner = &'a [u8];
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a [u8]>> + 'a>> {
        create_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<&'a [u8]>, _column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        Ok(polars_value)
    }
}

fn create_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a [u8]>> + 'a>> {
    let column_name = column.name().as_str();
    let iter: Box<dyn Iterator<Item = Option<&[u8]>>> = match column.dtype() {
        DataType::Binary => Box::new(column.binary()?.iter()),
        DataType::BinaryOffset => Box::new(column.binary_offset()?.iter()),
        dtype => {
            return Err(
                polars_err!(SchemaMismatch: "Cannot get &[u8] from column '{column_name}' with dtype : {dtype}"),
            )
        }
    };

    Ok(iter)
}

#[cfg(test)]
mod tests {
    const ROW_COUNT: usize = 64;

    use super::*;
    use itertools::{izip, Itertools};
    use rand::{rngs::StdRng, SeedableRng};
    use shared_test_helpers::*;

    #[test]
    fn binary_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::Binary;

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col.clone();
        let col_values = col_values.binary().unwrap().iter().map(|v| v.unwrap()).collect_vec();

        let col_opt_values = col_opt.clone();
        let col_opt_values = col_opt_values.binary().unwrap().iter().collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_iter = col_values.iter();
        let col_opt_iter = col_opt_values.iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(&col, &col_opt)| TestRow { col, col_opt })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow<'a> {
            col: &'a [u8],
            col_opt: Option<&'a [u8]>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }

    #[test]
    fn binary_offset_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::BinaryOffset;

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col.clone();
        let col_values = col_values
            .binary_offset()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect_vec();

        let col_opt_values = col_opt.clone();
        let col_opt_values = col_opt_values.binary_offset().unwrap().iter().collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_iter = col_values.iter();
        let col_opt_iter = col_opt_values.iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(&col, &col_opt)| TestRow { col, col_opt })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow<'a> {
            col: &'a [u8],
            col_opt: Option<&'a [u8]>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }
}

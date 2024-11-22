use super::iter_from_column_str::create_iter;
use super::*;
use iter_from_column_trait::IterFromColumn;
use polars::prelude::*;

impl<'a> IterFromColumn<'a> for String {
    type RawInner = &'a str;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
        create_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<&'a str>, column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        Ok(polars_value
            .ok_or_else(|| <&'a str as IterFromColumn<'a>>::unexpected_null_value_error(column_name))?
            .to_string())
    }
}

impl<'a> IterFromColumn<'a> for Option<String> {
    type RawInner = &'a str;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
        create_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<&'a str>, _column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        Ok(polars_value.map(|s| s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use itertools::{izip, Itertools};
    use polars::prelude::*;
    use rand::{rngs::StdRng, SeedableRng};
    use shared_test_helpers::*;

    const ROW_COUNT: usize = 64;

    #[test]
    fn str_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::String;

        let col = create_column("col", dtype.clone(), false, height, &mut rng);
        let col_opt = create_column("col_opt", dtype, true, height, &mut rng);

        let col_values = col.str().unwrap().iter().map(|v| v.unwrap().to_owned()).collect_vec();
        let col_opt_values = col_opt
            .str()
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.to_owned()))
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_iter = col_values.into_iter();
        let col_opt_iter = col_opt_values.into_iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(col, col_opt)| TestRow { col, col_opt })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow {
            col: String,
            col_opt: Option<String>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }

    #[cfg(feature = "dtype-categorical")]
    #[test]
    fn cat_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::Categorical(None, CategoricalOrdering::Physical);

        let col = create_column("col", dtype.clone(), false, height, &mut rng);
        let col_opt = create_column("col_opt", dtype, true, height, &mut rng);

        let col_values = col
            .categorical()
            .unwrap()
            .iter_str()
            .map(|v| v.unwrap().to_owned())
            .collect_vec();
        let col_opt_values = col_opt
            .categorical()
            .unwrap()
            .iter_str()
            .map(|v| v.map(|s| s.to_owned()))
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_iter = col_values.into_iter();
        let col_opt_iter = col_opt_values.into_iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(col, col_opt)| TestRow { col, col_opt })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow {
            col: String,
            col_opt: Option<String>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }

    #[cfg(feature = "dtype-categorical")]
    #[test]
    fn enum_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;

        let enum_value_series = Series::new("enum".into(), &["A", "B", "C", "D", "E"]);
        let categories = enum_value_series.str().unwrap().downcast_iter().next().unwrap().clone();
        let dtype = create_enum_dtype(categories);

        let col = create_column("col", dtype.clone(), false, height, &mut rng);
        let col_opt = create_column("col_opt", dtype, true, height, &mut rng);

        let col_values = col
            .categorical()
            .unwrap()
            .iter_str()
            .map(|v| v.unwrap().to_owned())
            .collect_vec();
        let col_opt_values = col_opt
            .categorical()
            .unwrap()
            .iter_str()
            .map(|v| v.map(|s| s.to_owned()))
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_iter = col_values.into_iter();
        let col_opt_iter = col_opt_values.into_iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(col, col_opt)| TestRow { col, col_opt })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow {
            col: String,
            col_opt: Option<String>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }
}

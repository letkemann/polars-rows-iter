use crate::{iter_from_column::iter_from_column_series::create_series_iter, *};
use polars::prelude::*;

impl<'a, T> IterFromColumn<'a> for Vec<T>
where
    T: for<'inner> IterFromColumn<'inner>,
{
    type RawInner = Series;
    fn create_iter(column: &'a Column) -> PolarsResult<impl Iterator<Item = Option<Series>> + 'a> {
        create_series_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<Series>, column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        polars_value
            .map(|series| collect_inner_values::<T>(series, column_name))
            .transpose()?
            .ok_or_else(|| <Series as IterFromColumn<'a>>::unexpected_null_value_error(column_name))
    }
}

impl<'a, T> IterFromColumn<'a> for Option<Vec<T>>
where
    T: for<'inner> IterFromColumn<'inner>,
{
    type RawInner = Series;
    fn create_iter(column: &'a Column) -> PolarsResult<impl Iterator<Item = Option<Series>> + 'a> {
        create_series_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<Series>, column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        polars_value
            .map(|series| collect_inner_values::<T>(series, column_name))
            .transpose()
    }
}

fn collect_inner_values<T>(series: Series, column_name: &str) -> PolarsResult<Vec<T>>
where
    T: for<'inner> IterFromColumn<'inner>,
{
    let column = series.into_column();
    let column_dtype = column.dtype().clone();

    let result = <T as IterFromColumn>::create_iter(&column)?
        .map(|v| <T as IterFromColumn>::get_value(v, column_name, &column_dtype))
        .collect();
    result
}

#[cfg(test)]
mod tests {
    use crate::*;
    use itertools::Itertools;
    use polars::prelude::*;

    // ---- Vec<f64> / Option<Vec<f64>> ----

    #[test]
    fn vec_f64_rows_iter_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &[1.0f64, 2.0, 3.0]);
        let s2 = Series::new(PlSmallStr::EMPTY, &[4.0f64, 5.0]);
        let s3 = Series::new(PlSmallStr::EMPTY, &[6.0f64]);

        let col = Column::new("col".into(), vec![s1.clone(), s2.clone(), s3.clone()]);
        let col_opt = Column::new("col_opt".into(), vec![Some(s1), None, Some(s3)]);

        let df = DataFrame::new(3, vec![col, col_opt]).unwrap();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow {
            col: Vec<f64>,
            col_opt: Option<Vec<f64>>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(
            rows,
            vec![
                TestRow {
                    col: vec![1.0, 2.0, 3.0],
                    col_opt: Some(vec![1.0, 2.0, 3.0])
                },
                TestRow {
                    col: vec![4.0, 5.0],
                    col_opt: None
                },
                TestRow {
                    col: vec![6.0],
                    col_opt: Some(vec![6.0])
                },
            ]
        );
    }

    #[test]
    fn vec_f64_scalar_iter_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &[1.0f64, 2.0]);
        let s2 = Series::new(PlSmallStr::EMPTY, &[3.0f64]);

        let col = Column::new("col".into(), vec![s1, s2]);
        let df = DataFrame::new(2, vec![col]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<Vec<f64>>>>()
            .unwrap();

        assert_eq!(values, vec![vec![1.0, 2.0], vec![3.0]]);
    }

    #[test]
    fn vec_f64_scalar_iter_opt_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &[1.0f64, 2.0]);

        let col = Column::new("col".into(), vec![Some(s1), None]);
        let df = DataFrame::new(2, vec![col]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<Option<Vec<f64>>>>>()
            .unwrap();

        assert_eq!(values, vec![Some(vec![1.0, 2.0]), None]);
    }

    // ---- Vec<Option<f64>> / Option<Vec<Option<f64>>> ----

    #[test]
    fn vec_option_f64_rows_iter_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &[Some(1.0f64), None, Some(3.0)]);
        let s2 = Series::new(PlSmallStr::EMPTY, &[Some(4.0f64), Some(5.0)]);
        let s3 = Series::new(PlSmallStr::EMPTY, &[None::<f64>]);

        let col = Column::new("col".into(), vec![s1.clone(), s2.clone(), s3.clone()]);
        let col_opt = Column::new("col_opt".into(), vec![Some(s1), None, Some(s3)]);

        let df = DataFrame::new(3, vec![col, col_opt]).unwrap();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow {
            col: Vec<Option<f64>>,
            col_opt: Option<Vec<Option<f64>>>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(
            rows,
            vec![
                TestRow {
                    col: vec![Some(1.0), None, Some(3.0)],
                    col_opt: Some(vec![Some(1.0), None, Some(3.0)])
                },
                TestRow {
                    col: vec![Some(4.0), Some(5.0)],
                    col_opt: None
                },
                TestRow {
                    col: vec![None],
                    col_opt: Some(vec![None])
                },
            ]
        );
    }

    #[test]
    fn vec_option_f64_scalar_iter_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &[Some(1.0f64), None]);
        let s2 = Series::new(PlSmallStr::EMPTY, &[Some(3.0f64)]);

        let col = Column::new("col".into(), vec![s1, s2]);
        let df = DataFrame::new(2, vec![col]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<Vec<Option<f64>>>>>()
            .unwrap();

        assert_eq!(values, vec![vec![Some(1.0), None], vec![Some(3.0)]]);
    }

    #[test]
    fn vec_option_f64_scalar_iter_opt_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &[Some(1.0f64), None]);

        let col = Column::new("col".into(), vec![Some(s1), None]);
        let df = DataFrame::new(2, vec![col]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<Option<Vec<Option<f64>>>>>>()
            .unwrap();

        assert_eq!(values, vec![Some(vec![Some(1.0), None]), None]);
    }

    // ---- Vec<String> / Option<Vec<String>> ----

    #[test]
    fn vec_string_rows_iter_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &["a", "b", "c"]);
        let s2 = Series::new(PlSmallStr::EMPTY, &["d", "e"]);
        let s3 = Series::new(PlSmallStr::EMPTY, &["f"]);

        let col = Column::new("col".into(), vec![s1.clone(), s2.clone(), s3.clone()]);
        let col_opt = Column::new("col_opt".into(), vec![Some(s1), None, Some(s3)]);

        let df = DataFrame::new(3, vec![col, col_opt]).unwrap();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow {
            col: Vec<String>,
            col_opt: Option<Vec<String>>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(
            rows,
            vec![
                TestRow {
                    col: vec!["a".into(), "b".into(), "c".into()],
                    col_opt: Some(vec!["a".into(), "b".into(), "c".into()])
                },
                TestRow {
                    col: vec!["d".into(), "e".into()],
                    col_opt: None
                },
                TestRow {
                    col: vec!["f".into()],
                    col_opt: Some(vec!["f".into()])
                },
            ]
        );
    }

    #[test]
    fn vec_string_scalar_iter_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &["a", "b"]);
        let s2 = Series::new(PlSmallStr::EMPTY, &["c"]);

        let col = Column::new("col".into(), vec![s1, s2]);
        let df = DataFrame::new(2, vec![col]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<Vec<String>>>>()
            .unwrap();

        assert_eq!(
            values,
            vec![vec!["a".to_string(), "b".to_string()], vec!["c".to_string()],]
        );
    }

    #[test]
    fn vec_string_scalar_iter_opt_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &["a", "b"]);

        let col = Column::new("col".into(), vec![Some(s1), None]);
        let df = DataFrame::new(2, vec![col]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<Option<Vec<String>>>>>()
            .unwrap();

        assert_eq!(values, vec![Some(vec!["a".to_string(), "b".to_string()]), None]);
    }

    // ---- Vec<Option<String>> / Option<Vec<Option<String>>> ----

    #[test]
    fn vec_option_string_rows_iter_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &[Some("a"), None, Some("c")]);
        let s2 = Series::new(PlSmallStr::EMPTY, &[Some("d"), Some("e")]);
        let s3 = Series::new(PlSmallStr::EMPTY, &[None::<&str>]);

        let col = Column::new("col".into(), vec![s1.clone(), s2.clone(), s3.clone()]);
        let col_opt = Column::new("col_opt".into(), vec![Some(s1), None, Some(s3)]);

        let df = DataFrame::new(3, vec![col, col_opt]).unwrap();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow {
            col: Vec<Option<String>>,
            col_opt: Option<Vec<Option<String>>>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(
            rows,
            vec![
                TestRow {
                    col: vec![Some("a".into()), None, Some("c".into())],
                    col_opt: Some(vec![Some("a".into()), None, Some("c".into())])
                },
                TestRow {
                    col: vec![Some("d".into()), Some("e".into())],
                    col_opt: None
                },
                TestRow {
                    col: vec![None],
                    col_opt: Some(vec![None])
                },
            ]
        );
    }

    #[test]
    fn vec_option_string_scalar_iter_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &[Some("a"), None]);
        let s2 = Series::new(PlSmallStr::EMPTY, &[Some("c")]);

        let col = Column::new("col".into(), vec![s1, s2]);
        let df = DataFrame::new(2, vec![col]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<Vec<Option<String>>>>>()
            .unwrap();

        assert_eq!(
            values,
            vec![vec![Some("a".to_string()), None], vec![Some("c".to_string())],]
        );
    }

    #[test]
    fn vec_option_string_scalar_iter_opt_test() {
        let s1 = Series::new(PlSmallStr::EMPTY, &[Some("a"), None]);

        let col = Column::new("col".into(), vec![Some(s1), None]);
        let df = DataFrame::new(2, vec![col]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<Option<Vec<Option<String>>>>>>()
            .unwrap();

        assert_eq!(values, vec![Some(vec![Some("a".to_string()), None]), None]);
    }

    // ---- Error cases ----

    #[test]
    fn vec_f64_errors_on_null_inner_values() {
        let column_name = "col";

        let s1 = Series::new(PlSmallStr::EMPTY, &[Some(1.0f64), None]);

        let col = Column::new(column_name.into(), vec![s1]);
        let df = DataFrame::new(1, vec![col]).unwrap();

        let err_msg = df
            .scalar_iter(column_name)
            .unwrap()
            .collect::<PolarsResult<Vec<Vec<f64>>>>()
            .unwrap_err()
            .to_string();

        assert_eq!(
            err_msg,
            format!("Found unexpected None/null value in column '{column_name}' with mandatory values!"),
        );
    }

    #[test]
    fn vec_f64_errors_on_non_list_column() {
        let column_name = "col";

        let col = Column::new(column_name.into(), &[1.0f64, 2.0, 3.0]);
        let df = DataFrame::new(3, vec![col]).unwrap();

        let err_msg = df.scalar_iter::<Vec<f64>>(column_name).err().unwrap().to_string();

        assert_eq!(
            err_msg,
            format!("Cannot get Series from column '{column_name}' with dtype: f64")
        );
    }
}

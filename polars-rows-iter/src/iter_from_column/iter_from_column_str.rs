use super::*;
use iter_from_column_trait::IterFromColumn;
use polars::prelude::*;

impl<'a> IterFromColumn<'a> for &'a str {
    type RawInner = &'a str;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
        create_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<&'a str>, column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        polars_value.ok_or_else(|| <&'a str as IterFromColumn<'a>>::unexpected_null_value_error(column_name))
    }
}

impl<'a> IterFromColumn<'a> for Option<&'a str> {
    type RawInner = &'a str;
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
        create_iter(column)
    }

    #[inline]
    fn get_value(polars_value: Option<&'a str>, _column_name: &str, _dtype: &DataType) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        Ok(polars_value)
    }
}

fn create_str_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    Ok(Box::new(column.str()?.iter()))
}

#[cfg(feature = "dtype-categorical")]
fn create_cat_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    Ok(Box::new(column.cat32()?.iter_str()))
}

#[cfg(feature = "dtype-categorical")]
fn create_enum_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    Ok(Box::new(column.cat8()?.iter_str()))
}

pub fn create_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    let iter = match column.dtype() {
        DataType::String => create_str_iter(column)?,
        #[cfg(feature = "dtype-categorical")]
        DataType::Categorical(_, _) => create_cat_iter(column)?,
        #[cfg(feature = "dtype-categorical")]
        DataType::Enum(_, _) => create_enum_iter(column)?,
        dtype => {
            let column_name = column.name().as_str();
            return Err(
                polars_err!(SchemaMismatch: "Cannot get &str from column '{column_name}' with dtype '{dtype}'.\
                                             Make sure to enable 'dtype-categorical' feature for 'Categorical' and 'Enum' dtypes."),
            );
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
    use shared_test_helpers::*;

    const ROW_COUNT: usize = 64;

    #[test]
    fn str_rows_iter_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::String;

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col.str().unwrap().iter().map(|v| v.unwrap().to_owned()).collect_vec();
        let col_opt_values = col_opt
            .str()
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.to_owned()))
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_iter = col_values.iter();
        let col_opt_iter = col_opt_values.iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(col, col_opt)| TestRow {
                col: col.as_ref(),
                col_opt: col_opt.as_ref().map(|v| v.as_str()),
            })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow<'a> {
            col: &'a str,
            col_opt: Option<&'a str>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }

    #[test]
    fn str_scalar_iter_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::String;

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col.str().unwrap().iter().map(|v| v.unwrap().to_owned()).collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<&str>>>()
            .unwrap();

        assert_eq!(values, col_values)
    }

    #[test]
    fn str_scalar_iter_opt_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::String;

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_opt_values = col_opt
            .str()
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.to_owned()))
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_opt_values = col_opt_values
            .iter()
            .map(|v| v.as_ref().map(|s| s.as_str()))
            .collect_vec();

        let values = df
            .scalar_iter("col_opt")
            .unwrap()
            .collect::<PolarsResult<Vec<Option<&str>>>>()
            .unwrap();

        assert_eq!(values, col_opt_values)
    }

    #[cfg(feature = "dtype-categorical")]
    #[test]
    fn cat_rows_iter_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;

        let cats = Categories::new(PlSmallStr::EMPTY, PlSmallStr::EMPTY, CategoricalPhysical::U32);
        let dtype = DataType::from_categories(cats);

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col
            .cat32()
            .unwrap()
            .iter_str()
            .map(|v| v.unwrap().to_owned())
            .collect_vec();
        let col_opt_values = col_opt
            .cat32()
            .unwrap()
            .iter_str()
            .map(|v| v.map(|s| s.to_owned()))
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_iter = col_values.iter();
        let col_opt_iter = col_opt_values.iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(col, col_opt)| TestRow {
                col: col.as_ref(),
                col_opt: col_opt.as_ref().map(|v| v.as_str()),
            })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow<'a> {
            col: &'a str,
            col_opt: Option<&'a str>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }

    #[cfg(feature = "dtype-categorical")]
    #[test]
    fn cat_scalar_iter_test() {
        use crate::DataframeRowsIterExt;

        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;

        let cats = Categories::new(PlSmallStr::EMPTY, PlSmallStr::EMPTY, CategoricalPhysical::U32);
        let dtype = DataType::from_categories(cats);

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col
            .cat32()
            .unwrap()
            .iter_str()
            .map(|v| v.unwrap().to_owned())
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<&str>>>()
            .unwrap();

        assert_eq!(values, col_values)
    }

    #[cfg(feature = "dtype-categorical")]
    #[test]
    fn cat_rows_iter_opt_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;

        let cats = Categories::new(PlSmallStr::EMPTY, PlSmallStr::EMPTY, CategoricalPhysical::U32);
        let dtype = DataType::from_categories(cats);

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_opt_values = col_opt
            .cat32()
            .unwrap()
            .iter_str()
            .map(|v| v.map(|s| s.to_owned()))
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_opt_values = col_opt_values
            .iter()
            .map(|v| v.as_ref().map(|s| s.as_str()))
            .collect_vec();

        let values = df
            .scalar_iter("col_opt")
            .unwrap()
            .collect::<PolarsResult<Vec<Option<&str>>>>()
            .unwrap();

        assert_eq!(values, col_opt_values)
    }

    #[cfg(feature = "dtype-categorical")]
    #[test]
    fn enum_rows_iter_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;

        let categories = FrozenCategories::new(["A", "B", "C", "D", "E"]).unwrap();
        let dtype = DataType::from_frozen_categories(categories);

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col
            .cat8()
            .unwrap()
            .iter_str()
            .map(|v| v.unwrap().to_owned())
            .collect_vec();
        let col_opt_values = col_opt
            .cat8()
            .unwrap()
            .iter_str()
            .map(|v| v.map(|s| s.to_owned()))
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_iter = col_values.iter();
        let col_opt_iter = col_opt_values.iter();

        let expected_rows = izip!(col_iter, col_opt_iter)
            .map(|(col, col_opt)| TestRow {
                col: col.as_ref(),
                col_opt: col_opt.as_ref().map(|v| v.as_str()),
            })
            .collect_vec();

        #[derive(Debug, FromDataFrameRow, PartialEq)]
        struct TestRow<'a> {
            col: &'a str,
            col_opt: Option<&'a str>,
        }

        let rows = df.rows_iter::<TestRow>().unwrap().map(|v| v.unwrap()).collect_vec();

        assert_eq!(rows, expected_rows)
    }

    #[cfg(feature = "dtype-categorical")]
    #[test]
    fn enum_scalar_iter_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;

        let categories = FrozenCategories::new(["A", "B", "C", "D", "E"]).unwrap();
        let dtype = DataType::from_frozen_categories(categories);

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_values = col
            .cat8()
            .unwrap()
            .iter_str()
            .map(|v| v.unwrap().to_owned())
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let values = df
            .scalar_iter("col")
            .unwrap()
            .collect::<PolarsResult<Vec<&str>>>()
            .unwrap();

        assert_eq!(values, col_values)
    }

    #[cfg(feature = "dtype-categorical")]
    #[test]
    fn enum_scalar_iter_opt_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;

        let categories = FrozenCategories::new(["A", "B", "C", "D", "E"]).unwrap();
        let dtype = DataType::from_frozen_categories(categories);

        let col = create_column("col", &dtype, false, height, &mut rng);
        let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

        let col_opt_values = col_opt
            .cat8()
            .unwrap()
            .iter_str()
            .map(|v| v.map(|s| s.to_owned()))
            .collect_vec();

        let df = DataFrame::new(vec![col, col_opt]).unwrap();

        let col_opt_values = col_opt_values
            .iter()
            .map(|v| v.as_ref().map(|s| s.as_str()))
            .collect_vec();

        let values = df
            .scalar_iter("col_opt")
            .unwrap()
            .collect::<PolarsResult<Vec<Option<&str>>>>()
            .unwrap();

        assert_eq!(values, col_opt_values)
    }
}

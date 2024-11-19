use super::*;
use iter_from_column::IterFromColumn;
use polars_rows_iter_derive::iter_from_column_for_type;

iter_from_column_for_type!(bool);
iter_from_column_for_type!(i8);
iter_from_column_for_type!(i16);
iter_from_column_for_type!(i32);
iter_from_column_for_type!(i64);
iter_from_column_for_type!(u8);
iter_from_column_for_type!(u16);
iter_from_column_for_type!(u32);
iter_from_column_for_type!(u64);
iter_from_column_for_type!(f32);
iter_from_column_for_type!(f64);

#[cfg(test)]
mod tests {

    use crate::*;
    use itertools::{izip, Itertools};
    use polars::prelude::*;
    use rand::{rngs::StdRng, SeedableRng};
    use shared_test_helpers::*;

    const ROW_COUNT: usize = 64;

    macro_rules! create_test_for_type {
        ($func_name:ident, $type:ty, $type_name:ident, $dtype:expr, $height:ident) => {
            #[test]
            fn $func_name<'a>() {
                let mut rng = StdRng::seed_from_u64(0);
                let height = $height;
                let dtype = $dtype;

                let col = create_column("col", dtype.clone(), false, height, &mut rng);
                let col_opt = create_column("col_opt", dtype, true, height, &mut rng);

                let col_values = col
                    .$type_name()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.unwrap())
                    .collect_vec();
                let col_opt_values = col_opt.$type_name().unwrap().into_iter().collect_vec();

                let df = DataFrame::new(vec![col, col_opt]).unwrap();

                let col_iter = col_values.into_iter();
                let col_opt_iter = col_opt_values.into_iter();

                let expected_rows = izip!(col_iter, col_opt_iter)
                    .map(|(col, col_opt)| TestRow { col, col_opt })
                    .collect_vec();

                #[derive(Debug, FromDataFrameRow, PartialEq)]
                struct TestRow {
                    col: $type,
                    col_opt: Option<$type>,
                }

                let rows = df
                    .rows_iter::<TestRow>()
                    .unwrap()
                    .map(|v| v.unwrap())
                    .collect_vec();

                assert_eq!(rows, expected_rows)
            }
        };
    }

    create_test_for_type!(bool_test, bool, bool, DataType::Boolean, ROW_COUNT);
    create_test_for_type!(i8_test, i8, i8, DataType::Int8, ROW_COUNT);
    create_test_for_type!(i16_test, i16, i16, DataType::Int16, ROW_COUNT);
    create_test_for_type!(i32_test, i32, i32, DataType::Int32, ROW_COUNT);
    create_test_for_type!(i64_test, i64, i64, DataType::Int64, ROW_COUNT);
    create_test_for_type!(u8_test, u8, u8, DataType::UInt8, ROW_COUNT);
    create_test_for_type!(u16_test, u16, u16, DataType::UInt16, ROW_COUNT);
    create_test_for_type!(u32_test, u32, u32, DataType::UInt32, ROW_COUNT);
    create_test_for_type!(u64_test, u64, u64, DataType::UInt64, ROW_COUNT);
    create_test_for_type!(f32_test, f32, f32, DataType::Float32, ROW_COUNT);
    create_test_for_type!(f64_test, f64, f64, DataType::Float64, ROW_COUNT);

    #[test]
    fn str_test() {
        let mut rng = StdRng::seed_from_u64(0);
        let height = ROW_COUNT;
        let dtype = DataType::String;

        let col = create_column("col", dtype.clone(), false, height, &mut rng);
        let col_opt = create_column("col_opt", dtype, true, height, &mut rng);

        let col_values = col
            .str()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap().to_owned())
            .collect_vec();
        let col_opt_values = col_opt
            .str()
            .unwrap()
            .into_iter()
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
}

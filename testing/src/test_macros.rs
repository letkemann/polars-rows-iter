#[macro_export]
macro_rules! create_rows_iter_test_for_chunked_type {
    ($func_name:ident, $type:ty, $type_name:ident, $dtype:expr, $height:ident) => {
        #[test]
        fn $func_name() {
            let mut rng = StdRng::seed_from_u64(0);
            let height = $height;
            let dtype = $dtype;

            let col = create_column("col", &dtype, false, height, &mut rng);
            let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

            let col_values = col
                .as_series()
                .unwrap()
                .$type_name()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect_vec();

            let col_opt_values = col_opt
                .as_series()
                .unwrap()
                .$type_name()
                .unwrap()
                .iter()
                .collect_vec();

            let df = DataFrame::new(vec![col, col_opt]).unwrap();

            let col_iter = col_values.iter();
            let col_opt_iter = col_opt_values.iter();

            let expected_rows = izip!(col_iter, col_opt_iter)
                .map(|(&col, &col_opt)| TestRow { col, col_opt })
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

#[macro_export]
macro_rules! create_rows_iter_test_for_logical_type {
    ($func_name:ident, $type:ty, $type_name:ident, $dtype:expr, $height:ident) => {
        #[test]
        fn $func_name() {
            let mut rng = StdRng::seed_from_u64(0);
            let height = $height;
            let dtype = $dtype;

            let col = create_column("col", &dtype, false, height, &mut rng);
            let col_opt = create_column("col_opt", &dtype, true, height, &mut rng);

            let col_values = col
                .as_series()
                .unwrap()
                .$type_name()
                .unwrap()
                .phys
                .iter()
                .map(|v| v.unwrap())
                .collect_vec();

            let col_opt_values = col_opt
                .as_series()
                .unwrap()
                .$type_name()
                .unwrap()
                .phys
                .iter()
                .collect_vec();

            let df = DataFrame::new(vec![col, col_opt]).unwrap();

            let col_iter = col_values.iter();
            let col_opt_iter = col_opt_values.iter();

            let expected_rows = izip!(col_iter, col_opt_iter)
                .map(|(&col, &col_opt)| TestRow { col, col_opt })
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

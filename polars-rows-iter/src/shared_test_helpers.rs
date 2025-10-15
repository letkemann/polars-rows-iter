use itertools::Itertools;
use polars::prelude::*;
use rand::{
    distr::{Alphanumeric, Distribution, StandardUniform},
    rngs::StdRng,
    Rng, SeedableRng,
};
use std::collections::HashMap;

pub type IsOptional = bool;

const TIME64_MAX_VALUE: i64 = 24 * 60 * 60 * 1_000_000_000;

#[derive(Debug, Clone)]
pub struct ColumnType(pub DataType, pub IsOptional);

pub fn create_values<T, F>(height: usize, mut get_value: F) -> Vec<T>
where
    F: FnMut() -> T,
{
    let mut values = Vec::<T>::with_capacity(height);
    for _ in 0..height {
        values.push(get_value());
    }

    values
}

pub fn create_optional_bool(rng: &mut StdRng) -> Option<bool> {
    let is_none = rng.random_bool(0.5);
    if !is_none {
        Some(rng.random_bool(0.5))
    } else {
        None
    }
}

pub fn create_optional_number<T>(rng: &mut StdRng) -> Option<T>
where
    StandardUniform: Distribution<T>,
{
    let is_none = rng.random_bool(0.5);
    if !is_none {
        Some(rng.random())
    } else {
        None
    }
}

pub fn create_optional<T, F>(rng: &mut StdRng, mut create_value: F) -> Option<T>
where
    F: FnMut(&mut StdRng) -> T,
{
    let is_none = rng.random_bool(0.5);
    if !is_none {
        Some(create_value(rng))
    } else {
        None
    }
}

pub fn create_random_string(rng: &mut StdRng) -> String {
    let size: usize = rng.random_range(4..32);
    rng.sample_iter(&Alphanumeric).take(size).map(char::from).collect()
}

pub fn create_random_binary(rng: &mut StdRng) -> Vec<u8> {
    let size: usize = rng.random_range(4..32);
    rng.sample_iter(&Alphanumeric).take(size).collect()
}

pub fn create_enum_values<'a>(
    categories: &'a FrozenCategories,
    mapping: &'a CategoricalMapping,
    height: usize,
    rng: &mut StdRng,
) -> Vec<&'a str> {
    (0..height)
        .map(|_| {
            let enum_index = rng.random_range(0..categories.categories().len() as u32);
            mapping.cat_to_str(enum_index).unwrap()
        })
        .collect()
}

pub fn create_optional_enum_values<'a>(
    categories: &'a FrozenCategories,
    mapping: &'a CategoricalMapping,
    height: usize,
    rng: &mut StdRng,
) -> Vec<Option<&'a str>> {
    (0..height)
        .map(move |_| {
            let enum_index = rng.random_range(0..categories.categories().len() as u32);
            match rng.random_bool(0.5) {
                true => mapping.cat_to_str(enum_index),
                false => None,
            }
        })
        .collect()
}

pub fn create_column(name: &str, dtype: &DataType, optional: IsOptional, height: usize, rng: &mut StdRng) -> Column {
    // println!("Creating column {name} with type {dtype} (optional: {optional})");
    let name = name.into();
    match dtype {
        DataType::Boolean => match optional {
            true => Column::new(name, create_values(height, || create_optional_bool(rng))),
            false => Column::new(name, create_values(height, || rng.random_bool(0.5))),
        },
        DataType::UInt8 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<u8>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<u8>())),
        },
        DataType::UInt16 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<u16>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<u16>())),
        },
        DataType::UInt32 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<u32>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<u32>())),
        },
        DataType::UInt64 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<u64>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<u64>())),
        },
        DataType::Int8 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i8>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<i8>())),
        },
        DataType::Int16 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i16>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<i16>())),
        },
        DataType::Int32 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i32>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<i32>())),
        },
        DataType::Int64 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i64>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<i64>())),
        },
        DataType::Float32 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<f32>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<f32>())),
        },
        DataType::Float64 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<f64>(rng))),
            false => Column::new(name, create_values(height, || rng.random::<f64>())),
        },
        DataType::String => match optional {
            true => Column::new(
                name,
                create_values(height, || create_optional(rng, create_random_string)),
            ),
            false => Column::new(name, create_values(height, || create_random_string(rng))),
        },
        DataType::Categorical(mapping, ordering) => match optional {
            true => Column::new(
                name,
                create_values(height, || create_optional(rng, create_random_string)),
            )
            .cast(&DataType::Categorical(mapping.clone(), ordering.clone()))
            .unwrap(),
            false => Column::new(name, create_values(height, || create_random_string(rng)))
                .cast(&DataType::Categorical(mapping.clone(), ordering.clone()))
                .unwrap(),
        },
        DataType::Enum(categories, mapping) => match optional {
            true => Column::new(
                name,
                create_optional_enum_values(categories.as_ref(), mapping.as_ref(), height, rng),
            )
            .cast(&DataType::Enum(categories.clone(), mapping.clone()))
            .unwrap(),
            false => Column::new(
                name,
                create_enum_values(categories.as_ref(), mapping.as_ref(), height, rng),
            )
            .cast(&DataType::Enum(categories.clone(), mapping.clone()))
            .unwrap(),
        },
        DataType::Datetime(unit, zone) => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i64>(rng)))
                .cast(&DataType::Datetime(*unit, zone.clone()))
                .unwrap(),
            false => Column::new(name, create_values(height, || rng.random::<i64>()))
                .cast(&DataType::Datetime(*unit, zone.clone()))
                .unwrap(),
        },
        DataType::Date => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i32>(rng)))
                .cast(&DataType::Date)
                .unwrap(),
            false => Column::new(name, create_values(height, || rng.random::<i32>()))
                .cast(&DataType::Date)
                .unwrap(),
        },
        DataType::Time => match optional {
            true => Column::new(
                name,
                create_values(height, || {
                    create_optional(rng, |rng| {
                        let value: i64 = rng.random_range(0..TIME64_MAX_VALUE);
                        value
                    })
                }),
            )
            .cast(&DataType::Time)
            .unwrap(),
            false => Column::new(
                name,
                create_values(height, || {
                    let value: i64 = rng.random_range(0..TIME64_MAX_VALUE);
                    value
                }),
            )
            .cast(&DataType::Time)
            .unwrap(),
        },
        DataType::Duration(unit) => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i64>(rng)))
                .cast(&DataType::Duration(*unit))
                .unwrap(),
            false => Column::new(name, create_values(height, || rng.random::<i64>()))
                .cast(&DataType::Duration(*unit))
                .unwrap(),
        },
        DataType::Binary => match optional {
            true => Column::new(
                name,
                create_values(height, || create_optional(rng, create_random_binary)),
            ),
            false => Column::new(name, create_values(height, || create_random_binary(rng))),
        },
        DataType::BinaryOffset => match optional {
            true => {
                let values: LargeBinaryArray = create_values(height, || create_optional(rng, create_random_binary))
                    .into_iter()
                    .collect();

                BinaryOffsetChunked::from(values).into_column().with_name(name)
            }
            false => {
                let values: LargeBinaryArray = create_values(height, || Some(create_random_binary(rng)))
                    .into_iter()
                    .collect();

                BinaryOffsetChunked::from(values).into_column().with_name(name)
            }
        },
        DataType::List(dtype) => {
            if optional {
                let series = (0..height)
                    .map(|_| {
                        if rng.random_bool(0.5) {
                            Some(
                                create_column("", dtype.as_ref(), optional, height, rng)
                                    .as_materialized_series()
                                    .clone(),
                            )
                        } else {
                            None
                        }
                    })
                    .collect_vec();

                Column::new(name, series)
            } else {
                let series = (0..height)
                    .map(|_| {
                        create_column("", dtype.as_ref(), optional, height, rng)
                            .as_materialized_series()
                            .clone()
                    })
                    .collect_vec();

                Column::new(name, series)
            }
        }
        _ => todo!(),
    }
}

pub fn create_dataframe(columns: HashMap<&str, ColumnType>, height: usize) -> DataFrame {
    let mut rng = StdRng::seed_from_u64(0);
    let columns = columns
        .into_iter()
        .map(|(name, ColumnType(dtype, optional))| create_column(name, dtype.as_ref(), optional, height, &mut rng))
        .collect::<Vec<Column>>();

    DataFrame::new(columns).unwrap()
}

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

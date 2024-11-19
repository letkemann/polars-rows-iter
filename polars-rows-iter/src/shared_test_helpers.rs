use std::collections::HashMap;

use polars::prelude::*;
use rand::{
    distributions::{Alphanumeric, Distribution, Standard},
    rngs::StdRng,
    Rng, SeedableRng,
};

pub type IsOptional = bool;

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
    let is_none = rng.gen_bool(0.5);
    if !is_none {
        Some(rng.gen_bool(0.5))
    } else {
        None
    }
}

pub fn create_optional_number<T>(rng: &mut StdRng) -> Option<T>
where
    Standard: Distribution<T>,
{
    let is_none = rng.gen_bool(0.5);
    if !is_none {
        Some(rng.gen())
    } else {
        None
    }
}

pub fn create_optional<T, F>(rng: &mut StdRng, mut create_value: F) -> Option<T>
where
    F: FnMut(&mut StdRng) -> T,
{
    let is_none = rng.gen_bool(0.5);
    if !is_none {
        Some(create_value(rng))
    } else {
        None
    }
}

pub fn create_random_string(rng: &mut StdRng) -> String {
    let size: usize = rng.gen_range(4..32);
    rng.sample_iter(&Alphanumeric).take(size).map(char::from).collect()
}

pub fn create_column(name: &str, dtype: DataType, optional: IsOptional, height: usize, rng: &mut StdRng) -> Column {
    // println!("Creating column {name} with type {dtype} (optional: {optional})");
    let name = name.into();
    match dtype {
        DataType::Boolean => match optional {
            true => Column::new(name, create_values(height, || create_optional_bool(rng))),
            false => Column::new(name, create_values(height, || rng.gen_bool(0.5))),
        },
        DataType::UInt8 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<u8>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<u8>())),
        },
        DataType::UInt16 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<u16>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<u16>())),
        },
        DataType::UInt32 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<u32>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<u32>())),
        },
        DataType::UInt64 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<u64>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<u64>())),
        },
        DataType::Int8 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i8>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<i8>())),
        },
        DataType::Int16 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i16>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<i16>())),
        },
        DataType::Int32 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i32>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<i32>())),
        },
        DataType::Int64 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<i64>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<i64>())),
        },
        DataType::Float32 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<f32>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<f32>())),
        },
        DataType::Float64 => match optional {
            true => Column::new(name, create_values(height, || create_optional_number::<f64>(rng))),
            false => Column::new(name, create_values(height, || rng.gen::<f64>())),
        },
        DataType::String => match optional {
            true => Column::new(
                name,
                create_values(height, || create_optional(rng, create_random_string)),
            ),
            false => Column::new(name, create_values(height, || create_random_string(rng))),
        },
        DataType::Categorical(_, ordering) => match optional {
            true => Column::new(
                name,
                create_values(height, || create_optional(rng, create_random_string)),
            )
            .cast(&DataType::Categorical(None, ordering))
            .unwrap(),
            false => Column::new(name, create_values(height, || create_random_string(rng)))
                .cast(&DataType::Categorical(None, ordering))
                .unwrap(),
        },
        _ => todo!(),
    }
}

pub fn create_dataframe(columns: HashMap<&str, ColumnType>, height: usize) -> DataFrame {
    let mut rng = StdRng::seed_from_u64(0);
    let columns = columns
        .into_iter()
        .map(|(name, ColumnType(dtype, optional))| create_column(name, dtype, optional, height, &mut rng))
        .collect::<Vec<Column>>();

    DataFrame::new(columns).unwrap()
}

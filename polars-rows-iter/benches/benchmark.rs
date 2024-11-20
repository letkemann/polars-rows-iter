use criterion::*;
use itertools::izip;
use polars::prelude::*;
use polars_rows_iter::*;
use std::{collections::HashMap, time::Duration};
pub type IsOptional = bool;

#[path = "../src/shared_test_helpers.rs"]
mod shared;
use shared::*;

fn create_all_column_types_dataframe(height: usize) -> DataFrame {
    let columns = [
        ("_col_bool", ColumnType(DataType::Boolean, false)),
        ("_col_bool_opt", ColumnType(DataType::Boolean, true)),
        ("_col_i32", ColumnType(DataType::Int32, false)),
        ("_col_i32_opt", ColumnType(DataType::Int32, true)),
        ("_col_u32", ColumnType(DataType::UInt32, false)),
        ("_col_u32_opt", ColumnType(DataType::UInt32, true)),
        ("_col_i64", ColumnType(DataType::Int64, false)),
        ("_col_i64_opt", ColumnType(DataType::Int64, true)),
        ("_col_u64", ColumnType(DataType::UInt64, false)),
        ("_col_u64_opt", ColumnType(DataType::UInt64, true)),
        ("_col_f32", ColumnType(DataType::Float32, false)),
        ("_col_f32_opt", ColumnType(DataType::Float32, true)),
        ("_col_f64", ColumnType(DataType::Float64, false)),
        ("_col_f64_opt", ColumnType(DataType::Float64, true)),
        ("_col_str", ColumnType(DataType::String, false)),
        ("_col_str_opt", ColumnType(DataType::String, true)),
        (
            "_col_cat",
            ColumnType(DataType::Categorical(None, CategoricalOrdering::Physical), false),
        ),
        (
            "_col_cat_opt",
            ColumnType(DataType::Categorical(None, CategoricalOrdering::Physical), true),
        ),
    ]
    .into_iter()
    .collect::<HashMap<&str, ColumnType>>();

    create_dataframe(columns.clone(), height)
}

#[derive(Debug, FromDataFrameRow)]
struct AllTypesRow<'a> {
    _col_bool: bool,
    _col_bool_opt: Option<bool>,
    _col_i32: i32,
    _col_i32_opt: Option<i32>,
    _col_u32: u32,
    _col_u32_opt: Option<u32>,
    _col_i64: i64,
    _col_i64_opt: Option<i64>,
    _col_u64: u64,
    _col_u64_opt: Option<u64>,
    _col_f32: f32,
    _col_f32_opt: Option<f32>,
    _col_f64: f64,
    _col_f64_opt: Option<f64>,
    _col_str: &'a str,
    _col_str_opt: Option<&'a str>,
    _col_cat: &'a str,
    _col_cat_opt: Option<&'a str>,
}

fn create_primitive_column_types_dataframe(height: usize) -> DataFrame {
    let columns = [
        ("_col_bool", ColumnType(DataType::Boolean, false)),
        ("_col_bool_opt", ColumnType(DataType::Boolean, true)),
        ("_col_i32", ColumnType(DataType::Int32, false)),
        ("_col_i32_opt", ColumnType(DataType::Int32, true)),
        ("_col_u32", ColumnType(DataType::UInt32, false)),
        ("_col_u32_opt", ColumnType(DataType::UInt32, true)),
        ("_col_i64", ColumnType(DataType::Int64, false)),
        ("_col_i64_opt", ColumnType(DataType::Int64, true)),
        ("_col_u64", ColumnType(DataType::UInt64, false)),
        ("_col_u64_opt", ColumnType(DataType::UInt64, true)),
        ("_col_f32", ColumnType(DataType::Float32, false)),
        ("_col_f32_opt", ColumnType(DataType::Float32, true)),
        ("_col_f64", ColumnType(DataType::Float64, false)),
        ("_col_f64_opt", ColumnType(DataType::Float64, true)),
    ]
    .into_iter()
    .collect::<HashMap<&str, ColumnType>>();

    create_dataframe(columns.clone(), height)
}

#[derive(Debug, FromDataFrameRow)]
struct PrimitiveTypesRow {
    _col_bool: bool,
    _col_bool_opt: Option<bool>,
    _col_i32: i32,
    _col_i32_opt: Option<i32>,
    _col_u32: u32,
    _col_u32_opt: Option<u32>,
    _col_i64: i64,
    _col_i64_opt: Option<i64>,
    _col_u64: u64,
    _col_u64_opt: Option<u64>,
    _col_f32: f32,
    _col_f32_opt: Option<f32>,
    _col_f64: f64,
    _col_f64_opt: Option<f64>,
}

fn create_mandatory_column_types_dataframe(height: usize) -> DataFrame {
    let columns = [
        ("_col_bool", ColumnType(DataType::Boolean, false)),
        ("_col_i32", ColumnType(DataType::Int32, false)),
        ("_col_u32", ColumnType(DataType::UInt32, false)),
        ("_col_i64", ColumnType(DataType::Int64, false)),
        ("_col_u64", ColumnType(DataType::UInt64, false)),
        ("_col_f32", ColumnType(DataType::Float32, false)),
        ("_col_f64", ColumnType(DataType::Float64, false)),
        ("_col_str", ColumnType(DataType::String, false)),
        (
            "_col_cat",
            ColumnType(DataType::Categorical(None, CategoricalOrdering::Physical), false),
        ),
    ]
    .into_iter()
    .collect::<HashMap<&str, ColumnType>>();

    create_dataframe(columns.clone(), height)
}

#[derive(Debug, FromDataFrameRow)]
struct MandatoryTypesRow<'a> {
    _col_bool: bool,
    _col_i32: i32,
    _col_u32: u32,
    _col_i64: i64,
    _col_u64: u64,
    _col_f32: f32,
    _col_f64: f64,
    _col_str: &'a str,
    _col_cat: &'a str,
}

fn create_optional_column_types_dataframe(height: usize) -> DataFrame {
    let columns = [
        ("_col_bool_opt", ColumnType(DataType::Boolean, true)),
        ("_col_i32_opt", ColumnType(DataType::Int32, true)),
        ("_col_u32_opt", ColumnType(DataType::UInt32, true)),
        ("_col_i64_opt", ColumnType(DataType::Int64, true)),
        ("_col_u64_opt", ColumnType(DataType::UInt64, true)),
        ("_col_f32_opt", ColumnType(DataType::Float32, true)),
        ("_col_f64_opt", ColumnType(DataType::Float64, true)),
        ("_col_str_opt", ColumnType(DataType::String, true)),
        (
            "_col_cat_opt",
            ColumnType(DataType::Categorical(None, CategoricalOrdering::Physical), true),
        ),
    ]
    .into_iter()
    .collect::<HashMap<&str, ColumnType>>();

    create_dataframe(columns.clone(), height)
}

#[derive(Debug, FromDataFrameRow)]
struct OptionalTypesRow<'a> {
    _col_bool_opt: Option<bool>,
    _col_i32_opt: Option<i32>,
    _col_u32_opt: Option<u32>,
    _col_i64_opt: Option<i64>,
    _col_u64_opt: Option<u64>,
    _col_f32_opt: Option<f32>,
    _col_f64_opt: Option<f64>,
    _col_str_opt: Option<&'a str>,
    _col_cat_opt: Option<&'a str>,
}

fn get_dataframe_heights_to_benchmark() -> Vec<usize> {
    vec![1usize, 10usize, 100usize, 1_000usize, 10_000usize]
}

fn add_all_column_types_group(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = c.benchmark_group("all_types");
    group.plot_config(plot_config);
    group.measurement_time(Duration::from_secs(8));

    for height in get_dataframe_heights_to_benchmark() {
        let dataframe = create_all_column_types_dataframe(height);

        group.bench_with_input(BenchmarkId::new(".rows_iter()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_with_polars_rows_iter::<AllTypesRow>(df).unwrap();
            })
        });
        group.bench_with_input(BenchmarkId::new("izip!()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_all_types_with_zipped_column_iterators(df).unwrap();
            })
        });
        group.bench_with_input(BenchmarkId::new(".get_row()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_with_polars_get_row(df).unwrap();
            })
        });
    }

    group.finish();
}

fn add_primitive_column_types_group(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = c.benchmark_group("primitive_types");
    group.plot_config(plot_config);
    group.measurement_time(Duration::from_secs(10));

    for height in get_dataframe_heights_to_benchmark() {
        let dataframe = create_primitive_column_types_dataframe(height);

        group.bench_with_input(BenchmarkId::new(".rows_iter()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_with_polars_rows_iter::<PrimitiveTypesRow>(black_box(df)).unwrap();
            })
        });
        group.bench_with_input(BenchmarkId::new("izip!()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_primitive_types_with_zipped_column_iterators(black_box(df)).unwrap();
            })
        });
        group.bench_with_input(BenchmarkId::new(".get_row()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_with_polars_get_row(black_box(df)).unwrap();
            })
        });
    }

    group.finish();
}

fn add_mandatory_column_types_group(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = c.benchmark_group("mandatory_types");
    group.plot_config(plot_config);
    group.measurement_time(Duration::from_secs(10));

    for height in get_dataframe_heights_to_benchmark() {
        let dataframe = create_mandatory_column_types_dataframe(height);

        group.bench_with_input(BenchmarkId::new(".rows_iter()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_with_polars_rows_iter::<MandatoryTypesRow>(black_box(df)).unwrap();
            })
        });
        group.bench_with_input(BenchmarkId::new("izip!()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_mandatory_types_with_zipped_column_iterators(black_box(df)).unwrap();
            })
        });
        group.bench_with_input(BenchmarkId::new(".get_row()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_with_polars_get_row(black_box(df)).unwrap();
            })
        });
    }

    group.finish();
}

fn add_optional_column_types_group(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = c.benchmark_group("optional_types");
    group.plot_config(plot_config);
    group.measurement_time(Duration::from_secs(10));

    for height in get_dataframe_heights_to_benchmark() {
        let dataframe = create_optional_column_types_dataframe(height);

        group.bench_with_input(BenchmarkId::new(".rows_iter()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_with_polars_rows_iter::<OptionalTypesRow>(df).unwrap();
            })
        });
        group.bench_with_input(BenchmarkId::new("izip!()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_optional_types_with_zipped_column_iterators(df).unwrap();
            })
        });
        group.bench_with_input(BenchmarkId::new(".get_row()", height), &dataframe, |b, df| {
            b.iter(|| {
                iterate_with_polars_get_row(df).unwrap();
            })
        });
    }

    group.finish();
}

pub fn iteration_compare(c: &mut Criterion) {
    add_all_column_types_group(c);
    add_primitive_column_types_group(c);
    add_mandatory_column_types_group(c);
    add_optional_column_types_group(c);
}

fn iterate_with_polars_rows_iter<'a, T>(df: &'a DataFrame) -> PolarsResult<()>
where
    T: FromDataFrameRow<'a>,
{
    let iter = df.rows_iter::<T>()?;

    for row in iter {
        let row = row?;
        black_box(row);
    }

    Ok(())
}

fn iterate_with_polars_get_row(df: &DataFrame) -> PolarsResult<()> {
    for idx in 0..df.height() {
        let row = df.get_row(idx)?;
        black_box(row);
    }

    Ok(())
}

fn iterate_all_types_with_zipped_column_iterators(df: &DataFrame) -> PolarsResult<()> {
    let col_bool_iter = df.column("_col_bool")?.bool()?.into_iter();
    let col_bool_opt_iter = df.column("_col_bool_opt")?.bool()?.into_iter();
    let col_i32_iter = df.column("_col_i32")?.i32()?.into_iter();
    let col_i32_opt_iter = df.column("_col_i32_opt")?.i32()?.into_iter();
    let col_u32_iter = df.column("_col_u32")?.u32()?.into_iter();
    let col_u32_opt_iter = df.column("_col_u32_opt")?.u32()?.into_iter();
    let col_i64_iter = df.column("_col_i64")?.i64()?.into_iter();
    let col_i64_opt_iter = df.column("_col_i64_opt")?.i64()?.into_iter();
    let col_u64_iter = df.column("_col_u64")?.u64()?.into_iter();
    let col_u64_opt_iter = df.column("_col_u64_opt")?.u64()?.into_iter();
    let col_f32_iter = df.column("_col_f32")?.f32()?.into_iter();
    let col_f32_opt_iter = df.column("_col_f32_opt")?.f32()?.into_iter();
    let col_f64_iter = df.column("_col_f64")?.f64()?.into_iter();
    let col_f64_opt_iter = df.column("_col_f64_opt")?.f64()?.into_iter();
    let col_str_iter = df.column("_col_str")?.str()?.into_iter();
    let col_str_opt_iter = df.column("_col_str_opt")?.str()?.into_iter();
    let col_cat_iter = df.column("_col_cat")?.categorical()?.iter_str();
    let col_cat_opt_iter = df.column("_col_cat_opt")?.categorical()?.iter_str();

    let row_iter = izip!(
        col_bool_iter,
        col_bool_opt_iter,
        col_i32_iter,
        col_i32_opt_iter,
        col_u32_iter,
        col_u32_opt_iter,
        col_i64_iter,
        col_i64_opt_iter,
        col_u64_iter,
        col_u64_opt_iter,
        col_f32_iter,
        col_f32_opt_iter,
        col_f64_iter,
        col_f64_opt_iter,
        col_str_iter,
        col_str_opt_iter,
        col_cat_iter,
        col_cat_opt_iter,
    );

    for (
        col_bool_val,
        col_bool_opt_val,
        col_i32_val,
        col_i32_opt_val,
        col_u32_val,
        col_u32_opt_val,
        col_i64_val,
        col_i64_opt_val,
        col_u64_val,
        col_u64_opt_val,
        col_f32_val,
        col_f32_opt_val,
        col_f64_val,
        col_f64_opt_val,
        col_str_val,
        col_str_opt_val,
        col_cat_val,
        col_cat_opt_val,
    ) in row_iter
    {
        let col_bool_val: bool = col_bool_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_i32_val: i32 = col_i32_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_u32_val: u32 = col_u32_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_i64_val: i64 = col_i64_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_u64_val: u64 = col_u64_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_f32_val: f32 = col_f32_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_f64_val: f64 = col_f64_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_str_val: &str = col_str_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_cat_val: &str = col_cat_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;

        black_box(col_bool_val);
        black_box(col_bool_opt_val);
        black_box(col_i32_val);
        black_box(col_i32_opt_val);
        black_box(col_u32_val);
        black_box(col_u32_opt_val);
        black_box(col_i64_val);
        black_box(col_i64_opt_val);
        black_box(col_u64_val);
        black_box(col_u64_opt_val);
        black_box(col_f32_val);
        black_box(col_f32_opt_val);
        black_box(col_f64_val);
        black_box(col_f64_opt_val);
        black_box(col_str_val);
        black_box(col_str_opt_val);
        black_box(col_cat_val);
        black_box(col_cat_opt_val);
    }

    Ok(())
}

fn iterate_primitive_types_with_zipped_column_iterators(df: &DataFrame) -> PolarsResult<()> {
    let col_bool_iter = df.column("_col_bool")?.bool()?.into_iter();
    let col_bool_opt_iter = df.column("_col_bool_opt")?.bool()?.into_iter();
    let col_i32_iter = df.column("_col_i32")?.i32()?.into_iter();
    let col_i32_opt_iter = df.column("_col_i32_opt")?.i32()?.into_iter();
    let col_u32_iter = df.column("_col_u32")?.u32()?.into_iter();
    let col_u32_opt_iter = df.column("_col_u32_opt")?.u32()?.into_iter();
    let col_i64_iter = df.column("_col_i64")?.i64()?.into_iter();
    let col_i64_opt_iter = df.column("_col_i64_opt")?.i64()?.into_iter();
    let col_u64_iter = df.column("_col_u64")?.u64()?.into_iter();
    let col_u64_opt_iter = df.column("_col_u64_opt")?.u64()?.into_iter();
    let col_f32_iter = df.column("_col_f32")?.f32()?.into_iter();
    let col_f32_opt_iter = df.column("_col_f32_opt")?.f32()?.into_iter();
    let col_f64_iter = df.column("_col_f64")?.f64()?.into_iter();
    let col_f64_opt_iter = df.column("_col_f64_opt")?.f64()?.into_iter();

    let row_iter = izip!(
        col_bool_iter,
        col_bool_opt_iter,
        col_i32_iter,
        col_i32_opt_iter,
        col_u32_iter,
        col_u32_opt_iter,
        col_i64_iter,
        col_i64_opt_iter,
        col_u64_iter,
        col_u64_opt_iter,
        col_f32_iter,
        col_f32_opt_iter,
        col_f64_iter,
        col_f64_opt_iter,
    );

    for (
        col_bool_val,
        col_bool_opt_val,
        col_i32_val,
        col_i32_opt_val,
        col_u32_val,
        col_u32_opt_val,
        col_i64_val,
        col_i64_opt_val,
        col_u64_val,
        col_u64_opt_val,
        col_f32_val,
        col_f32_opt_val,
        col_f64_val,
        col_f64_opt_val,
    ) in row_iter
    {
        let col_bool_val: bool = col_bool_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_i32_val: i32 = col_i32_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_u32_val: u32 = col_u32_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_i64_val: i64 = col_i64_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_u64_val: u64 = col_u64_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_f32_val: f32 = col_f32_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_f64_val: f64 = col_f64_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;

        black_box(col_bool_val);
        black_box(col_bool_opt_val);
        black_box(col_i32_val);
        black_box(col_i32_opt_val);
        black_box(col_u32_val);
        black_box(col_u32_opt_val);
        black_box(col_i64_val);
        black_box(col_i64_opt_val);
        black_box(col_u64_val);
        black_box(col_u64_opt_val);
        black_box(col_f32_val);
        black_box(col_f32_opt_val);
        black_box(col_f64_val);
        black_box(col_f64_opt_val);
    }

    Ok(())
}

fn iterate_mandatory_types_with_zipped_column_iterators(df: &DataFrame) -> PolarsResult<()> {
    let col_bool_iter = df.column("_col_bool")?.bool()?.into_iter();
    let col_i32_iter = df.column("_col_i32")?.i32()?.into_iter();
    let col_u32_iter = df.column("_col_u32")?.u32()?.into_iter();
    let col_i64_iter = df.column("_col_i64")?.i64()?.into_iter();
    let col_u64_iter = df.column("_col_u64")?.u64()?.into_iter();
    let col_f32_iter = df.column("_col_f32")?.f32()?.into_iter();
    let col_f64_iter = df.column("_col_f64")?.f64()?.into_iter();
    let col_str_iter = df.column("_col_str")?.str()?.into_iter();
    let col_cat_iter = df.column("_col_cat")?.categorical()?.iter_str();

    let row_iter = izip!(
        col_bool_iter,
        col_i32_iter,
        col_u32_iter,
        col_i64_iter,
        col_u64_iter,
        col_f32_iter,
        col_f64_iter,
        col_str_iter,
        col_cat_iter,
    );

    for (
        col_bool_val,
        col_i32_val,
        col_u32_val,
        col_i64_val,
        col_u64_val,
        col_f32_val,
        col_f64_val,
        col_str_val,
        col_cat_val,
    ) in row_iter
    {
        let col_bool_val: bool = col_bool_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_i32_val: i32 = col_i32_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_u32_val: u32 = col_u32_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_i64_val: i64 = col_i64_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_u64_val: u64 = col_u64_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_f32_val: f32 = col_f32_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_f64_val: f64 = col_f64_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_str_val: &str = col_str_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;
        let col_cat_val: &str = col_cat_val.ok_or_else(|| polars_err!(SchemaMismatch: "Unexpected null value"))?;

        black_box(col_bool_val);
        black_box(col_i32_val);
        black_box(col_u32_val);
        black_box(col_i64_val);
        black_box(col_u64_val);
        black_box(col_f32_val);
        black_box(col_f64_val);
        black_box(col_str_val);
        black_box(col_cat_val);
    }

    Ok(())
}

fn iterate_optional_types_with_zipped_column_iterators(df: &DataFrame) -> PolarsResult<()> {
    let col_bool_opt_iter = df.column("_col_bool_opt")?.bool()?.into_iter();
    let col_i32_opt_iter = df.column("_col_i32_opt")?.i32()?.into_iter();
    let col_u32_opt_iter = df.column("_col_u32_opt")?.u32()?.into_iter();
    let col_i64_opt_iter = df.column("_col_i64_opt")?.i64()?.into_iter();
    let col_u64_opt_iter = df.column("_col_u64_opt")?.u64()?.into_iter();
    let col_f32_opt_iter = df.column("_col_f32_opt")?.f32()?.into_iter();
    let col_f64_opt_iter = df.column("_col_f64_opt")?.f64()?.into_iter();
    let col_str_opt_iter = df.column("_col_str_opt")?.str()?.into_iter();
    let col_cat_opt_iter = df.column("_col_cat_opt")?.categorical()?.iter_str();

    let row_iter = izip!(
        col_bool_opt_iter,
        col_i32_opt_iter,
        col_u32_opt_iter,
        col_i64_opt_iter,
        col_u64_opt_iter,
        col_f32_opt_iter,
        col_f64_opt_iter,
        col_str_opt_iter,
        col_cat_opt_iter,
    );

    for (
        col_bool_opt_val,
        col_i32_opt_val,
        col_u32_opt_val,
        col_i64_opt_val,
        col_u64_opt_val,
        col_f32_opt_val,
        col_f64_opt_val,
        col_str_opt_val,
        col_cat_opt_val,
    ) in row_iter
    {
        black_box(col_bool_opt_val);
        black_box(col_i32_opt_val);
        black_box(col_u32_opt_val);
        black_box(col_i64_opt_val);
        black_box(col_u64_opt_val);
        black_box(col_f32_opt_val);
        black_box(col_f64_opt_val);
        black_box(col_str_opt_val);
        black_box(col_cat_opt_val);
    }

    Ok(())
}

criterion_group!(benches, iteration_compare);
criterion_main!(benches);

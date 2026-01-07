#![allow(dead_code)]

use polars::prelude::*;
use polars_rows_iter::*;

fn create_dataframe() -> PolarsResult<DataFrame> {
    df!(
        "col_a" => [1i32, 2, 3, 4, 5],
        "col_b" => ["a", "b", "c", "d", "e"],
        "col_c" => [Some("A"), Some("B"), None, None, Some("E")],
    )
}

fn main() -> PolarsResult<()> {
    let df = create_dataframe()?;

    let col_c = format!("col_{}", 'c');

    let iter = df_rows_iter!(
        &df,
        "col_a" => i32,
        "col_b" => &str,
        col_c => Option<&str>
    )
    .unwrap();

    for row in iter {
        let (col_a, col_b, col_c) = row.unwrap();
        println!("col_a: {col_a}, col_b: {col_b}, col_c: {col_c:?}");
    }

    Ok(())
}

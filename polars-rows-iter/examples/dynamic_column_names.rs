#![allow(dead_code)]

use polars::prelude::*;
use polars_rows_iter::*;

const ID: &str = "id";

#[derive(Debug, FromDataFrameRow)]
struct MyRow<'a> {
    #[column(ID)]
    id: i32,
    value_b: &'a str,
    value_c: String,
    optional: Option<f64>,
}

fn create_dataframe() -> PolarsResult<DataFrame> {
    df!(
        "id" => [1i32, 2, 3, 4, 5],
        "col_b" => ["a", "b", "c", "d", "e"],
        "col_c" => ["A", "B", "C", "D", "E"],
        "col_d" => [Some(1.0f64), None, None, Some(2.0), Some(3.0)]
    )
}

fn run() -> PolarsResult<()> {
    let df = create_dataframe()?;

    let value_b_column_name = "col_b".to_string();
    let value_c_column_name = "col_c";

    let rows_iter = df.rows_iter_with_columns::<MyRow>(|columns| {
        columns
            .value_b(&value_b_column_name)
            .value_c(value_c_column_name)
            .optional("col_d")
    })?;

    for row in rows_iter {
        let row = row?;
        println!("{row:?}");
    }

    Ok(())
}

fn main() {
    run().unwrap()
}

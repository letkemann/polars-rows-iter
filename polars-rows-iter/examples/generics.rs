#![allow(dead_code)]

use polars::prelude::*;
use polars_rows_iter::*;

mod column_names {
    pub const COLUMN_D: &str = "col_d";
}

#[derive(Debug, FromDataFrameRow)]
struct MyRow<A, B, C, D> {
    col_a: A,
    #[column("col_b")]
    b: B,
    col_c: C,
    #[column(column_names::COLUMN_D)]
    d: D,
}

fn create_dataframe() -> PolarsResult<DataFrame> {
    df!(
        "col_a" => [1i32, 2, 3, 4, 5],
        "col_b" => ["a", "b", "c", "d", "e"],
        "col_c" => ["A", "B", "C", "D", "E"],
        "col_d" => [Some(1.0f64), None, None, Some(2.0), Some(3.0)]
    )
}

fn run() -> PolarsResult<()> {
    let df = create_dataframe()?;

    let rows_iter = df.rows_iter::<MyRow<i32, &str, String, Option<f64>>>()?;

    for row in rows_iter {
        let row = row?;

        println!("{row:?}");
    }

    Ok(())
}

fn main() {
    run().unwrap()
}

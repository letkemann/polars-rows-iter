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

fn run() -> PolarsResult<()> {
    let df = create_dataframe()?;

    let values_a = df.scalar_iter("col_a")?.collect::<PolarsResult<Vec<i32>>>()?;
    let values_b = df.scalar_iter("col_b")?.collect::<PolarsResult<Vec<&str>>>()?;
    let values_c = df
        .scalar_iter("col_c")?
        .collect::<PolarsResult<Vec<Option<String>>>>()?;

    println!("{values_a:?}");
    println!("{values_b:?}");
    println!("{values_c:?}");

    Ok(())
}

fn main() {
    run().unwrap()
}

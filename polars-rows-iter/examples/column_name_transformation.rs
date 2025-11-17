#![allow(dead_code)]

use polars::prelude::*;
use polars_rows_iter::*;

// Struct with transformations applied:
// - prefix("api_"): adds "api_" before field name
// - convert_case(Pascal): converts snake_case to PascalCase
// - postfix("_field"): adds "_field" after field name
//
// So field "user_name" becomes column "api_UserName_field"
#[derive(Debug, FromDataFrameRow)]
#[from_dataframe(prefix("api_"), convert_case(Pascal), postfix("_field"))]
struct ApiRow<'a> {
    user_id: i32,
    user_name: &'a str,
    is_active: bool,
}

fn create_dataframe() -> PolarsResult<DataFrame> {
    df!(
        "api_UserId_field" => [1i32, 2, 3],
        "api_UserName_field" => ["Alice", "Bob", "Charlie"],
        "api_IsActive_field" => [true, false, true]
    )
}

fn run() -> PolarsResult<()> {
    let df = create_dataframe()?;

    println!("DataFrame columns: {:?}", df.get_column_names());
    println!("\nIterating rows with column name transformations:");

    for row in df.rows_iter::<ApiRow>()? {
        println!("  {:?}", row?);
    }

    Ok(())
}

fn main() {
    run().unwrap()
}

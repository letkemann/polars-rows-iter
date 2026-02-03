# polars-rows-iter

Library for simple and convenient row iteration of polars dataframes

This crate provides two main approaches for iterating over DataFrame rows:

- **Struct-based iteration** using `#[derive(FromDataFrameRow)]` - best for complex rows with many columns
- **Tuple-based iteration** using the `df_rows_iter!` macro - best for quick, simple iterations

### Example with tuple-based iteration:

For simple use cases where you don't need a dedicated struct, use the `df_rows_iter!` macro to iterate over rows as tuples:

```rust
use polars::prelude::*;
use polars_rows_iter::*;

fn main() {
    let df = df!(
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [25i32, 30, 35],
        "score" => [Some(95.5f64), None, Some(87.0)]
    ).unwrap();

    let score_col = format!("sco{}", "re"); // dynamic column name

    let iter = df_rows_iter!(
        &df,
        "name" => &str,
        "age" => i32,
        score_col => Option<f64>
    ).unwrap();

    for row in iter {
        let (name, age, score) = row.unwrap();
        println!("{name}: age {age}, score {score:?}");
    }
}
```

The macro supports tuples of up to 10 elements. Each element is specified as `column_name => Type`.
Column names can be string literals or any expression that implements `AsRef<str>`.

### Example with static column names:

```rust
use polars::prelude::*;
use polars_rows_iter::*;

fn main() {
    #[derive(Debug, FromDataFrameRow)]
    #[derive(PartialEq)] // for assert_eq
    struct MyRow<'a>
    {
        #[column("col_a")]
        a: i32,
        // the column name defaults to the field name if no explicit name given
        col_b: &'a str,
        col_c: String,
        #[column("col_d")]
        optional: Option<f64>
    }

    let df = df!(
            "col_a" => [1i32, 2, 3, 4, 5],
            "col_b" => ["a", "b", "c", "d", "e"],
            "col_c" => ["A", "B", "C", "D", "E"],
            "col_d" => [Some(1.0f64), None, None, Some(2.0), Some(3.0)]
        ).unwrap();

    let rows_iter = df.rows_iter::<MyRow>().unwrap(); // ready to use row iterator
    // collect to vector for assert_eq
    let rows_vec = rows_iter.collect::<PolarsResult<Vec<MyRow>>>().unwrap();

    assert_eq!(
        rows_vec,
        [
            MyRow { a: 1, col_b: "a", col_c: "A".to_string(), optional: Some(1.0) },
            MyRow { a: 2, col_b: "b", col_c: "B".to_string(), optional: None },
            MyRow { a: 3, col_b: "c", col_c: "C".to_string(), optional: None },
            MyRow { a: 4, col_b: "d", col_c: "D".to_string(), optional: Some(2.0) },
            MyRow { a: 5, col_b: "e", col_c: "E".to_string(), optional: Some(3.0) },
        ]
    );
}

```

### Example with dynamic column names:

```rust
use polars::prelude::*;
use polars_rows_iter::*;

const ID: &str = "id";

#[derive(Debug, FromDataFrameRow)]
#[derive(PartialEq)] // for assert_eq
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

fn main() {
    let df = create_dataframe().unwrap();

    let value_b_column_name = "col_b".to_string();
    let value_c_column_name = "col_c";

    let rows_iter = df
        .rows_iter_with_columns::<MyRow>(|columns| {
            columns
                .value_b(&value_b_column_name)
                .value_c(value_c_column_name)
                .optional("col_d")
        })
        .unwrap(); // ready to use row iterator

    // collect to vector for assert_eq
    let rows_vec = rows_iter.collect::<PolarsResult<Vec<MyRow>>>().unwrap();

    assert_eq!(
        rows_vec,
        [
            MyRow { id: 1, value_b: "a", value_c: "A".to_string(), optional: Some(1.0) },
            MyRow { id: 2, value_b: "b", value_c: "B".to_string(), optional: None },
            MyRow { id: 3, value_b: "c", value_c: "C".to_string(), optional: None },
            MyRow { id: 4, value_b: "d", value_c: "D".to_string(), optional: Some(2.0) },
            MyRow { id: 5, value_b: "e", value_c: "E".to_string(), optional: Some(3.0) },
        ]
    );
}
```

### Example with column name transformations:

You can use attributes to automatically transform field names to match your DataFrame's column naming conventions:

```rust
use polars::prelude::*;
use polars_rows_iter::*;

fn main() {
    // Transformations are applied in order:
    // 1. prefix("api_"): adds "api_" before field name
    // 2. convert_case(Pascal): converts snake_case to PascalCase
    // 3. postfix("_field"): adds "_field" after field name
    //
    // So field "user_name" becomes column "api_UserName_field"
    #[derive(Debug, FromDataFrameRow)]
    #[from_dataframe(prefix("api_"), convert_case(Pascal), postfix("_field"))]
    struct ApiRow<'a> {
        user_id: i32,
        user_name: &'a str,
        is_active: bool,
    }

    let df = df!(
        "api_UserId_field" => [1i32, 2, 3],
        "api_UserName_field" => ["Alice", "Bob", "Charlie"],
        "api_IsActive_field" => [true, false, true]
    ).unwrap();

    for row in df.rows_iter::<ApiRow>().unwrap() {
        println!("{:?}", row.unwrap());
    }
}
```

Supported case conversions (from the [`convert_case`](https://docs.rs/convert_case/latest/convert_case/enum.Case.html) crate): `Upper`, `Lower`, `Title`, `Toggle`, `Camel`, `Pascal`, `UpperCamel`, `Snake`, `UpperSnake`, `ScreamingSnake`, `Kebab`, `Cobol`, `UpperKebab`, `Train`, `Flat`, `UpperFlat`, `Alternating`

### Version Compatibility

| polars-rows-iter | Polars |
| ---------------- | ------ |
| v0.13.0          | 0.52.0 |
| v0.12.1          | 0.52.0 |
| v0.12.0          | 0.52.0 |
| v0.11.1          | 0.52.0 |
| v0.11.0          | 0.51.0 |
| v0.10.0          | 0.51.0 |
| v0.9.8           | 0.51.0 |
| v0.9.7           | 0.50.0 |
| v0.9.6           | 0.49.1 |
| v0.9.5           | 0.48.1 |
| v0.9.4           | 0.48.0 |
| v0.9.3           | 0.47.1 |
| v0.9.2           | 0.46.0 |
| v0.9.1           | 0.45.1 |
| v0.8.0           | 0.45.1 |
| v0.7.0           | 0.45.0 |
| v0.6.0           | 0.44.2 |
| v0.5.0           | 0.44.2 |
| v0.4.0           | 0.44.2 |
| v0.3.0           | 0.44.2 |
| v0.2.0           | 0.44.2 |

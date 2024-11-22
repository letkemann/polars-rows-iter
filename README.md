# polars-rows-iter

Library for simple and convenient row iteration of polars dataframes

### Example:

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

### Performance



### Todos
- Document how to extend for custom types
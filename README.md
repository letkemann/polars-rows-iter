# polars-rows-iter

This is a **early state** library for simple and convenient row iteration of polars dataframes.<br>
Currently are only primitive and string types supported.

Todos:

- Unit-Tests
- Documentation
- DType: Date support
- DType: Binary Support

### Example:

```rust
use polars::prelude::*;
use polars_rows_iter::*;

#[derive(Debug, FromDataFrameRow)]
#[derive(PartialEq)] // for assert_eq
struct MyRow<'a>
{
    #[column("col_a")]
    a: i32,
    // the column name defaults to the field name if no explicit name given
    col_b: &'a str
}

let df = df!(
        "col_a" => [1i32, 2, 3, 4, 5],
        "col_b" => ["a", "b", "c", "d", "e"]
    ).unwrap();

let rows_iter = df.rows_iter::<MyRow>().unwrap();
// unwrap rows and collect to vector for assert_eq
let rows_vec = rows_iter.map(|row|row.unwrap()).collect::<Vec<MyRow>>();

assert_eq!(
    rows_vec,
    [
        MyRow { a: 1, col_b: "a" },
        MyRow { a: 2, col_b: "b" },
        MyRow { a: 3, col_b: "c" },
        MyRow { a: 4, col_b: "d" },
        MyRow { a: 5, col_b: "e" },
    ]
);
```

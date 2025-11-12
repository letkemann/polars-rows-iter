use polars::df;
use polars_rows_iter::*;

const COLUMN: &str = "ColZ";

#[derive(Debug, FromDataFrameRow)]
#[from_dataframe(convert_case(Snake), prefix("test"), postfix("test2"))]
struct DataRow0<'a, T1, T2> {
    #[column("col x")]
    _x: T1,
    #[column("col-y")]
    _y: T2,
    #[column(COLUMN)]
    _z: &'a str,
}

// #[derive(Debug, FromDataFrameRow)]
// struct DataRow1<T> {
//     #[column("col_x")]
//     _x: T,
// }

#[test]
fn sandbox() {
    let df = df!(
        "col_x" => [1i32, 2, 3, 4],
        "col_y" => [10f64, 20.0, 30.0, 40.0],
        "col_z" => ["a", "b", "c", "d"]
    )
    .unwrap();

    println!("{df:?}");

    let iter = df.rows_iter::<DataRow0<i32, f64>>().unwrap();

    for row in iter {
        let row = row.unwrap();
        println!("{row:?}");
    }
}

// struct Row<'a> {
//     col_a: i32,
//     col_b: &'a str,
// }

// struct RowRowsIterator<'a> {
//     col_a_iter: Box<dyn Iterator<Item = PolarsResult<i32>> + 'a>,
//     col_b_iter: Box<dyn Iterator<Item = PolarsResult<&'a str>> + 'a>,
// }

// impl<'a> RowRowsIterator<'a> {
//     #[inline]
//     fn create(col_a: PolarsResult<i32>, col_b: Result<&'a str, PolarsError>) -> PolarsResult<Row<'a>> {
//         Ok(Row {
//             col_a: col_a?,
//             col_b: col_b?,
//         })
//     }
// }

// impl<'a> Iterator for RowRowsIterator<'a> {
//     type Item = PolarsResult<Row<'a>>;

//     fn next(&mut self) -> Option<Self::Item> {
//         let col_a_value = self.col_a_iter.next()?;
//         let col_b_value = self.col_b_iter.next()?;

//         Some(Self::create(col_a_value, col_b_value))
//     }
// }

// struct RowBuilder {}

// impl ColumnNameBuilder for RowBuilder {
//     fn build(self) -> std::collections::HashMap<&'static str, String> {
//         todo!()
//     }
// }

// impl<'a> FromDataFrameRow<'a> for Row<'a> {
//     fn from_dataframe(
//         dataframe: &'a DataFrame,
//         columns: HashMap<&'static str, String>,
//     ) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
//     where
//         Self: Sized,
//     {
//         let col_a_iter = IterFromColumn::create_iter(dataframe.column("col_a")?)?;
//         let col_b_iter = IterFromColumn::create_iter(dataframe.column("col_b")?)?;

//         let iter: Box<dyn Iterator<Item = PolarsResult<Self>> + 'a> =
//             Box::new(RowRowsIterator::<'a> { col_a_iter, col_b_iter });

//         Ok(iter)
//     }

//     type Builder = RowBuilder;

//     fn create_builder() -> Self::Builder {
//         todo!()
//     }
// }

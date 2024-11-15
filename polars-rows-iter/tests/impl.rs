use polars::prelude::*;

pub trait FromDataFrameRow<'a> {
    fn from_dataframe(dataframe: &'a DataFrame) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
    where
        Self: Sized;
}

pub trait IterFromColumn<'a> {
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
    where
        Self: Sized;
}

pub trait DataframeRowsIterExt<'a> {
    fn rows_iter<T>(&'a self) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>;
}

impl<'a> DataframeRowsIterExt<'a> for DataFrame {
    fn rows_iter<T>(&'a self) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<T>> + 'a>>
    where
        T: FromDataFrameRow<'a>,
    {
        Ok(T::from_dataframe(self)?)
    }
}

fn mandatory_value<T>(polars_value: Option<T>) -> PolarsResult<T> {
    match polars_value {
        Some(value) => Ok(value),
        None => Err(polars_err!(SchemaMismatch: "Found unexpected None/null value in columns with mandatory values!")),
    }
}

impl<'a> IterFromColumn<'a> for &'a str {
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>> {
        let iter = Box::new(column.str()?.into_iter().map(mandatory_value));
        Ok(iter)
    }
}

impl<'a> IterFromColumn<'a> for i32 {
    fn create_iter(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>> {
        let iter = Box::new(column.i32()?.into_iter().map(mandatory_value));
        Ok(iter)
    }
}

#[derive(Debug)]
struct Row<'a> {
    col_a: i32,
    col_b: &'a str,
}

#[test]
fn test() {
    let df = df!(
        "col_a" => [1i32, 2, 3, 4],
        "col_b" => ["a", "b", "c", "d"]
    )
    .unwrap();

    println!("{df:?}");

    let iter = df.rows_iter::<Row>().unwrap();

    for row in iter {
        let row = row.unwrap();
        println!("{row:?}");
    }
}

struct RowRowsIterator<'a> {
    col_a_iter: Box<dyn Iterator<Item = PolarsResult<i32>> + 'a>,
    col_b_iter: Box<dyn Iterator<Item = PolarsResult<&'a str>> + 'a>,
}

impl<'a> RowRowsIterator<'a> {
    #[inline]
    fn create(col_a: PolarsResult<i32>, col_b: Result<&'a str, PolarsError>) -> PolarsResult<Row<'a>> {
        Ok(Row {
            col_a: col_a?,
            col_b: col_b?,
        })
    }
}

impl<'a> Iterator for RowRowsIterator<'a> {
    type Item = PolarsResult<Row<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let col_a_value = self.col_a_iter.next()?;
        let col_b_value = self.col_b_iter.next()?;

        Some(Self::create(col_a_value, col_b_value))
    }
}

impl<'a> FromDataFrameRow<'a> for Row<'a> {
    fn from_dataframe(dataframe: &'a DataFrame) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
    where
        Self: Sized,
    {
        let col_a_iter = IterFromColumn::create_iter(dataframe.column("col_a")?)?;
        let col_b_iter = IterFromColumn::create_iter(dataframe.column("col_b")?)?;

        let iter: Box<dyn Iterator<Item = PolarsResult<Self>> + 'a> =
            Box::new(RowRowsIterator::<'a> { col_a_iter, col_b_iter });

        Ok(iter)
    }
}

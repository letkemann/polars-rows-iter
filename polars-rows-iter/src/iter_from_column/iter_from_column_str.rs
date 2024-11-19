use super::*;
use iter_from_column::IterFromColumn;
use polars::prelude::*;

impl<'a> IterFromColumn<'a, &'a str> for &'a str {
    fn create_iter(
        dataframe: &'a DataFrame,
        column_name: &'a str,
    ) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
        create_iter(dataframe, column_name)
    }
}

impl<'a> IterFromColumn<'a, &'a str> for Option<&'a str> {
    fn create_iter(
        dataframe: &'a DataFrame,
        column_name: &'a str,
    ) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
        create_iter(dataframe, column_name)
    }
}

struct PolarsIteratorStrWrapper<'a> {
    inner: Box<dyn PolarsIterator<Item = Option<&'a str>> + 'a>,
}

impl<'a> Iterator for PolarsIteratorStrWrapper<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

fn create_str_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    let inner = column.str()?.into_iter();
    Ok(Box::new(PolarsIteratorStrWrapper { inner }))
}

#[cfg(feature = "dtype-categorical")]
struct CategoricalIteratorWrapper<'a> {
    inner: CatIter<'a>,
}

#[cfg(feature = "dtype-categorical")]
impl<'a> Iterator for CategoricalIteratorWrapper<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[cfg(feature = "dtype-categorical")]
fn create_cat_iter<'a>(column: &'a Column) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    let inner = column.categorical()?.iter_str();
    Ok(Box::new(CategoricalIteratorWrapper { inner }))
}

fn create_iter<'a>(
    dataframe: &'a DataFrame,
    column_name: &'a str,
) -> PolarsResult<Box<dyn Iterator<Item = Option<&'a str>> + 'a>> {
    let column = dataframe.column(column_name)?;

    let iter = match column.dtype() {
        DataType::String => create_str_iter(column)?,
        #[cfg(feature = "dtype-categorical")]
        DataType::Categorical(_, _) => create_cat_iter(column)?,
        dtype => {
            return Err(polars_err!(SchemaMismatch: "Cannot get &str from column '{column_name}' with dtype : {dtype}"))
        }
    };

    Ok(iter)
}

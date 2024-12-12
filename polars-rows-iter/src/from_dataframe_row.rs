use std::collections::HashMap;

use polars::prelude::*;

pub trait ColumnNameBuilder<'a> {
    fn build(self) -> HashMap<&'a str, &'a str>;
}

pub trait FromDataFrameRow<'a> {
    type Builder: ColumnNameBuilder<'a>;
    fn from_dataframe(
        dataframe: &'a DataFrame,
        columns: HashMap<&str, &str>,
    ) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
    where
        Self: Sized;

    fn create_builder() -> Self::Builder;
}

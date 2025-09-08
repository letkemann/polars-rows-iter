use std::collections::HashMap;

use polars::prelude::*;

pub trait ColumnNameBuilder {
    fn build(self) -> HashMap<&'static str, String>;
}

pub trait FromDataFrameRow<'a> {
    type Builder: ColumnNameBuilder;
    fn from_dataframe(
        dataframe: &'a DataFrame,
        columns: HashMap<&'static str, String>,
    ) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
    where
        Self: Sized;

    fn create_builder() -> Self::Builder;
}

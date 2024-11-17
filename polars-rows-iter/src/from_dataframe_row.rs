use polars::prelude::*;

pub trait FromDataFrameRow<'a> {
    fn from_dataframe(dataframe: &'a DataFrame) -> PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + 'a>>
    where
        Self: Sized;
}

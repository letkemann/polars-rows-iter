use crate::{iter_from_column_raw::IterFromColumnRaw, IterFromColumn};
use polars::{error::PolarsResult, prelude::Column};

pub trait ColumnExt {
    fn scalar_iter<'a, T>(&'a self) -> PolarsResult<impl Iterator<Item = PolarsResult<T>> + 'a>
    where
        T: IterFromColumn<'a> + 'a,
        Option<T::RawInner>: IterFromColumnRaw<'a> + 'a;

    fn raw_iter<'a, T>(&'a self) -> PolarsResult<impl Iterator<Item = T> + 'a>
    where
        T: IterFromColumnRaw<'a> + 'a;
}

impl ColumnExt for Column {
    fn scalar_iter<'a, T>(&'a self) -> PolarsResult<impl Iterator<Item = PolarsResult<T>> + 'a>
    where
        T: IterFromColumn<'a> + 'a,
        Option<T::RawInner>: IterFromColumnRaw<'a> + 'a,
    {
        let iter = <Option<<T as IterFromColumn<'a>>::RawInner> as IterFromColumnRaw<'a>>::create_iter(self)?;
        let iter = iter.map(|v| <T as IterFromColumn<'a>>::get_value(v, self.name(), self.dtype()));

        Ok(iter)
    }

    fn raw_iter<'a, T>(&'a self) -> PolarsResult<impl Iterator<Item = T> + 'a>
    where
        T: IterFromColumnRaw<'a> + 'a,
    {
        let iter = <T as IterFromColumnRaw<'a>>::create_iter(self)?;

        Ok(iter)
    }
}

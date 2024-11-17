use polars::prelude::*;

pub fn mandatory_value<T>(polars_value: Option<T>) -> PolarsResult<T> {
    match polars_value {
        Some(value) => Ok(value),
        None => Err(polars_err!(SchemaMismatch: "Found unexpected None/null value in columns with mandatory values!")),
    }
}

pub fn optional_value<T>(polars_value: Option<T>) -> PolarsResult<Option<T>> {
    Ok(polars_value)
}

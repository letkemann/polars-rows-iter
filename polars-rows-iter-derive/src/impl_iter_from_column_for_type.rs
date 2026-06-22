use quote::quote;

pub fn create_impl_for(ident: syn::Ident) -> proc_macro::TokenStream {
    quote! {
        impl<'a> ::polars_rows_iter::IterFromColumnRaw<'a> for Option<#ident> {
            fn create_iter(column: &'a polars::prelude::Column) -> polars::prelude::PolarsResult<impl Iterator<Item = Self> + 'a> {
                Ok(column.#ident()?.iter())
            }
        }

        impl<'a> ::polars_rows_iter::IterFromColumn<'a> for #ident {
            type RawInner = #ident;

            #[inline]
            fn get_value(polars_value: Option<#ident>, column_name: &str, dtype: &polars::prelude::DataType) -> polars::prelude::PolarsResult<Self>
            where
                Self: Sized,
            {
                polars_value.ok_or_else(|| <#ident as ::polars_rows_iter::IterFromColumn<'a>>::unexpected_null_value_error(column_name))
            }
        }

        impl<'a> ::polars_rows_iter::IterFromColumn<'a> for Option<#ident> {
            type RawInner = #ident;

            #[inline]
            fn get_value(polars_value: Option<#ident>, _column_name: &str, dtype: &polars::prelude::DataType) -> polars::prelude::PolarsResult<Self>
            where
                Self: Sized,
            {
                Ok(polars_value)
            }
        }
    }
    .into()
}

use quote::quote;

pub fn create_impl_for(ident: syn::Ident) -> proc_macro::TokenStream {
    quote! {
        impl<'a> IterFromColumn<'a> for #ident {
            type RawInner = #ident;
            fn create_iter(column: &'a polars::prelude::Column) -> polars::prelude::PolarsResult<Box<dyn Iterator<Item = Option<#ident>> + 'a>> {
                Ok(Box::new(column.#ident()?.iter()))
            }

            #[inline]
            fn get_value(polars_value: Option<#ident>, column_name: &str, dtype: &polars::prelude::DataType) -> polars::prelude::PolarsResult<Self>
            where
                Self: Sized,
            {
                polars_value.ok_or_else(|| <#ident as IterFromColumn<'a>>::unexpected_null_value_error(column_name))
            }
        }

        impl<'a> IterFromColumn<'a> for Option<#ident> {
            type RawInner = #ident;
            fn create_iter(column: &'a polars::prelude::Column) -> polars::prelude::PolarsResult<Box<dyn Iterator<Item = Option<#ident>> + 'a>> {
                let iter = Box::new(column.#ident()?.iter());
                Ok(iter)
            }

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

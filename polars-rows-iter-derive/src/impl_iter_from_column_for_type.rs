use quote::quote;

pub fn create_impl_for(ident: syn::Ident) -> proc_macro::TokenStream {
    quote! {
        impl<'a> IterFromColumn<'a> for #ident {
            fn create_iter(
                dataframe: &'a polars::prelude::DataFrame,
                column_name: &'a str,
            ) -> polars::prelude::PolarsResult<Box<dyn Iterator<Item = polars::prelude::PolarsResult<Self>> + 'a>> {
                Ok(Box::new(dataframe.column(column_name)?.#ident()?.into_iter().map(mandatory_value)))
            }
        }

        impl<'a> IterFromColumn<'a> for Option<#ident> {
            fn create_iter(
                dataframe: &'a polars::prelude::DataFrame,
                column_name: &'a str,
            ) -> polars::prelude::PolarsResult<Box<dyn Iterator<Item = polars::prelude::PolarsResult<Self>> + 'a>> {
                let iter = Box::new(dataframe.column(column_name)?.#ident()?.into_iter().map(optional_value));
                Ok(iter)
            }
        }
    }
    .into()
}
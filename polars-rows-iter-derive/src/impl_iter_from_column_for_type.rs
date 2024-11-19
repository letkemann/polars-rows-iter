use quote::quote;

pub fn create_impl_for(ident: syn::Ident) -> proc_macro::TokenStream {
    quote! {
        impl<'a> IterFromColumn<'a, #ident> for #ident {
            fn create_iter(
                dataframe: &'a polars::prelude::DataFrame,
                column_name: &'a str,
            ) -> polars::prelude::PolarsResult<Box<dyn Iterator<Item = Option<#ident>> + 'a>> {
                Ok(Box::new(dataframe.column(column_name)?.#ident()?.into_iter()))
            }
        }

        impl<'a> IterFromColumn<'a, #ident> for Option<#ident> {
            fn create_iter(
                dataframe: &'a polars::prelude::DataFrame,
                column_name: &'a str,
            ) -> polars::prelude::PolarsResult<Box<dyn Iterator<Item = Option<#ident>> + 'a>> {
                let iter = Box::new(dataframe.column(column_name)?.#ident()?.into_iter());
                Ok(iter)
            }
        }
    }
    .into()
}

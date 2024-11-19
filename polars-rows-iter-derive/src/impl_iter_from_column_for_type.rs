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

            #[inline]
            fn get_value(polars_value: Option<#ident>, column_name: &'a str) -> polars::prelude::PolarsResult<Self>
            where
                Self: Sized,
            {
                polars_value.ok_or_else(|| polars::prelude::polars_err!(SchemaMismatch: "Found unexpected None/null value in column {column_name} with mandatory values!"))
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

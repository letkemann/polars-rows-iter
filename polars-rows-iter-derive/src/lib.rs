//! # polars-rows-iter-derive
//!
//! This crate exports the macros required by the main polars-rows-iter crate.

mod from_dataframe_row_derive;
mod impl_iter_from_column_for_type;

#[proc_macro_derive(FromDataFrameRow, attributes(column))]
pub fn from_dataframe_row_derive_macro(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: syn::DeriveInput = syn::parse2(input.into()).unwrap();
    from_dataframe_row_derive::from_dataframe_row_derive_impl(ast).into()
}

#[proc_macro]
pub fn iter_from_column_for_type(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ident: syn::Ident = syn::parse(input).unwrap();
    impl_iter_from_column_for_type::create_impl_for(ident)
}

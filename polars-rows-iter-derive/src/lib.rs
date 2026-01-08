//! # polars-rows-iter-derive
//!
//! This crate exports the macros required by the main polars-rows-iter crate.

mod context;
mod field_info;
mod from_dataframe_attribute;
mod from_dataframe_row_derive;
mod impl_iter_from_column_for_type;
mod tuple_iterators;

#[proc_macro_derive(FromDataFrameRow, attributes(column, from_dataframe))]
pub fn from_dataframe_row_derive_macro(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: syn::DeriveInput = match syn::parse2(input.into()) {
        Ok(ast) => ast,
        Err(e) => return e.into_compile_error().into(),
    };
    from_dataframe_row_derive::from_dataframe_row_derive_impl(ast)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro]
pub fn iter_from_column_for_type(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ident: syn::Ident = match syn::parse(input) {
        Ok(ident) => ident,
        Err(e) => return e.into_compile_error().into(),
    };
    impl_iter_from_column_for_type::create_impl_for(ident)
}

#[proc_macro]
pub fn impl_tuple_rows_iter(input_stream: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input_stream as tuple_iterators::Input);
    tuple_iterators::create_iterators_and_macro(input.count).into()
}

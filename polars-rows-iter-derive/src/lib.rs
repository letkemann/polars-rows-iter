mod from_dataframe_row_derive;
mod impl_iter_from_column_for_type;

#[proc_macro_derive(FromDataFrameRow)]
pub fn from_dataframe_row_derive_macro(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();
    from_dataframe_row_derive::from_dataframe_row_derive_impl(ast)
}

#[proc_macro]
pub fn iter_from_column_for_type(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ident: syn::Ident = syn::parse(input).unwrap();
    impl_iter_from_column_for_type::create_impl_for(ident)
}

use crate::context::Context;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Expr, Ident, Type};

#[derive(Debug)]
pub struct FieldInfo {
    pub name: String,
    pub ident: Ident,
    pub dtype_ident: Ident,
    pub iter_ident: Ident,
    pub inner_ty: Type,
    pub is_optional: bool,
    pub column_name_expr: Expr,
}

impl FieldInfo {
    pub fn create_default_column_name(&self, ctx: &Context) -> TokenStream {
        let default_column_name_expr = &self.column_name_expr;

        match (
            ctx.attributes.prefix.as_ref(),
            ctx.attributes.convert_case.as_ref(),
            ctx.attributes.postfix.as_ref(),
        ) {
            (None, None, None) => quote! {
                let default_column_name = #default_column_name_expr;
            },

            (None, None, Some(postfix)) => quote! {
                let default_column_name = format!(
                    "{column_name}{postfix}",
                    column_name = #default_column_name_expr,
                    postfix = #postfix,
                );
                let default_column_name = default_column_name.as_str();
            },

            (None, Some(case), None) => quote! {
                let default_column_name = format!(
                    "{column_name}",
                    column_name = #default_column_name_expr.to_case(Case::#case),
                );
                let default_column_name = default_column_name.as_str();
            },

            (None, Some(case), Some(postfix)) => quote! {
                let default_column_name = format!(
                    "{column_name}{postfix}",
                    column_name = #default_column_name_expr.to_case(Case::#case),
                    postfix = #postfix,
                );
                let default_column_name = default_column_name.as_str();
            },

            (Some(prefix), None, None) => quote! {
                let default_column_name = format!(
                    "{prefix}{column_name}",
                    prefix = #prefix,
                    column_name = #default_column_name_expr,
                );
                let default_column_name = default_column_name.as_str();
            },

            (Some(prefix), None, Some(postfix)) => quote! {
                let default_column_name = format!(
                    "{prefix}{column_name}{postfix}",
                    prefix = #prefix,
                    column_name = #default_column_name_expr,
                    postfix = #postfix,
                );
                let default_column_name = default_column_name.as_str();
            },

            (Some(prefix), Some(case), None) => quote! {
                let default_column_name = format!(
                    "{prefix}{column_name}",
                    prefix = #prefix,
                    column_name = #default_column_name_expr.to_case(Case::#case),
                );
                let default_column_name = default_column_name.as_str();
            },

            (Some(prefix), Some(case), Some(postfix)) => quote! {
                let default_column_name = format!(
                    "{prefix}{column_name}{postfix}",
                    prefix = #prefix,
                    column_name = #default_column_name_expr.to_case(Case::#case),
                    postfix = #postfix,
                );
                let default_column_name = default_column_name.as_str();
            },
        }
    }
}

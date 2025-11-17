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
        let prefix = ctx.attributes.prefix.as_ref();
        let convert_case = ctx.attributes.convert_case.as_ref();
        let postfix = ctx.attributes.postfix.as_ref();

        // If no transformations are needed, use the expression directly
        if prefix.is_none() && convert_case.is_none() && postfix.is_none() {
            return quote! {
                let default_column_name = #default_column_name_expr;
            };
        }

        // Build format string parts and corresponding arguments
        let mut format_parts = Vec::new();
        let mut format_args = Vec::new();

        // Add prefix if present
        if let Some(prefix_expr) = prefix {
            format_parts.push("{prefix}");
            format_args.push(quote! { prefix = #prefix_expr });
        }

        // Add column name (with optional case conversion)
        format_parts.push("{column_name}");
        let column_expr = if let Some(case) = convert_case {
            quote! { #default_column_name_expr.to_case(Case::#case) }
        } else {
            quote! { #default_column_name_expr }
        };
        format_args.push(quote! { column_name = #column_expr });

        // Add postfix if present
        if let Some(postfix_expr) = postfix {
            format_parts.push("{postfix}");
            format_args.push(quote! { postfix = #postfix_expr });
        }

        // Combine format parts into a single format string
        let format_str = format_parts.join("");

        quote! {
            let default_column_name = format!(
                #format_str,
                #(#format_args),*
            );
            let default_column_name = default_column_name.as_str();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::from_dataframe_attribute::FromDataFrameAttribute;
    use syn::parse_quote;

    fn create_test_field_info(column_name: &str) -> FieldInfo {
        create_test_field_info_with_expr(parse_quote!(#column_name))
    }

    fn create_test_field_info_with_expr(column_name_expr: syn::Expr) -> FieldInfo {
        FieldInfo {
            name: "test_field".to_string(),
            ident: parse_quote!(test_field),
            dtype_ident: parse_quote!(test_dtype),
            iter_ident: parse_quote!(test_iter),
            inner_ty: parse_quote!(String),
            is_optional: false,
            column_name_expr,
        }
    }

    fn create_test_context(
        prefix: Option<syn::Expr>,
        convert_case: Option<syn::Expr>,
        postfix: Option<syn::Expr>,
    ) -> Context {
        Context {
            struct_ident: parse_quote!(TestStruct),
            builder_struct_ident: parse_quote!(TestStructBuilder),
            iter_struct_ident: parse_quote!(TestStructIterator),
            fields_list: vec![],
            has_lifetime: false,
            type_generics: vec![],
            attributes: FromDataFrameAttribute {
                convert_case,
                prefix,
                postfix,
            },
        }
    }

    #[test]
    fn test_no_transformation() {
        let field = create_test_field_info("my_column");
        let ctx = create_test_context(None, None, None);

        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        // Should directly assign the column name without any transformation
        assert!(result_str.contains("let default_column_name = \"my_column\""));
        assert!(!result_str.contains("format !"));
    }

    #[test]
    fn test_prefix_only() {
        let field = create_test_field_info("column");
        let ctx = create_test_context(Some(parse_quote!("prefix_")), None, None);

        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        // quote! adds spaces, so "format!" becomes "format !"
        assert!(result_str.contains("format !"));
        assert!(result_str.contains("\"{prefix}{column_name}\""));
        assert!(result_str.contains("prefix = \"prefix_\""));
        assert!(result_str.contains("column_name = \"column\""));
        assert!(result_str.contains("as_str ()"));
    }

    #[test]
    fn test_postfix_only() {
        let field = create_test_field_info("column");
        let ctx = create_test_context(None, None, Some(parse_quote!("_suffix")));

        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        assert!(result_str.contains("format !"));
        assert!(result_str.contains("\"{column_name}{postfix}\""));
        assert!(result_str.contains("postfix = \"_suffix\""));
        assert!(result_str.contains("column_name = \"column\""));
        assert!(result_str.contains("as_str ()"));
    }

    #[test]
    fn test_convert_case_only() {
        let field = create_test_field_info("myColumn");
        let ctx = create_test_context(None, Some(parse_quote!(Snake)), None);

        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        assert!(result_str.contains("format !"));
        assert!(result_str.contains("\"{column_name}\""));
        assert!(result_str.contains("to_case (Case :: Snake)"));
        assert!(result_str.contains("as_str ()"));
    }

    #[test]
    fn test_prefix_and_postfix() {
        let field = create_test_field_info("column");
        let ctx = create_test_context(Some(parse_quote!("pre_")), None, Some(parse_quote!("_post")));

        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        assert!(result_str.contains("format !"));
        assert!(result_str.contains("\"{prefix}{column_name}{postfix}\""));
        assert!(result_str.contains("prefix = \"pre_\""));
        assert!(result_str.contains("postfix = \"_post\""));
        assert!(result_str.contains("column_name = \"column\""));
        assert!(result_str.contains("as_str ()"));
    }

    #[test]
    fn test_prefix_and_convert_case() {
        let field = create_test_field_info("myColumn");
        let ctx = create_test_context(Some(parse_quote!("tbl_")), Some(parse_quote!(Snake)), None);

        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        assert!(result_str.contains("format !"));
        assert!(result_str.contains("\"{prefix}{column_name}\""));
        assert!(result_str.contains("prefix = \"tbl_\""));
        assert!(result_str.contains("to_case (Case :: Snake)"));
        assert!(result_str.contains("as_str ()"));
    }

    #[test]
    fn test_postfix_and_convert_case() {
        let field = create_test_field_info("myColumn");
        let ctx = create_test_context(None, Some(parse_quote!(Snake)), Some(parse_quote!("_col")));

        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        assert!(result_str.contains("format !"));
        assert!(result_str.contains("\"{column_name}{postfix}\""));
        assert!(result_str.contains("postfix = \"_col\""));
        assert!(result_str.contains("to_case (Case :: Snake)"));
        assert!(result_str.contains("as_str ()"));
    }

    #[test]
    fn test_all_transformations() {
        let field = create_test_field_info("myColumn");
        let ctx = create_test_context(
            Some(parse_quote!("db_")),
            Some(parse_quote!(Snake)),
            Some(parse_quote!("_field")),
        );

        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        assert!(result_str.contains("format !"));
        assert!(result_str.contains("\"{prefix}{column_name}{postfix}\""));
        assert!(result_str.contains("prefix = \"db_\""));
        assert!(result_str.contains("postfix = \"_field\""));
        assert!(result_str.contains("to_case (Case :: Snake)"));
        assert!(result_str.contains("as_str ()"));
    }

    #[test]
    fn test_with_expression_column_name() {
        let field = create_test_field_info_with_expr(parse_quote!(MY_CONST));
        let ctx = create_test_context(Some(parse_quote!("prefix_")), None, None);

        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        // Should handle constant references correctly
        assert!(result_str.contains("column_name = MY_CONST"));
    }

    #[test]
    fn test_output_always_creates_str_reference() {
        let field = create_test_field_info("column");

        // Test with transformations that require format!
        let ctx = create_test_context(Some(parse_quote!("pre_")), None, None);
        let result = field.create_default_column_name(&ctx);
        let result_str = result.to_string();

        // When using format!, should convert to &str
        assert!(result_str.contains("as_str ()"));

        // Test without transformations
        let ctx_no_transform = create_test_context(None, None, None);
        let result_no_transform = field.create_default_column_name(&ctx_no_transform);
        let result_no_transform_str = result_no_transform.to_string();

        // When no transformation, uses the expression directly (already &str)
        assert!(!result_no_transform_str.contains("as_str"));
    }
}

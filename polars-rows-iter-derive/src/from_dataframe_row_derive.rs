use crate::{context::Context, field_info::FieldInfo, from_dataframe_attribute::FromDataFrameAttribute};
use itertools::Itertools;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    punctuated::Punctuated, spanned::Spanned, DeriveInput, Expr, ExprLit, Field, GenericArgument, GenericParam,
    Generics, Ident, Lifetime, LifetimeParam, LitStr, PathArguments, Token, Type, TypeParam, TypeReference,
    WhereClause, WherePredicate,
};

const ROW_ITERATOR_NAME: &str = "RowsIterator";

pub fn from_dataframe_row_derive_impl(ast: DeriveInput) -> syn::Result<TokenStream> {
    let attributes = FromDataFrameAttribute::from_ast(&ast)?;

    let struct_data = match &ast.data {
        syn::Data::Struct(data_struct) => data_struct,
        syn::Data::Enum(e) => {
            return Err(syn::Error::new_spanned(
                e.enum_token,
                "FromDataFrameRow cannot be derived for enums",
            ))
        }
        syn::Data::Union(u) => {
            return Err(syn::Error::new_spanned(
                u.union_token,
                "FromDataFrameRow cannot be derived for unions",
            ))
        }
    };

    let struct_ident = ast.ident.clone();
    let struct_ident_str = struct_ident.to_string();

    let iter_struct_ident = Ident::new(
        format!("{struct_ident_str}{ROW_ITERATOR_NAME}").as_str(),
        Span::call_site(),
    );

    let fields_list: Vec<FieldInfo> = struct_data
        .fields
        .iter()
        .cloned()
        .map(create_iterator_struct_field_info)
        .collect::<syn::Result<Vec<_>>>()?;

    let has_lifetime = match ast.generics.lifetimes().count() {
        0 => false,
        1 => true,
        _ => {
            let extra_lifetime = ast.generics.lifetimes().nth(1).unwrap();
            return Err(syn::Error::new(
                extra_lifetime.lifetime.span(),
                "FromDataFrameRow only supports a single lifetime parameter",
            ));
        }
    };

    let builder_struct_ident = Ident::new(&format!("{struct_ident}ColumnBuilder"), Span::call_site());

    let ctx = Context {
        struct_ident,
        builder_struct_ident,
        iter_struct_ident,
        fields_list,
        has_lifetime,
        type_generics: ast.generics.type_params().cloned().collect(),
        attributes,
    };

    let builder_struct = create_builder_struct(&ctx);
    let builder_struct_impl = create_builder_struct_impl(&ctx);
    let builder_struct_column_name_builder_impl = create_builder_struct_column_name_builder_impl(&ctx);
    let row_struct_impl = create_row_struct_impl(&ctx);
    let from_dataframe_row_trait_impl = create_from_dataframe_row_trait_impl(&ctx);
    let iterator_struct = create_iterator_struct(&ctx);
    let iterator_struct_impl = create_iterator_struct_impl(&ctx);
    let iterator_impl_for_iterator_struct = create_iterator_impl_for_iterator_struct(&ctx);

    let stream: TokenStream = quote! {
        #builder_struct
        #builder_struct_impl
        #builder_struct_column_name_builder_impl
        #row_struct_impl
        #from_dataframe_row_trait_impl
        #iterator_struct
        #iterator_struct_impl
        #iterator_impl_for_iterator_struct
    };

    Ok(stream)
}

fn create_lifetime_param(name: &str) -> LifetimeParam {
    LifetimeParam {
        attrs: vec![],
        lifetime: Lifetime {
            apostrophe: Span::call_site(),
            ident: Ident::new(name, Span::call_site()),
        },
        colon_token: None,
        bounds: Punctuated::new(),
    }
}

fn create_impl_generics(ctx: &Context, lifetime: &LifetimeParam) -> Generics {
    let generics = ctx
        .type_generics
        .iter()
        .map(|p| GenericParam::Type(p.clone()))
        .chain(std::iter::once(GenericParam::Lifetime(lifetime.clone())));

    Generics {
        lt_token: Some(Token![<](Span::call_site())),
        params: Punctuated::from_iter(generics),
        gt_token: Some(Token![>](Span::call_site())),
        where_clause: None,
    }
}

fn create_struct_generics(ctx: &Context, lifetime: Option<&LifetimeParam>) -> Generics {
    let generics = lifetime
        .map(|ltp| GenericParam::Lifetime(ltp.clone()))
        .into_iter()
        .chain(ctx.type_generics.iter().map(|tp| GenericParam::Type(tp.clone())))
        .collect_vec();

    Generics {
        lt_token: Some(Token![<](Span::call_site())),
        params: Punctuated::from_iter(generics),
        gt_token: Some(Token![>](Span::call_site())),
        where_clause: None,
    }
}

fn create_where_clause(ctx: &Context, ltp: &LifetimeParam, with_lifetime: bool) -> Option<WhereClause> {
    if ctx.type_generics.is_empty() {
        return None;
    }

    Some(WhereClause {
        where_token: Token![where](Span::call_site()),
        predicates: Punctuated::from_iter(
            ctx.type_generics
                .iter()
                .map(|tp| create_where_predicate(tp, ltp, with_lifetime)),
        ),
    })
}

fn create_where_predicate(tp: &TypeParam, ltp: &LifetimeParam, with_lifetime: bool) -> WherePredicate {
    let tp_ident = &tp.ident;

    let stream = match with_lifetime {
        true => quote! { #tp_ident : ::polars_rows_iter::IterFromColumn<#ltp> + #ltp },
        false => quote! { #tp_ident : ::polars_rows_iter::IterFromColumn<#ltp> },
    };

    syn::parse2(stream).expect("internal error: failed to parse generated where predicate")
}

fn create_builder_struct(ctx: &Context) -> proc_macro2::TokenStream {
    let builder_ident = &ctx.builder_struct_ident;

    quote! {
        pub struct #builder_ident {
            columns: std::collections::HashMap<&'static str, String>,
        }
    }
}

fn create_builder_struct_impl(ctx: &Context) -> proc_macro2::TokenStream {
    let builder_struct_ident = &ctx.builder_struct_ident;

    let field_column_func_list = ctx
        .fields_list
        .iter()
        .map(|f| {
            let field_ident = &f.ident;
            let field_name = f.ident.to_string();
            quote! {
                fn #field_ident<T:Into<String>>(&mut self, column_name: T) -> &mut Self
                {
                    let _ = self.columns.insert(#field_name, column_name.into());
                    self
                }
            }
        })
        .collect_vec();

    quote! {
        impl #builder_struct_ident {
            #(#field_column_func_list)*
        }
    }
}

fn create_builder_struct_column_name_builder_impl(ctx: &Context) -> proc_macro2::TokenStream {
    let builder_struct_ident = &ctx.builder_struct_ident;

    quote! {
        impl ::polars_rows_iter::ColumnNameBuilder for #builder_struct_ident {
            fn build(self) -> std::collections::HashMap<&'static str, String> {
                self.columns
            }
        }
    }
}

fn create_row_struct_impl(ctx: &Context) -> proc_macro2::TokenStream {
    let lifetime = create_lifetime_param("a");
    let struct_ident = &ctx.struct_ident;

    let impl_generics = create_impl_generics(ctx, &lifetime);
    let struct_generics = create_struct_generics(ctx, ctx.has_lifetime.then_some(&lifetime));
    let where_clause = create_where_clause(ctx, &lifetime, false);

    let column_name_expr_list = ctx.fields_list.iter().map(|f| &f.column_name_expr).collect_vec();

    let stream = quote::quote! {
        #[automatically_derived]
        impl #impl_generics #struct_ident #struct_generics #where_clause{
            fn get_column_names() -> Vec<&'static str>
                where
                    Self: Sized
            {
                vec![#(#column_name_expr_list,)*]
            }
        }
    };

    stream
}

fn create_from_dataframe_row_trait_impl(ctx: &Context) -> proc_macro2::TokenStream {
    let lifetime = create_lifetime_param("a");

    let lifetime_generics = Generics {
        lt_token: Some(Token![<](Span::call_site())),
        params: Punctuated::from_iter([GenericParam::Lifetime(lifetime.clone())]),
        gt_token: Some(Token![>](Span::call_site())),
        where_clause: None,
    };

    let impl_generics = create_impl_generics(ctx, &lifetime);

    let iter_create_list = ctx.fields_list.iter().map(|f| {
        let field_name = f.ident.to_string();
        let ident_iter = &f.iter_ident;
        let ident_dtype = &f.dtype_ident;

        let default_column_name = f.create_default_column_name(ctx);

        let field_type = remove_lifetime(f.inner_ty.clone());
        quote! {

            let column_name = columns.remove(#field_name);
            let column_name = column_name.as_deref();
            #default_column_name
            let column = dataframe.column(column_name.unwrap_or(default_column_name))?;
            let #ident_iter = Box::new(<#field_type as ::polars_rows_iter::IterFromColumn<#lifetime>>::create_iter(column)?);
            let #ident_dtype = column.dtype().clone();
        }
    });

    let struct_generics = create_struct_generics(ctx, ctx.has_lifetime.then_some(&lifetime));
    let where_clause = create_where_clause(ctx, &lifetime, true);

    let struct_ident = &ctx.struct_ident;
    let iter_struct_ident = &ctx.iter_struct_ident;
    let iter_ident_list = ctx.fields_list.iter().map(|f| {
        let ident_iter = &f.iter_ident;
        let ident_dtype = &f.dtype_ident;
        quote! { #ident_iter, #ident_dtype }
    });

    // let struct_ident = match ctx.has_lifetime {
    //     true => quote! { #struct_ident<#lifetime> },
    //     false => quote! { #struct_ident },
    // };

    let iter_struct_ident = match ctx.has_lifetime {
        true => quote! { #iter_struct_ident::<#lifetime> },
        false => quote! { #iter_struct_ident },
    };

    let builder_struct_ident = &ctx.builder_struct_ident;

    quote::quote! {
        #[automatically_derived]
        impl #impl_generics ::polars_rows_iter::FromDataFrameRow #lifetime_generics for #struct_ident #struct_generics #where_clause{
            type Builder = #builder_struct_ident;
            fn from_dataframe(
                dataframe: & #lifetime ::polars::prelude::DataFrame,
                mut columns: std::collections::HashMap<&'static str, String>
            ) -> ::polars::prelude::PolarsResult<Box<dyn Iterator<Item = ::polars::prelude::PolarsResult<Self>> + #lifetime>>
                where
                    Self: Sized
            {
                use ::polars_rows_iter::convert_case::{Case, Casing};

                #(#iter_create_list)*

                Ok(Box::new(#iter_struct_ident { #(#iter_ident_list,)* }))
            }

            fn create_builder() -> #builder_struct_ident {
                #builder_struct_ident{
                    columns: std::collections::HashMap::new()
                }
            }
        }
    }
}

#[derive(Debug, deluxe::ExtractAttributes)]
#[deluxe(attributes(column))]
struct ColumnFieldAttributes(#[deluxe(flatten)] Vec<syn::Expr>);

fn create_iterator_struct_field_info(mut field: Field) -> syn::Result<FieldInfo> {
    let ident = match &field.ident {
        Some(ident) => ident.clone(),
        None => {
            return Err(syn::Error::new_spanned(
                &field,
                "FromDataFrameRow requires named fields (tuple structs not supported)",
            ))
        }
    };
    let name = ident.to_string();

    let iter_ident = Ident::new(format!("{name}_iter").as_str(), Span::call_site());
    let dtype_ident = Ident::new(format!("{name}_dtype").as_str(), Span::call_site());
    let ty = field.ty.clone();

    let attrs: ColumnFieldAttributes = deluxe::extract_attributes(&mut field)?;

    let column_name_expr = match attrs.0.len() {
        0 => Expr::Lit(ExprLit {
            attrs: vec![],
            lit: syn::Lit::Str(LitStr::new(&name, field.span())),
        }),
        1 => attrs.0[0].clone(),
        _ => {
            return Err(syn::Error::new_spanned(
                &field,
                format!("field '{}' can have only one #[column(...)] attribute", name),
            ))
        }
    };

    let mut is_optional = false;
    let inner_ty = get_inner_type_from_options(ty.clone(), &mut is_optional);

    Ok(FieldInfo {
        name,
        ident,
        iter_ident,
        dtype_ident,
        inner_ty,
        is_optional,
        column_name_expr,
    })
}

fn try_get_inner_option_type(ty: &Type) -> Option<Type> {
    if let Type::Path(type_path) = ty {
        let segment = type_path.path.segments.first()?;
        if segment.ident == "Option" {
            if let PathArguments::AngleBracketed(gen) = &segment.arguments {
                let gen_args = gen.args.first()?;
                if let GenericArgument::Type(inner_type) = gen_args {
                    return Some(inner_type.clone());
                }
            }
        }
    }

    None
}

fn get_inner_type_from_options(ty: Type, is_optional: &mut bool) -> Type {
    if let Some(inner) = try_get_inner_option_type(&ty) {
        *is_optional = true;
        get_inner_type_from_options(inner, is_optional)
    } else {
        ty
    }
}

fn create_iterator_struct_field(field_info: &FieldInfo, lifetime: &LifetimeParam) -> proc_macro2::TokenStream {
    let ident = &field_info.iter_ident;
    let dtype_ident = &field_info.dtype_ident;
    let ty = coerce_lifetime(field_info.inner_ty.clone(), lifetime);
    quote! {
        #ident : Box<dyn Iterator<Item = Option<<#ty as ::polars_rows_iter::IterFromColumn<#lifetime>>::RawInner>> + #lifetime>,
        #dtype_ident: ::polars::prelude::DataType,
    }
}

fn create_iterator_struct(ctx: &Context) -> proc_macro2::TokenStream {
    let lifetime = create_lifetime_param("a");

    let fields = ctx
        .fields_list
        .iter()
        .map(|field_info| create_iterator_struct_field(field_info, &lifetime));

    let iter_struct_ident = &ctx.iter_struct_ident;

    let struct_generics = create_struct_generics(ctx, Some(&lifetime));
    let where_clause = create_where_clause(ctx, &lifetime, false);

    quote! {
        #[automatically_derived]
        struct #iter_struct_ident #struct_generics #where_clause {
            #(#fields)*
        }
    }
}

fn create_iterator_struct_impl(ctx: &Context) -> proc_macro2::TokenStream {
    let lifetime = create_lifetime_param("a");

    let fn_params = ctx.fields_list.iter().map(|field_info| {
        let ident = &field_info.ident;
        let field_type = coerce_lifetime(field_info.inner_ty.clone(), &lifetime);
        quote! { #ident: Option<<#field_type as ::polars_rows_iter::IterFromColumn<#lifetime>>::RawInner> }
    });

    let assignments = ctx.fields_list.iter().map(|field_info| {
        let ident = &field_info.ident;
        let ident_dtype = &field_info.dtype_ident;
        let field_type = coerce_lifetime(field_info.inner_ty.clone(), &lifetime);
        let column_name = &field_info.column_name_expr;

        match field_info.is_optional {
            true => quote! { #ident: <Option<#field_type> as ::polars_rows_iter::IterFromColumn<#lifetime>>::get_value(#ident, #column_name, &self.#ident_dtype)? },
            false => quote! { #ident: <#field_type as ::polars_rows_iter::IterFromColumn<#lifetime>>::get_value(#ident, #column_name, &self.#ident_dtype)? },
        }
    });

    let struct_ident = &ctx.struct_ident;
    let iter_struct_ident = &ctx.iter_struct_ident;

    let impl_generics = create_impl_generics(ctx, &lifetime);
    let struct_generics = create_struct_generics(ctx, Some(&lifetime));
    let type_generics = create_struct_generics(ctx, ctx.has_lifetime.then_some(&lifetime));
    let where_clause = create_where_clause(ctx, &lifetime, false);

    quote! {
        #[automatically_derived]
        impl #impl_generics #iter_struct_ident #struct_generics #where_clause {
            #[allow(clippy::too_many_arguments)]
            fn create(
                &self,
                #(#fn_params,)*
            ) -> ::polars::prelude::PolarsResult<#struct_ident #type_generics> {

                Ok(#struct_ident {
                    #(#assignments,)*
                })

            }
        }
    }
}

fn coerce_lifetime(ty: Type, lifetime: &LifetimeParam) -> Type {
    match ty {
        Type::Reference(type_reference) => Type::Reference(TypeReference {
            lifetime: type_reference.lifetime.map(|_| lifetime.lifetime.clone()),
            ..type_reference
        }),
        t => t,
    }
}

fn remove_lifetime(ty: Type) -> Type {
    match ty {
        Type::Reference(type_reference) => Type::Reference(TypeReference {
            lifetime: None,
            ..type_reference
        }),
        t => t,
    }
}

fn create_iterator_impl_for_iterator_struct(ctx: &Context) -> proc_macro2::TokenStream {
    let lifetime = create_lifetime_param("a");

    let fields: Vec<_> = ctx
        .fields_list
        .iter()
        .map(|f| {
            (
                Ident::new(
                    format!("{field_name}_value", field_name = f.name).as_str(),
                    Span::call_site(),
                ),
                &f.iter_ident,
            )
        })
        .collect();

    let next_value_list = fields.iter().map(|(value_ident, iter_ident)| {
        quote! { let #value_ident = self.#iter_ident.next()? }
    });

    let value_ident_list = fields.iter().map(|(value_ident, _)| value_ident);

    let struct_ident = &ctx.struct_ident;
    let iter_struct_ident = &ctx.iter_struct_ident;

    let impl_generics = create_impl_generics(ctx, &lifetime);
    let struct_generics = create_struct_generics(ctx, Some(&lifetime));
    let type_generics = create_struct_generics(ctx, ctx.has_lifetime.then_some(&lifetime));
    let where_clause = create_where_clause(ctx, &lifetime, false);

    quote! {
        impl #impl_generics Iterator for #iter_struct_ident #struct_generics #where_clause {
            type Item = ::polars::prelude::PolarsResult<#struct_ident #type_generics>;

            fn next(&mut self) -> Option<Self::Item> {
                #(#next_value_list;)*

                Some(self.create(#(#value_ident_list,)*))
            }
        }
    }
}

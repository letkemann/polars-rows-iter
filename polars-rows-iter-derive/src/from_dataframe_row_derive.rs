use std::iter::once;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{
    punctuated::Punctuated, DeriveInput, Field, GenericParam, Generics, Ident, Lifetime, LifetimeParam, Token, Type,
    TypeReference,
};

const ROW_ITERATOR_NAME: &'static str = "RowsIterator";

struct FieldInfo {
    pub name: String,
    pub ident: Ident,
    pub iter_ident: Ident,
    pub ty: Type,
}

struct Context {
    struct_ident: Ident,
    iter_struct_ident: Ident,
    fields_list: Vec<FieldInfo>,
    has_lifetime: bool,
}

pub fn from_dataframe_row_derive_impl(ast: DeriveInput) -> TokenStream {
    let struct_data = match &ast.data {
        syn::Data::Struct(data_struct) => data_struct,
        syn::Data::Enum(_) => panic!("Enums not supported"),
        syn::Data::Union(_) => panic!("Unions not supported"),
    };

    if ast.generics.type_params().count() > 0 {
        panic!("Generic types in row structs are currently not supported!")
    }

    let struct_ident = ast.ident.clone();
    let struct_ident_str = struct_ident.to_string();

    let iter_struct_ident = Ident::new(
        format!("{struct_ident_str}{ROW_ITERATOR_NAME}").as_str(),
        Span::call_site(),
    );

    let fields_list: Vec<_> = struct_data
        .fields
        .iter()
        .map(create_iterator_struct_field_info)
        .collect();

    let has_lifetime = match ast.generics.lifetimes().count() {
        0 => false,
        1 => true,
        _ => panic!("Multiple lifetimes in row structure are not supported!"),
    };

    let ctx = Context {
        struct_ident,
        iter_struct_ident,
        fields_list,
        has_lifetime,
    };

    let from_dataframe_row_trait_impl = create_from_dataframe_row_trait_impl(&ctx, &ast.generics);
    let iterator_struct = create_iterator_struct(&ctx);
    let iterator_struct_impl = create_iterator_struct_impl(&ctx);
    let iterator_impl_fo_iterator_struct = create_iterator_impl_for_iterator_struct(&ctx);

    let stream: TokenStream = quote! {
        #from_dataframe_row_trait_impl
        #iterator_struct
        #iterator_struct_impl
        #iterator_impl_fo_iterator_struct
    }
    .into();

    stream
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

fn create_impl_generics(struct_generics: &Generics, lifetime: &LifetimeParam) -> Generics {
    let generics = struct_generics
        .type_params()
        .map(|p| GenericParam::Type(p.clone()))
        .chain(once(GenericParam::Lifetime(lifetime.clone())));

    let generics = Generics {
        lt_token: Some(Token![<](Span::call_site())),
        params: Punctuated::from_iter(generics),
        gt_token: Some(Token![>](Span::call_site())),
        where_clause: None,
    };

    generics
}

fn create_from_dataframe_row_trait_impl(ctx: &Context, generics: &Generics) -> proc_macro2::TokenStream {
    let lifetime = create_lifetime_param("a");

    let lifetime_generics = Generics {
        lt_token: Some(Token![<](Span::call_site())),
        params: Punctuated::from_iter([GenericParam::Lifetime(lifetime.clone())]),
        gt_token: Some(Token![>](Span::call_site())),
        where_clause: None,
    };

    // let (lifetime, lifetime_generics) = get_or_create_lifetime_generics(generics);
    let impl_generics = create_impl_generics(generics, &lifetime);

    let iter_create_list = ctx.fields_list.iter().map(|f| {
        let iter_ident = &f.iter_ident;
        let column_name = f.name.as_str();
        quote! { let #iter_ident = IterFromColumn::create_iter(dataframe, #column_name)? }
    });

    let struct_ident = &ctx.struct_ident;
    let iter_struct_ident = &ctx.iter_struct_ident;
    let iter_ident_list = ctx.fields_list.iter().map(|f| &f.iter_ident);

    let struct_ident = match ctx.has_lifetime {
        true => quote! { #struct_ident<#lifetime> },
        false => quote! { #struct_ident },
    };

    let iter_struct_ident = match ctx.has_lifetime {
        true => quote! { #iter_struct_ident::<#lifetime> },
        false => quote! { #iter_struct_ident },
    };

    quote::quote! {
        #[automatically_derived]
        impl #impl_generics FromDataFrameRow #lifetime_generics for #struct_ident {
            fn from_dataframe(dataframe: & #lifetime DataFrame) ->  PolarsResult<Box<dyn Iterator<Item = PolarsResult<Self>> + #lifetime>>
                where
                    Self: Sized
            {
                #(#iter_create_list;)*

                Ok(Box::new(#iter_struct_ident { #(#iter_ident_list,)* }))
            }
        }
    }
}

fn create_iterator_struct_field_info(field: &Field) -> FieldInfo {
    let ident = field.ident.as_ref().expect("anonymous fields not supported").clone();
    let name = ident.to_string();

    let iter_ident = Ident::new(format!("{name}_iter").as_str(), Span::call_site());
    let ty = field.ty.clone();

    FieldInfo {
        name,
        ident,
        iter_ident,
        ty,
    }
}

fn create_iterator_struct_field(field_info: &FieldInfo, lifetime: &LifetimeParam) -> proc_macro2::TokenStream {
    let ident = &field_info.iter_ident;
    let ty = coerce_lifetime(field_info.ty.clone(), lifetime);
    quote! {
        #ident : Box<dyn Iterator<Item = PolarsResult<#ty>> + #lifetime>
    }
}

fn create_iterator_struct(ctx: &Context) -> proc_macro2::TokenStream {
    let lifetime = create_lifetime_param("a");

    let fields = ctx
        .fields_list
        .iter()
        .map(|field_info| create_iterator_struct_field(field_info, &lifetime));

    let iter_struct_ident = &ctx.iter_struct_ident;

    quote! {
        #[automatically_derived]
        struct #iter_struct_ident <#lifetime> {
            #(#fields, )*
        }
    }
}

fn create_iterator_struct_impl(ctx: &Context) -> proc_macro2::TokenStream {
    let lifetime = create_lifetime_param("a");

    let fn_params = ctx.fields_list.iter().map(|field_info| {
        let ident = &field_info.ident;
        let ty = coerce_lifetime(field_info.ty.clone(), &lifetime);
        quote! { #ident: PolarsResult<#ty> }
    });

    let assignments = ctx.fields_list.iter().map(|field_info| {
        let ident = &field_info.ident;
        quote! { #ident: #ident? }
    });

    let struct_ident = &ctx.struct_ident;
    let iter_struct_ident = &ctx.iter_struct_ident;

    let struct_ident_with_lifetime_if_nec = match ctx.has_lifetime {
        true => quote! { #struct_ident<#lifetime> },
        false => quote! { #struct_ident },
    };

    quote! {
        #[automatically_derived]
        impl<#lifetime> #iter_struct_ident <#lifetime> {
            fn create(
                #(#fn_params,)*
            ) -> PolarsResult<#struct_ident_with_lifetime_if_nec> {

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

    let struct_ident = match ctx.has_lifetime {
        true => quote! { #struct_ident<#lifetime> },
        false => quote! { #struct_ident },
    };

    quote! {
        impl<#lifetime> Iterator for #iter_struct_ident<#lifetime> {
            type Item = PolarsResult<#struct_ident>;

            fn next(&mut self) -> Option<Self::Item> {
                #(#next_value_list;)*

                Some(Self::create(#(#value_ident_list,)*))
            }
        }
    }
}

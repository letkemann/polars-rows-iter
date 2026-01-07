use itertools::Itertools;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Error, Ident, Lit, Token,
};

#[derive(Debug)]
pub struct Input {
    pub count: u8,
}

impl Parse for Input {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let count = if let Lit::Int(lit) = Lit::parse(input)? {
            let count = lit.base10_parse::<u8>()?;
            if count < 1 {
                return Err(Error::new(lit.span(), "'count' has to be at least one!"));
            }
            count
        } else {
            return Err(Error::new(
                input.span(),
                "Macro received single positive integer parameter!",
            ));
        };

        Ok(Input { count })
    }
}

struct Type {
    index: u8,
    type_gp: syn::GenericParam,
    type_gp_constraint: TokenStream,
    iter_gp: syn::GenericParam,
    iter_gp_constraint: TokenStream,
    iter_field_ident: syn::Ident,
    iter_field_decl: TokenStream,
    column_field_ident: syn::Ident,
    column_dtype_field_ident: syn::Ident,
    column_name_gp: syn::GenericParam,
    column_name_gp_constraint: TokenStream,
    column_name_arg_ident: syn::Ident,
    column_name_arg_decl: TokenStream,
}

impl Type {
    pub fn new(index: u8) -> Self {
        // generic types
        let type_gp = Self::create_generic_param("T", index);
        let type_gp_constraint = quote! { #type_gp: ::polars_rows_iter::IterFromColumn<'a> + 'a };

        let iter_gp = Self::create_generic_param("I", index);
        let iter_gp_constraint = quote! { #iter_gp : Iterator<Item = Option<#type_gp::RawInner>> + 'a };
        let iter_field_ident = Ident::new(&format!("iter_{index}"), Span::call_site());
        let iter_field_decl = quote! { #iter_field_ident: #iter_gp };

        let column_field_ident = Ident::new(&format!("column_{index}"), Span::call_site());
        let column_dtype_field_ident = Ident::new(&format!("dtype_{index}"), Span::call_site());

        // column name input types
        let column_name_gp = Self::create_generic_param("S", index);
        let column_name_gp_constraint = quote! { #column_name_gp: AsRef<str> + 'a };
        let column_name_arg_ident = Ident::new(&format!("column_name_{index}"), Span::call_site());
        let column_name_arg_decl = quote! { #column_name_arg_ident: #column_name_gp };

        Self {
            index,
            type_gp,
            type_gp_constraint,
            iter_gp,
            iter_gp_constraint,
            iter_field_ident,
            iter_field_decl,
            column_field_ident,
            column_dtype_field_ident,
            column_name_gp,
            column_name_gp_constraint,
            column_name_arg_ident,
            column_name_arg_decl,
        }
    }

    fn create_generic_param(prefix: &str, index: u8) -> syn::GenericParam {
        syn::GenericParam::Type(syn::TypeParam {
            attrs: vec![],
            ident: Ident::new(&format!("{prefix}{index}"), Span::call_site()),
            bounds: syn::punctuated::Punctuated::new(),
            colon_token: None,
            default: None,
            eq_token: None,
        })
    }
}

pub fn create_iterators_and_macro(max_count: u8) -> proc_macro2::TokenStream {
    let iterators = (1..=max_count).map(create_iterator);
    let macro_arms = (1..=max_count).map(create_macro_arm);

    quote! {
        #(#iterators)*

        #[macro_export]
        macro_rules! df_rows_iter {
            #(#macro_arms)*
        }
    }
}

fn create_macro_arm(count: u8) -> proc_macro2::TokenStream {
    // Pattern: $n0:expr => $t0:ty, $n1:expr => $t1:ty, ...
    let pattern_parts = (0..count).map(|i| {
        let name_ident = Ident::new(&format!("n{i}"), Span::call_site());
        let type_ident = Ident::new(&format!("t{i}"), Span::call_site());
        quote! { $#name_ident:expr => $#type_ident:ty }
    });

    // Function call generic params: $t0, _, $t1, _, ...
    let generic_args = (0..count).flat_map(|i| {
        let t_ident = Ident::new(&format!("t{i}"), Span::call_site());
        [quote! { $#t_ident }, quote! { _ }]
    });

    // Function call args: $n0, $n1, ...
    let call_args = (0..count).map(|i| {
        let n_ident = Ident::new(&format!("n{i}"), Span::call_site());
        quote! { $#n_ident }
    });

    let func_ident = Ident::new(&format!("create_tuple_rows_iter_{count}"), Span::call_site());

    quote! {
        ($df:expr, #(#pattern_parts),*) => {
            #func_ident::<#(#generic_args),*>($df, #(#call_args),*)
        };
    }
}

fn create_iterator(count: u8) -> proc_macro2::TokenStream {
    let iter_struct_ident = Ident::new(&format!("TupleRowsIter{}", count), Span::call_site());

    let types = (0..count).map(Type::new).collect_vec();

    let generic_params = types.iter().flat_map(|ty| [&ty.type_gp, &ty.iter_gp]).collect_vec();

    let gp_types: Punctuated<_, Token![,]> = Punctuated::from_iter(generic_params.iter());

    let iter_struct_type = quote! {#iter_struct_ident<'a, #gp_types>};

    let generic_constraints = types
        .iter()
        .flat_map(|ty| [&ty.type_gp_constraint, &ty.iter_gp_constraint])
        .collect_vec();

    let tuple_types = types.iter().map(|ty| &ty.type_gp).collect_vec();
    let tuple_type = quote! {(#(#tuple_types,)*)};

    let func_create_tuple = create_func_create_tuple(&types, &tuple_type);
    let func_next = create_func_next(&types);

    let create_tuple_rows_iter_ident = Ident::new(&format!("create_tuple_rows_iter_{}", count), Span::call_site());

    let struct_fields = types.iter().map(create_type_field_declaration).collect_vec();
    let struct_field_implementations = types.iter().map(create_type_field_implementations).collect_vec();

    let col_args = types.iter().map(|ty| &ty.column_name_arg_decl).collect_vec();

    let field_idents = types
        .iter()
        .flat_map(|ty| {
            [
                &ty.iter_field_ident,
                &ty.column_field_ident,
                &ty.column_dtype_field_ident,
            ]
        })
        .collect_vec();

    let phantom_ident = Ident::new("_phantom", Span::call_site());
    let phantom_generic_params = types.iter().map(|ty| {
        let type_gp = &ty.type_gp;
        quote! {&'a #type_gp }
    });

    let create_func_generic_constraints = types
        .iter()
        .flat_map(|ty| [&ty.type_gp_constraint, &ty.column_name_gp_constraint])
        .collect_vec();

    let create_func_generic_params = types
        .iter()
        .flat_map(|ty| [&ty.type_gp, &ty.column_name_gp])
        .collect_vec();

    quote! {
        struct #iter_struct_type
        where
            #(#generic_constraints,)*
        {
            #(#struct_fields,)*
            #phantom_ident: ::std::marker::PhantomData<(#(#phantom_generic_params,)*)>
        }

        impl<'a, #gp_types> #iter_struct_type
        where
            #(#generic_constraints,)*
        {
            #func_create_tuple
        }

        impl<'a, #gp_types> Iterator for #iter_struct_type
        where
            #(#generic_constraints,)*
        {
            type Item = ::polars::prelude::PolarsResult<#tuple_type>;

            #func_next
        }

        #[allow(clippy::too_many_arguments)]
        pub fn #create_tuple_rows_iter_ident<'a, #(#create_func_generic_params,)*>(
            df: &'a ::polars::prelude::DataFrame,
            #(#col_args,)*
        ) -> ::polars::prelude::PolarsResult<impl Iterator<Item = ::polars::prelude::PolarsResult<#tuple_type>> + use<'a, #(#create_func_generic_params,)*>>
        where
            #(#create_func_generic_constraints,)*
        {
            #(#struct_field_implementations)*

            Ok(#iter_struct_ident {
                #(#field_idents,)*
                #phantom_ident: ::std::marker::PhantomData{}
            })
        }

    }
}

fn create_type_field_declaration(ty: &Type) -> proc_macro2::TokenStream {
    let iter_decl = &ty.iter_field_decl;
    let col_ident = &ty.column_field_ident;
    let dtype_ident = &ty.column_dtype_field_ident;
    quote! {
        #iter_decl,
        #col_ident: &'a ::polars::prelude::Column,
        #dtype_ident: &'a ::polars::prelude::DataType
    }
}

fn create_type_field_implementations(ty: &Type) -> proc_macro2::TokenStream {
    let type_gp = &ty.type_gp;
    let iter_ident = &ty.iter_field_ident;
    let column_name_ident = &ty.column_name_arg_ident;
    let column_ident = &ty.column_field_ident;
    let column_dtype_ident = &ty.column_dtype_field_ident;

    quote! {
        let #column_ident = df.column(#column_name_ident.as_ref())?;
        let #column_dtype_ident = #column_ident.dtype();
        let #iter_ident = <#type_gp as ::polars_rows_iter::IterFromColumn>::create_iter(#column_ident)?;
    }
}

fn create_func_create_tuple(types: &[Type], tuple_type: &TokenStream) -> proc_macro2::TokenStream {
    let types_with_idents = types
        .iter()
        .map(|ty| (ty, Ident::new(&format!("v{}", ty.index), Span::call_site())))
        .collect_vec();

    let arg_idents = types_with_idents.iter().map(|(_, ident)| ident).collect_vec();
    let arg_decls = types_with_idents
        .iter()
        .map(|(ty, ident)| {
            let type_gp = &ty.type_gp;
            quote! { #ident: ::polars::prelude::PolarsResult<#type_gp> }
        })
        .collect_vec();

    quote! {
        #[allow(clippy::too_many_arguments)]
        fn create_tuple(#(#arg_decls,)*) -> ::polars::prelude::PolarsResult<#tuple_type> {
            Ok((#(#arg_idents?,)*))
        }
    }
}

fn create_func_next(types: &[Type]) -> proc_macro2::TokenStream {
    let create_tuple_value_names = types
        .iter()
        .map(|ty| {
            let type_gp = &ty.type_gp;
            let field_ident = &ty.iter_field_ident;
            let column_ident = &ty.column_field_ident;
            let column_dtype_ident = &ty.column_dtype_field_ident;

            quote! { <#type_gp as ::polars_rows_iter::IterFromColumn>::get_value(
                self.#field_ident.next()?,
                self.#column_ident.name().as_str(),
                self.#column_dtype_ident,
            )  }
        })
        .collect_vec();

    quote! {
        fn next(&mut self) -> Option<Self::Item> {
            Some(Self::create_tuple(#(#create_tuple_value_names,)*))
        }
    }
}

#[test]
fn test() {
    let stream = create_iterators_and_macro(3);

    println!("\n\nUnformatted stream:\n{stream}");

    let st = syn::parse2(stream).unwrap();

    let formatted = prettyplease::unparse(&st);

    println!("\n\nFormatted stream:\n{formatted}");
}

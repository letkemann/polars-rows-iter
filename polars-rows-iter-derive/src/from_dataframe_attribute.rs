#[derive(Debug)]
pub struct FromDataFrameAttribute {
    pub convert_case: Option<syn::Expr>,
    pub prefix: Option<syn::Expr>,
    pub postfix: Option<syn::Expr>,
}

impl FromDataFrameAttribute {
    pub fn from_ast(input: &syn::DeriveInput) -> Result<Self, syn::Error> {
        let mut convert_case = None;
        let mut prefix = None;
        let mut postfix = None;

        for attr in &input.attrs {
            if !attr.meta.path().is_ident("from_dataframe") {
                continue;
            }

            if let syn::Meta::List(meta) = &attr.meta {
                if meta.tokens.is_empty() {
                    continue;
                }
            }

            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("convert_case") {
                    let content;
                    syn::parenthesized!(content in meta.input);
                    convert_case = Some(content.parse()?)
                } else if meta.path.is_ident("prefix") {
                    let content;
                    syn::parenthesized!(content in meta.input);
                    prefix = Some(content.parse()?);
                } else if meta.path.is_ident("postfix") {
                    let content;
                    syn::parenthesized!(content in meta.input);
                    postfix = Some(content.parse()?);
                } else {
                    return Err(meta.error("Unsupported 'from_dataframe' property"));
                }
                Ok(())
            })?
        }

        Ok(Self {
            convert_case,
            prefix,
            postfix,
        })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn parse_test() {
        let input = quote::quote! {
            #[from_dataframe(convert_case(Snake), prefix("pre_"), postfix("_post"))]
            struct Test {}
        };

        let ast: syn::DeriveInput = syn::parse2(input).unwrap();

        let attr: crate::from_dataframe_attribute::FromDataFrameAttribute =
            super::FromDataFrameAttribute::from_ast(&ast).unwrap();

        println!("{attr:?}");
    }
}

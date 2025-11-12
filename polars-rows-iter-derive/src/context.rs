use crate::{field_info::FieldInfo, from_dataframe_attribute::FromDataFrameAttribute};
use syn::{Ident, TypeParam};

pub struct Context {
    pub struct_ident: Ident,
    pub builder_struct_ident: Ident,
    pub iter_struct_ident: Ident,
    pub fields_list: Vec<FieldInfo>,
    pub has_lifetime: bool,
    pub type_generics: Vec<TypeParam>,
    pub attributes: FromDataFrameAttribute,
}

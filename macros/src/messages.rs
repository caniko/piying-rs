use heck::ToUpperCamelCase;
use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote, quote_spanned};
use syn::{
    Attribute, Expr, Field, FnArg, GenericParam, Generics, Ident, ImplItem, ItemImpl, Meta,
    MetaNameValue, Pat, ReturnType, Signature, Token, Type, Visibility,
    parse::{Parse, ParseStream, Parser},
    parse_quote, parse_quote_spanned,
    punctuated::Punctuated,
    spanned::Spanned,
};

include!("messages/types.rs");
include!("messages/extract.rs");
include!("messages/tokens_parse.rs");

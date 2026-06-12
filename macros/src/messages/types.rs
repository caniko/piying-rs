pub struct Messages {
    item_impl: ItemImpl,
    ident: Ident,
    messages: Vec<Message>,
    errors: Option<syn::Error>,
}

#[derive(Clone)]
struct Message {
    vis: Visibility,
    sig: Signature,
    ident: Ident,
    fields: Punctuated<Field, Token![,]>,
    attrs: Vec<TokenStream>,
    generics: Generics,
    ctx: Option<(Ident, usize)>,
}

impl
    TryFrom<(
        Visibility,
        Signature,
        Vec<TokenStream>,
        Vec<Vec<Attribute>>,
        Generics,
        Option<Ident>,
    )> for Message
{
    type Error = syn::Error;

    fn try_from(
        (vis, mut sig, attrs, field_doc_attrs, generics, ctx): (
            Visibility,
            Signature,
            Vec<TokenStream>,
            Vec<Vec<Attribute>>,
            Generics,
            Option<Ident>,
        ),
    ) -> Result<Self, Self::Error> {
        let ident = format_ident!("{}", sig.ident.to_string().to_upper_camel_case());
        let mut ctx_pos = None;
        let fields: Punctuated<Field, Token![,]> = sig
            .inputs
            .iter_mut()
            .zip(field_doc_attrs)
            .enumerate()
            .filter_map(|(i, (input, doc_attrs))| match input {
                FnArg::Receiver(_) => None,
                FnArg::Typed(pat_type) => {
                    if let Some(ctx) = &ctx {
                        if let Pat::Ident(pat_ident) = &*pat_type.pat {
                            if &pat_ident.ident == ctx {
                                ctx_pos = Some(i.saturating_sub(1));
                                return None;
                            }
                        }
                    }

                    Some((doc_attrs, pat_type))
                },
            })
            .map::<syn::Result<Field>, _>(|(doc_attrs, pat_type)| {
                let ident = match pat_type.pat.as_ref() {
                    syn::Pat::Ident(pat_ident) => pat_ident.ident.clone(),
                    _ => return Err(syn::Error::new(pat_type.span(), "unsupported pattern - argments must be named when used with the actor macro")),
                };
                let ty = &pat_type.ty;

                Ok(parse_quote! {
                    #( #doc_attrs )*
                    #vis #ident: #ty
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(Message {
            vis,
            sig,
            ident,
            fields,
            attrs,
            generics,
            ctx: ctx.zip(ctx_pos),
        })
    }
}


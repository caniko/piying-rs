impl ToTokens for Messages {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let item_impl = &self.item_impl;
        let msg_enum = self.expand_msgs();
        let msg_impl_message = self.expand_msg_impls();
        let errors = self.errors.clone().map(|err| err.into_compile_error());

        tokens.extend(quote! {
            #item_impl

            #msg_enum
            #msg_impl_message
            #errors
        });
    }
}

impl Parse for Messages {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut item_impl: ItemImpl = input.parse()?;

        let ident = match item_impl.self_ty.as_ref() {
            Type::Path(type_path) => type_path
                .path
                .segments
                .last()
                .as_ref()
                .ok_or_else(|| syn::Error::new(type_path.path.span(), "missing ident from path"))?
                .ident
                .clone(),
            _ => {
                return Err(syn::Error::new(
                    item_impl.self_ty.span(),
                    "expected a path or ident",
                ));
            }
        };
        let (messages, errors) = Messages::extract_messages(&mut item_impl);

        Ok(Messages {
            item_impl,
            ident,
            messages,
            errors,
        })
    }
}

fn validate_param(ty: &Type) -> syn::Result<()> {
    match ty {
        Type::ImplTrait(_) => Err(syn::Error::new(
            ty.span(),
            "impl trait types are not supported in actor messages",
        )),
        Type::Infer(_) => Err(syn::Error::new(
            ty.span(),
            "type cannot be inferred in actor messages",
        )),
        Type::Reference(_) => Err(syn::Error::new(
            ty.span(),
            "references cannot be used in messages",
        )),
        Type::Group(group) => validate_param(group.elem.as_ref()),
        Type::Paren(ty) => validate_param(&ty.elem),
        _ => Ok(()),
    }
}

fn contains_generic_in_param(ty: &Type, generics: &[GenericParam]) -> Vec<GenericParam> {
    match ty {
        Type::Array(array) => contains_generic_in_param(&array.elem, generics),
        Type::BareFn(bare_fn) => {
            let mut params: Vec<_> = bare_fn
                .inputs
                .iter()
                .flat_map(|input| contains_generic_in_param(&input.ty, generics))
                .collect();
            if let ReturnType::Type(_, ty) = &bare_fn.output {
                params.extend(contains_generic_in_param(ty, generics));
            }
            params
        }
        Type::Group(group) => contains_generic_in_param(&group.elem, generics),
        Type::ImplTrait(_) => vec![],
        Type::Infer(_) => vec![],
        Type::Macro(_) => vec![],
        Type::Never(_) => vec![],
        Type::Paren(paren) => contains_generic_in_param(&paren.elem, generics),
        Type::Path(path) => {
            if let Some(ident) = path.path.get_ident() {
                let is_in_generics = generics
                    .iter()
                    .filter_map(|param| match param {
                        GenericParam::Type(type_param) => Some(type_param),
                        _ => None,
                    })
                    .any(|type_param| &type_param.ident == ident);
                if is_in_generics {
                    return vec![parse_quote! { #ident }];
                }
            }

            vec![]
        }
        Type::Ptr(ptr) => contains_generic_in_param(&ptr.elem, generics),
        Type::Reference(reference) => {
            let mut params = Vec::new();
            if let Some(lifetime) = &reference.lifetime {
                let is_in_generics = generics
                    .iter()
                    .filter_map(|param| match param {
                        GenericParam::Lifetime(lifetime) => Some(lifetime),
                        _ => None,
                    })
                    .any(|lt| &lt.lifetime == lifetime);
                if is_in_generics {
                    params.push(parse_quote! { #lifetime });
                }
            }
            params.extend(contains_generic_in_param(&reference.elem, generics));

            params
        }
        Type::Slice(slice) => contains_generic_in_param(&slice.elem, generics),
        Type::TraitObject(trait_obj) => trait_obj
            .bounds
            .iter()
            .flat_map(|bound| match bound {
                syn::TypeParamBound::Trait(trt) => {
                    if let Some(ident) = trt.path.get_ident() {
                        let is_in_generics = generics
                            .iter()
                            .filter_map(|param| match param {
                                GenericParam::Type(type_param) => Some(type_param),
                                _ => None,
                            })
                            .any(|type_param| &type_param.ident == ident);
                        if is_in_generics {
                            return vec![parse_quote! { #ident }];
                        }
                    }

                    vec![]
                }
                syn::TypeParamBound::Lifetime(lifetime) => {
                    let is_in_generics = generics
                        .iter()
                        .filter_map(|param| match param {
                            GenericParam::Lifetime(lifetime) => Some(lifetime),
                            _ => None,
                        })
                        .any(|lt| &lt.lifetime == lifetime);
                    if is_in_generics {
                        vec![parse_quote! { #lifetime }]
                    } else {
                        vec![]
                    }
                }
                syn::TypeParamBound::Verbatim(_) => vec![],
                _ => vec![],
            })
            .collect(),
        Type::Tuple(tuple) => tuple
            .elems
            .iter()
            .flat_map(|elem| contains_generic_in_param(elem, generics))
            .collect(),
        Type::Verbatim(_) => vec![],
        _ => vec![],
    }
}

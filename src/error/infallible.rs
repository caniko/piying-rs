/// An infallible error type, similar to [std::convert::Infallible].
///
/// Thespis provides its own Infallible type in order to implement Serialize/Deserialize for it.
#[derive(Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Infallible {}

impl Clone for Infallible {
    #[allow(clippy::non_canonical_clone_impl)]
    fn clone(&self) -> Infallible {
        *self
    }
}

impl fmt::Debug for Infallible {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl fmt::Display for Infallible {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl error::Error for Infallible {
    fn description(&self) -> &str {
        match *self {}
    }
}

impl PartialEq for Infallible {
    fn eq(&self, _: &Infallible) -> bool {
        match *self {}
    }
}

impl Eq for Infallible {}

impl PartialOrd for Infallible {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, _other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(_other))
    }
}

impl Ord for Infallible {
    fn cmp(&self, _other: &Self) -> cmp::Ordering {
        match *self {}
    }
}

impl Hash for Infallible {
    fn hash<H: Hasher>(&self, _: &mut H) {
        match *self {}
    }
}

#[cfg(all(feature = "remote", not(feature = "serde-codec"),))]
impl crate::remote::codec::Encode for Infallible {
    fn encode(&self) -> Result<Vec<u8>, crate::remote::codec::CodecError> {
        match *self {}
    }
}

#[cfg(all(feature = "remote", not(feature = "serde-codec"),))]
impl crate::remote::codec::Decode for Infallible {
    fn decode(_bytes: &[u8]) -> Result<Self, crate::remote::codec::CodecError> {
        Err(crate::remote::codec::CodecError::new(
            "cannot decode Infallible",
        ))
    }
}


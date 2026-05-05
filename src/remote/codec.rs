//! Pluggable codec traits for remote message serialization.
//!
//! The [`Encode`] and [`Decode`] traits abstract over the serialization
//! framework used for actor messages on the wire.  A blanket implementation
//! is provided behind the `serde-codec` feature flag so that consumer types
//! only need to derive `serde::Serialize` / `serde::Deserialize`.
//!
//! Downstream crates can provide their own blanket or manual implementations
//! for alternative serialization frameworks.

use std::{error, fmt};

// ---------------------------------------------------------------------------
// CodecError
// ---------------------------------------------------------------------------

/// An error that occurs during codec encode/decode operations.
///
/// Wraps an [`anyhow::Error`] for flexible error propagation while
/// implementing the standard [`Error`](error::Error) trait.
pub struct CodecError(anyhow::Error);

impl CodecError {
    /// Create a new codec error from any displayable value.
    ///
    /// This uses [`anyhow::Error::msg`] internally, which accepts any type
    /// implementing `Display + Debug + Send + Sync + 'static`.  Notably this
    /// includes `rkyv::rancor::Error` which does *not* implement
    /// `std::error::Error`.
    pub fn new(err: impl fmt::Display + fmt::Debug + Send + Sync + 'static) -> Self {
        CodecError(anyhow::Error::msg(err))
    }

    /// Consumes the error and returns the inner [`anyhow::Error`].
    pub fn into_inner(self) -> anyhow::Error {
        self.0
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl error::Error for CodecError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.0.source()
    }
}

impl From<String> for CodecError {
    fn from(s: String) -> Self {
        CodecError(anyhow::anyhow!("{s}"))
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for CodecError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for CodecError {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(CodecError::from(s))
    }
}

// ---------------------------------------------------------------------------
// Encode / Decode traits
// ---------------------------------------------------------------------------

/// Encode a value into bytes for remote transmission.
pub trait Encode: Send + 'static {
    /// Serialize this value into a byte vector.
    fn encode(&self) -> Result<Vec<u8>, CodecError>;
}

/// Decode bytes received from the network into a value.
pub trait Decode: Sized + Send + 'static {
    /// Deserialize a value from the given byte slice.
    fn decode(bytes: &[u8]) -> Result<Self, CodecError>;
}

// ---------------------------------------------------------------------------
// serde-codec blanket impls
// ---------------------------------------------------------------------------

#[cfg(feature = "serde-codec")]
impl<T> Encode for T
where
    T: serde::Serialize + Send + 'static,
{
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        rmp_serde::to_vec_named(self).map_err(|e| CodecError::new(e.to_string()))
    }
}

#[cfg(feature = "serde-codec")]
impl<T> Decode for T
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        rmp_serde::decode::from_slice(bytes).map_err(|e| CodecError::new(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// rkyv-codec blanket impls
// ---------------------------------------------------------------------------

#[cfg(feature = "rkyv-codec")]
impl<T> Encode for T
where
    T: rkyv::Archive
        + for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        > + Send
        + 'static,
{
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|v| v.to_vec())
            .map_err(|e| CodecError::new(e.to_string()))
    }
}

#[cfg(feature = "rkyv-codec")]
impl<T> Decode for T
where
    T: rkyv::Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned)
            .map_err(|e| CodecError::new(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// ThespisRkyvCodec — thin wrapper over libp2p_rkyv_codec for thespis wire types
// ---------------------------------------------------------------------------

#[cfg(feature = "rkyv-codec")]
mod rkyv_transport {
    use std::io;

    use async_trait::async_trait;
    use futures::prelude::*;
    use libp2p::{StreamProtocol, request_response};
    use libp2p_rkyv_codec::{VersionedFrameConfig, read_versioned_rkyv, write_versioned_rkyv};

    use super::super::messaging::{Config, SwarmRequest, SwarmResponse};

    const WIRE_MAGIC: [u8; 4] = *b"THSP";
    const WIRE_VERSION: u16 = 2;

    #[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    struct ThespisWireRequestEnvelope {
        magic: [u8; 4],
        version: u16,
        request: SwarmRequest,
    }

    #[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    struct ThespisWireResponseEnvelope {
        magic: [u8; 4],
        version: u16,
        response: SwarmResponse,
    }

    /// Length-prefixed rkyv codec for thespis's remote messaging protocol.
    ///
    /// The outer frame starts with `THSP`, a wire version, and the rkyv payload
    /// length so incompatible peers fail before actor/game dispatch.
    pub struct ThespisRkyvCodec {
        request_size_maximum: u64,
        response_size_maximum: u64,
    }

    impl ThespisRkyvCodec {
        /// Creates a new `ThespisRkyvCodec` from the messaging configuration.
        pub fn new(config: &Config) -> Self {
            ThespisRkyvCodec {
                request_size_maximum: config.request_size_maximum(),
                response_size_maximum: config.response_size_maximum(),
            }
        }

        fn request_frame_config(&self) -> VersionedFrameConfig {
            VersionedFrameConfig::new(WIRE_MAGIC, WIRE_VERSION, self.request_size_maximum)
        }

        fn response_frame_config(&self) -> VersionedFrameConfig {
            VersionedFrameConfig::new(WIRE_MAGIC, WIRE_VERSION, self.response_size_maximum)
        }
    }

    impl Clone for ThespisRkyvCodec {
        fn clone(&self) -> Self {
            Self {
                request_size_maximum: self.request_size_maximum,
                response_size_maximum: self.response_size_maximum,
            }
        }
    }

    impl std::fmt::Debug for ThespisRkyvCodec {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ThespisRkyvCodec")
                .field("wire_magic", &String::from_utf8_lossy(&WIRE_MAGIC))
                .field("wire_version", &WIRE_VERSION)
                .field("request_size_maximum", &self.request_size_maximum)
                .field("response_size_maximum", &self.response_size_maximum)
                .finish()
        }
    }

    fn io_invalid_data(message: impl Into<String>) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, message.into())
    }

    fn validate_request_envelope(envelope: ThespisWireRequestEnvelope) -> io::Result<SwarmRequest> {
        if envelope.magic != WIRE_MAGIC {
            return Err(io_invalid_data(format!(
                "request envelope bad magic: expected THSP, got {:02x?}",
                envelope.magic
            )));
        }
        if envelope.version != WIRE_VERSION {
            return Err(io_invalid_data(format!(
                "request envelope unsupported wire version: {} (expected {WIRE_VERSION})",
                envelope.version
            )));
        }
        Ok(envelope.request)
    }

    fn validate_response_envelope(
        envelope: ThespisWireResponseEnvelope,
    ) -> io::Result<SwarmResponse> {
        if envelope.magic != WIRE_MAGIC {
            return Err(io_invalid_data(format!(
                "response envelope bad magic: expected THSP, got {:02x?}",
                envelope.magic
            )));
        }
        if envelope.version != WIRE_VERSION {
            return Err(io_invalid_data(format!(
                "response envelope unsupported wire version: {} (expected {WIRE_VERSION})",
                envelope.version
            )));
        }
        Ok(envelope.response)
    }

    #[async_trait]
    impl request_response::Codec for ThespisRkyvCodec {
        type Protocol = StreamProtocol;
        type Request = SwarmRequest;
        type Response = SwarmResponse;

        async fn read_request<T>(
            &mut self,
            p: &Self::Protocol,
            io: &mut T,
        ) -> io::Result<Self::Request>
        where
            T: AsyncRead + Unpin + Send,
        {
            #[cfg(not(feature = "tracing"))]
            let _ = p;
            let envelope: ThespisWireRequestEnvelope =
                read_versioned_rkyv(io, self.request_frame_config(), "request").await?;
            let request = validate_request_envelope(envelope)?;
            #[cfg(feature = "tracing")]
            tracing::trace!(protocol = %p, summary = %request.summary(), "decoded thespis request");
            Ok(request)
        }

        async fn read_response<T>(
            &mut self,
            p: &Self::Protocol,
            io: &mut T,
        ) -> io::Result<Self::Response>
        where
            T: AsyncRead + Unpin + Send,
        {
            #[cfg(not(feature = "tracing"))]
            let _ = p;
            let envelope: ThespisWireResponseEnvelope =
                read_versioned_rkyv(io, self.response_frame_config(), "response").await?;
            let response = validate_response_envelope(envelope)?;
            #[cfg(feature = "tracing")]
            tracing::trace!(protocol = %p, summary = %response.summary(), "decoded thespis response");
            Ok(response)
        }

        async fn write_request<T>(
            &mut self,
            p: &Self::Protocol,
            io: &mut T,
            req: Self::Request,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            #[cfg(not(feature = "tracing"))]
            let _ = p;
            #[cfg(feature = "tracing")]
            tracing::trace!(protocol = %p, summary = %req.summary(), "encoding thespis request");
            let envelope = ThespisWireRequestEnvelope {
                magic: WIRE_MAGIC,
                version: WIRE_VERSION,
                request: req,
            };
            write_versioned_rkyv(io, &envelope, self.request_frame_config(), "request").await
        }

        async fn write_response<T>(
            &mut self,
            p: &Self::Protocol,
            io: &mut T,
            resp: Self::Response,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            #[cfg(not(feature = "tracing"))]
            let _ = p;
            #[cfg(feature = "tracing")]
            tracing::trace!(protocol = %p, summary = %resp.summary(), "encoding thespis response");
            let envelope = ThespisWireResponseEnvelope {
                magic: WIRE_MAGIC,
                version: WIRE_VERSION,
                response: resp,
            };
            write_versioned_rkyv(io, &envelope, self.response_frame_config(), "response").await
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::remote::wire::WireActorId;

        fn test_actor_id() -> WireActorId {
            WireActorId {
                sequence_id: 7,
                peer_id_bytes: vec![1, 2, 3, 4],
            }
        }

        #[test]
        fn request_envelope_roundtrip() {
            let request = SwarmRequest::Tell {
                actor_id: test_actor_id(),
                actor_remote_id: "actor".into(),
                message_remote_id: "message".into(),
                payload: vec![1, 2, 3],
                mailbox_timeout: None,
                immediate: false,
            };
            let envelope = ThespisWireRequestEnvelope {
                magic: WIRE_MAGIC,
                version: WIRE_VERSION,
                request,
            };
            let payload = libp2p_rkyv_codec::encode_rkyv(&envelope).unwrap();
            let decoded: ThespisWireRequestEnvelope =
                libp2p_rkyv_codec::decode_rkyv(&payload).unwrap();
            let request = validate_request_envelope(decoded).unwrap();
            assert_eq!(request.summary(), "Tell actor::message payload=3B");
        }

        #[test]
        fn response_envelope_roundtrip() {
            let envelope = ThespisWireResponseEnvelope {
                magic: WIRE_MAGIC,
                version: WIRE_VERSION,
                response: SwarmResponse::Tell(Ok(())),
            };
            let payload = libp2p_rkyv_codec::encode_rkyv(&envelope).unwrap();
            let decoded: ThespisWireResponseEnvelope =
                libp2p_rkyv_codec::decode_rkyv(&payload).unwrap();
            let response = validate_response_envelope(decoded).unwrap();
            assert_eq!(response.summary(), "Tell ok");
        }

        #[tokio::test]
        async fn frame_rejects_bad_magic() {
            let mut header = [0u8; 10];
            header[..4].copy_from_slice(b"BAD!");
            header[4..6].copy_from_slice(&WIRE_VERSION.to_le_bytes());
            let mut cursor = futures::io::Cursor::new(header);
            let config = VersionedFrameConfig::new(WIRE_MAGIC, WIRE_VERSION, 1024);
            assert!(
                read_versioned_rkyv::<ThespisWireRequestEnvelope, _>(
                    &mut cursor,
                    config,
                    "request"
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("bad magic")
            );
        }

        #[tokio::test]
        async fn frame_rejects_unsupported_version() {
            let mut header = [0u8; 10];
            header[..4].copy_from_slice(&WIRE_MAGIC);
            header[4..6].copy_from_slice(&1u16.to_le_bytes());
            let mut cursor = futures::io::Cursor::new(header);
            let config = VersionedFrameConfig::new(WIRE_MAGIC, WIRE_VERSION, 1024);
            assert!(
                read_versioned_rkyv::<ThespisWireRequestEnvelope, _>(
                    &mut cursor,
                    config,
                    "request"
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("unsupported wire version")
            );
        }
    }
}

#[cfg(feature = "rkyv-codec")]
pub use rkyv_transport::ThespisRkyvCodec;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "serde-codec")]
mod serde_tests {
    use super::*;

    #[test]
    fn serde_encode_decode_roundtrip() {
        let value: u64 = 12345;
        let bytes = value.encode().unwrap();
        let decoded = u64::decode(&bytes).unwrap();
        assert_eq!(decoded, 12345);
    }

    #[test]
    fn serde_decode_bad_bytes_returns_error() {
        let result = u64::decode(&[0xFF, 0xFF]);
        assert!(result.is_err());
    }
}

#[cfg(test)]
#[cfg(feature = "rkyv-codec")]
mod rkyv_tests {
    use super::*;

    #[test]
    fn rkyv_encode_decode_roundtrip() {
        let value: u64 = 12345;
        let bytes = value.encode().unwrap();
        let decoded = u64::decode(&bytes).unwrap();
        assert_eq!(decoded, 12345);
    }

    #[test]
    fn rkyv_decode_bad_bytes_returns_error() {
        let result = u64::decode(&[0xFF, 0xFF]);
        assert!(result.is_err());
    }
}

/// Error that can occur when sending a message to an actor.
#[cfg(feature = "remote")]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RemoteSendError<E = Infallible> {
    /// The actor isn't running.
    ActorNotRunning,
    /// The actor panicked or was stopped before a reply could be received.
    ActorStopped,
    /// The actor's remote ID was not found.
    UnknownActor {
        /// The remote ID of the actor.
        actor_remote_id: std::borrow::Cow<'static, str>,
    },
    /// The message remote ID was not found for the actor.
    UnknownMessage {
        /// The remote ID of the actor.
        actor_remote_id: std::borrow::Cow<'static, str>,
        /// The remote ID of the message.
        message_remote_id: std::borrow::Cow<'static, str>,
    },
    /// The remote actor was found given the ID, but was not the correct type.
    BadActorType,
    /// The actors mailbox is full.
    MailboxFull,
    /// Timed out waiting for a reply.
    ReplyTimeout,
    /// An error returned by the actor's message handler.
    HandlerError(E),
    /// Failed to serialize the message.
    SerializeMessage(crate::remote::codec::CodecError),
    /// Failed to deserialize the incoming message.
    DeserializeMessage(crate::remote::codec::CodecError),
    /// Failed to serialize the reply.
    SerializeReply(crate::remote::codec::CodecError),
    /// Failed to serialize the handler error.
    SerializeHandlerError(crate::remote::codec::CodecError),
    /// Failed to deserialize the handler error.
    DeserializeHandlerError(crate::remote::codec::CodecError),

    /// The actor swarm has not been bootstrapped.
    SwarmNotBootstrapped,
    /// The request could not be sent because a dialing attempt failed.
    DialFailure,
    /// The request timed out before a response was received.
    ///
    /// It is not known whether the request may have been
    /// received (and processed) by the remote peer.
    NetworkTimeout,
    /// The connection closed before a response was received.
    ///
    /// It is not known whether the request may have been
    /// received (and processed) by the remote peer.
    ConnectionClosed,
    /// The remote supports none of the requested protocols.
    UnsupportedProtocols,
    /// An IO failure happened on an outbound stream.
    #[cfg_attr(feature = "serde", serde(skip))]
    Io(Option<std::io::Error>),
}

#[cfg(feature = "remote")]
impl<E> RemoteSendError<E> {
    /// Maps the inner error to another type if the variant is [`HandlerError`](RemoteSendError::HandlerError).
    pub fn map_err<F, O>(self, mut op: O) -> RemoteSendError<F>
    where
        O: FnMut(E) -> F,
    {
        match self {
            RemoteSendError::ActorNotRunning => RemoteSendError::ActorNotRunning,
            RemoteSendError::ActorStopped => RemoteSendError::ActorStopped,
            RemoteSendError::UnknownActor { actor_remote_id } => {
                RemoteSendError::UnknownActor { actor_remote_id }
            }
            RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            } => RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            },
            RemoteSendError::BadActorType => RemoteSendError::BadActorType,
            RemoteSendError::MailboxFull => RemoteSendError::MailboxFull,
            RemoteSendError::ReplyTimeout => RemoteSendError::ReplyTimeout,
            RemoteSendError::HandlerError(err) => RemoteSendError::HandlerError(op(err)),
            RemoteSendError::SerializeMessage(err) => RemoteSendError::SerializeMessage(err),
            RemoteSendError::DeserializeMessage(err) => RemoteSendError::DeserializeMessage(err),
            RemoteSendError::SerializeReply(err) => RemoteSendError::SerializeReply(err),
            RemoteSendError::SerializeHandlerError(err) => {
                RemoteSendError::SerializeHandlerError(err)
            }
            RemoteSendError::DeserializeHandlerError(err) => {
                RemoteSendError::DeserializeHandlerError(err)
            }
            RemoteSendError::SwarmNotBootstrapped => RemoteSendError::SwarmNotBootstrapped,
            RemoteSendError::DialFailure => RemoteSendError::DialFailure,
            RemoteSendError::NetworkTimeout => RemoteSendError::NetworkTimeout,
            RemoteSendError::ConnectionClosed => RemoteSendError::ConnectionClosed,
            RemoteSendError::UnsupportedProtocols => RemoteSendError::UnsupportedProtocols,
            RemoteSendError::Io(err) => RemoteSendError::Io(err),
        }
    }
}

#[cfg(feature = "remote")]
impl<E> RemoteSendError<RemoteSendError<E>> {
    /// Flattens a nested SendError.
    pub fn flatten(self) -> RemoteSendError<E> {
        use RemoteSendError::*;
        match self {
            ActorNotRunning | HandlerError(ActorNotRunning) => ActorNotRunning,
            ActorStopped | HandlerError(ActorStopped) => ActorStopped,
            UnknownActor { actor_remote_id } | HandlerError(UnknownActor { actor_remote_id }) => {
                UnknownActor { actor_remote_id }
            }
            UnknownMessage {
                actor_remote_id,
                message_remote_id,
            }
            | HandlerError(UnknownMessage {
                actor_remote_id,
                message_remote_id,
            }) => UnknownMessage {
                actor_remote_id,
                message_remote_id,
            },
            BadActorType | HandlerError(BadActorType) => BadActorType,
            MailboxFull | HandlerError(MailboxFull) => MailboxFull,
            ReplyTimeout | HandlerError(ReplyTimeout) => ReplyTimeout,
            HandlerError(HandlerError(err)) => HandlerError(err),
            SerializeMessage(err) | HandlerError(SerializeMessage(err)) => SerializeMessage(err),
            DeserializeMessage(err) | HandlerError(DeserializeMessage(err)) => {
                DeserializeMessage(err)
            }
            SerializeReply(err) | HandlerError(SerializeReply(err)) => SerializeReply(err),
            SerializeHandlerError(err) | HandlerError(SerializeHandlerError(err)) => {
                SerializeHandlerError(err)
            }
            DeserializeHandlerError(err) | HandlerError(DeserializeHandlerError(err)) => {
                RemoteSendError::DeserializeHandlerError(err)
            }
            SwarmNotBootstrapped | HandlerError(SwarmNotBootstrapped) => SwarmNotBootstrapped,
            DialFailure | HandlerError(DialFailure) => DialFailure,
            NetworkTimeout | HandlerError(NetworkTimeout) => NetworkTimeout,
            ConnectionClosed | HandlerError(ConnectionClosed) => ConnectionClosed,
            UnsupportedProtocols | HandlerError(UnsupportedProtocols) => UnsupportedProtocols,
            Io(err) | HandlerError(Io(err)) => Io(err),
        }
    }
}

#[cfg(feature = "remote")]
impl<M, E> From<SendError<M, E>> for RemoteSendError<E> {
    fn from(err: SendError<M, E>) -> Self {
        match err {
            SendError::ActorNotRunning(_) => RemoteSendError::ActorNotRunning,
            SendError::ActorStopped => RemoteSendError::ActorStopped,
            SendError::MailboxFull(_) => RemoteSendError::MailboxFull,
            SendError::HandlerError(err) => RemoteSendError::HandlerError(err),
            SendError::Timeout(_) => RemoteSendError::ReplyTimeout,
        }
    }
}

#[cfg(feature = "remote")]
impl<E> From<libp2p::request_response::OutboundFailure> for RemoteSendError<E> {
    fn from(err: libp2p::request_response::OutboundFailure) -> Self {
        match err {
            libp2p::request_response::OutboundFailure::DialFailure => RemoteSendError::DialFailure,
            libp2p::request_response::OutboundFailure::Timeout => RemoteSendError::NetworkTimeout,
            libp2p::request_response::OutboundFailure::ConnectionClosed => {
                RemoteSendError::ConnectionClosed
            }
            libp2p::request_response::OutboundFailure::UnsupportedProtocols => {
                RemoteSendError::UnsupportedProtocols
            }
            libp2p::request_response::OutboundFailure::Io(err) => RemoteSendError::Io(Some(err)),
        }
    }
}

#[cfg(feature = "remote")]
impl<E> fmt::Display for RemoteSendError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoteSendError::ActorNotRunning => write!(f, "actor not running"),
            RemoteSendError::ActorStopped => write!(f, "actor stopped"),
            RemoteSendError::UnknownActor { actor_remote_id } => {
                write!(f, "unknown actor '{actor_remote_id}'")
            }
            RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            } => write!(
                f,
                "unknown message '{message_remote_id}' for actor '{actor_remote_id}'"
            ),
            RemoteSendError::BadActorType => write!(f, "bad actor type"),
            RemoteSendError::MailboxFull => write!(f, "mailbox full"),
            RemoteSendError::ReplyTimeout => write!(f, "timeout"),
            RemoteSendError::HandlerError(err) => err.fmt(f),
            RemoteSendError::SerializeMessage(err) => {
                write!(f, "failed to serialize message: {err}")
            }
            RemoteSendError::DeserializeMessage(err) => {
                write!(f, "failed to deserialize message: {err}")
            }
            RemoteSendError::SerializeReply(err) => {
                write!(f, "failed to serialize reply: {err}")
            }
            RemoteSendError::SerializeHandlerError(err) => {
                write!(f, "failed to serialize handler error: {err}")
            }
            RemoteSendError::DeserializeHandlerError(err) => {
                write!(f, "failed to deserialize handler error: {err}")
            }
            RemoteSendError::SwarmNotBootstrapped => write!(f, "swarm not bootstrapped"),
            RemoteSendError::DialFailure => write!(f, "dial failure"),
            RemoteSendError::NetworkTimeout => write!(f, "network timeout"),
            RemoteSendError::ConnectionClosed => write!(f, "connection closed"),
            RemoteSendError::UnsupportedProtocols => write!(f, "unsupported protocols"),
            RemoteSendError::Io(Some(err)) => err.fmt(f),
            RemoteSendError::Io(None) => write!(f, "io error"),
        }
    }
}

#[cfg(feature = "remote")]
impl<E> error::Error for RemoteSendError<E> where E: fmt::Debug + fmt::Display {}

/// An error returned when the remote system has already been bootstrapped.
#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug)]
pub struct SwarmAlreadyBootstrappedError;

#[cfg(feature = "remote")]
impl fmt::Display for SwarmAlreadyBootstrappedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "swarm already bootstrapped")
    }
}

#[cfg(feature = "remote")]
impl error::Error for SwarmAlreadyBootstrappedError {}

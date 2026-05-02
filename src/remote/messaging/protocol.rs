/// Represents different types of requests that can be made within the swarm.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub enum SwarmRequest {
    /// Represents a request to ask a peer for some data or action.
    ///
    /// This variant includes information about the actor, the message, payload, and timeout settings.
    Ask {
        /// Identifier of the actor initiating the request.
        actor_id: WireActorId,
        /// Remote identifier of the actor.
        actor_remote_id: String,
        /// Remote identifier of the message.
        message_remote_id: String,
        /// The payload data to be sent with the request.
        payload: Vec<u8>,
        /// Optional timeout duration for the mailbox to receive the request.
        mailbox_timeout: Option<Duration>,
        /// Optional timeout duration to wait for a reply to the request.
        reply_timeout: Option<Duration>,
        /// Indicates whether the request should be sent immediately.
        immediate: bool,
    },
    /// Represents a request to tell a peer some information without expecting a response.
    ///
    /// This variant includes information about the actor, the message, payload, and timeout settings.
    Tell {
        /// Identifier of the actor initiating the message.
        actor_id: WireActorId,
        /// Remote identifier of the actor.
        actor_remote_id: String,
        /// Remote identifier of the message.
        message_remote_id: String,
        /// The payload data to be sent with the message.
        payload: Vec<u8>,
        /// Optional timeout duration for the mailbox to receive the message.
        mailbox_timeout: Option<Duration>,
        /// Indicates whether the message should be sent immediately.
        immediate: bool,
    },
    /// A request to link two actors together.
    Link {
        /// Actor ID.
        actor_id: WireActorId,
        /// Actor remote ID.
        actor_remote_id: String,
        /// Sibling ID.
        sibling_id: WireActorId,
        /// Sibling remote ID.
        sibling_remote_id: String,
    },
    /// A request to unlink two actors.
    Unlink {
        /// Actor ID.
        actor_id: WireActorId,
        /// Actor remote ID.
        actor_remote_id: String,
        /// Sibling ID.
        sibling_id: WireActorId,
    },
    /// A signal notifying a linked actor has died.
    SignalLinkDied {
        /// The actor which died.
        dead_actor_id: WireActorId,
        /// The actor to notify.
        notified_actor_id: WireActorId,
        /// The actor to notify.
        notified_actor_remote_id: String,
        /// The reason the actor died.
        stop_reason: WireActorStopReason,
    },
}

impl SwarmRequest {
    /// Compact diagnostic summary that never includes payload bytes.
    pub fn summary(&self) -> String {
        match self {
            SwarmRequest::Ask {
                actor_remote_id,
                message_remote_id,
                payload,
                ..
            } => format!(
                "Ask {actor_remote_id}::{message_remote_id} payload={}B",
                payload.len()
            ),
            SwarmRequest::Tell {
                actor_remote_id,
                message_remote_id,
                payload,
                ..
            } => format!(
                "Tell {actor_remote_id}::{message_remote_id} payload={}B",
                payload.len()
            ),
            SwarmRequest::Link {
                actor_remote_id,
                sibling_remote_id,
                ..
            } => format!("Link {actor_remote_id}<->{sibling_remote_id}"),
            SwarmRequest::Unlink {
                actor_remote_id, ..
            } => format!("Unlink {actor_remote_id}"),
            SwarmRequest::SignalLinkDied {
                notified_actor_remote_id,
                ..
            } => format!("SignalLinkDied {notified_actor_remote_id}"),
        }
    }
}

/// Represents different types of responses that can be sent within the swarm.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub enum SwarmResponse {
    /// Represents the response to an `Ask` request.
    ///
    /// Contains either the successful payload data or an error indicating why the send failed.
    Ask(Result<Vec<u8>, WireRemoteSendError>),
    /// Represents the response to a `Tell` request.
    ///
    /// Contains either a successful acknowledgment or an error indicating why the send failed.
    Tell(Result<(), WireRemoteSendError>),
    /// Represents the response to a link request.
    Link(Result<(), WireRemoteSendError>),
    /// Represents the response to an unlink request.
    Unlink(Result<(), WireRemoteSendError>),
    /// Represents the response to a link died signal.
    SignalLinkDied(Result<(), WireRemoteSendError>),
    /// Represents a failure that occurred while attempting to send an outbound request.
    ///
    /// Contains the error that caused the outbound request to fail.
    OutboundFailure(WireRemoteSendError),
}

impl SwarmResponse {
    /// Compact diagnostic summary that never includes payload bytes.
    pub fn summary(&self) -> String {
        match self {
            SwarmResponse::Ask(Ok(payload)) => format!("Ask ok payload={}B", payload.len()),
            SwarmResponse::Ask(Err(err)) => format!("Ask err {err:?}"),
            SwarmResponse::Tell(Ok(())) => "Tell ok".to_string(),
            SwarmResponse::Tell(Err(err)) => format!("Tell err {err:?}"),
            SwarmResponse::Link(Ok(())) => "Link ok".to_string(),
            SwarmResponse::Link(Err(err)) => format!("Link err {err:?}"),
            SwarmResponse::Unlink(Ok(())) => "Unlink ok".to_string(),
            SwarmResponse::Unlink(Err(err)) => format!("Unlink err {err:?}"),
            SwarmResponse::SignalLinkDied(Ok(())) => "SignalLinkDied ok".to_string(),
            SwarmResponse::SignalLinkDied(Err(err)) => {
                format!("SignalLinkDied err {err:?}")
            }
            SwarmResponse::OutboundFailure(err) => format!("OutboundFailure {err:?}"),
        }
    }
}

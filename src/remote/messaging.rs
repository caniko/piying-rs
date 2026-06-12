//! Remote message passing infrastructure for actors across the network.
//!
//! This module provides the core messaging capabilities that enable actors running on different
//! nodes to communicate with each other seamlessly. It handles the serialization, routing, and
//! delivery of messages between remote actors while maintaining the same ergonomics as local
//! actor communication.
//!
//! # Key Responsibilities
//!
//! - **Message Serialization**: Automatically serializes and deserializes actor messages for
//!   network transmission using efficient binary protocols
//! - **Request-Response Communication**: Implements reliable ask/tell patterns for remote actors
//!   with configurable timeouts and delivery guarantees
//! - **Connection Management**: Manages libp2p connections and handles connection failures,
//!   retries, and peer disconnections gracefully
//! - **Actor Lifecycle Integration**: Supports remote actor linking, unlinking, and death
//!   notifications across network boundaries
//! - **Backpressure Handling**: Provides mailbox timeout controls to prevent overwhelming
//!   remote actors with too many concurrent messages
//!
//! # Architecture
//!
//! The messaging system is built on top of libp2p's request-response protocol, providing
//! reliable delivery semantics while maintaining the actor model's message-passing paradigm.
//! Messages are automatically routed to the appropriate peer based on the target actor's
//! peer ID, with transparent handling of network-level concerns.
//!
//! The module integrates closely with Thespis's local actor system, allowing remote actors
//! to be used interchangeably with local actors through the same `ActorRef` interface.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    sync::LazyLock,
    task,
    time::Duration,
};

use futures::FutureExt;
use libp2p::{
    PeerId, StreamProtocol, request_response,
    swarm::{
        ConnectionDenied, ConnectionId, DialFailure, FromSwarm, NetworkBehaviour, THandler,
        THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
};
use tokio::{sync::oneshot, task::JoinSet};

use crate::{
    actor::ActorId,
    error::{ActorStopReason, Infallible, RemoteSendError},
};

use super::wire::{WireActorId, WireActorStopReason, WireRemoteSendError};

use super::_internal::{
    REMOTE_ACTORS, REMOTE_MESSAGES, RemoteActorFns, RemoteMessageFns, RemoteMessageRegistrationID,
};

/// Protocol id for thespis messaging. With the `session-isolation` feature on
/// and `THESPAN_SESSION_ID` set, the id is suffixed at first access; otherwise
/// it stays equal to `/thespis/messaging/1.0.0` and is byte-identical to a
/// `const StreamProtocol::new`. Cheap to clone (`StreamProtocol` is `Cow`).
fn proto_name() -> StreamProtocol {
    use std::sync::LazyLock;
    static NAME: LazyLock<StreamProtocol> = LazyLock::new(|| {
        StreamProtocol::try_from_owned(super::session::applied_protocol("/thespis/messaging/1.0.0"))
            .expect("messaging protocol id must start with '/'")
    });
    NAME.clone()
}

static REMOTE_ACTORS_MAP: LazyLock<HashMap<&'static str, RemoteActorFns>> = LazyLock::new(|| {
    let mut existing_ids = HashSet::new();
    for (id, _) in REMOTE_ACTORS {
        if !existing_ids.insert(id) {
            panic!("duplicate remote actor detected for actor '{id}'");
        }
    }
    REMOTE_ACTORS.iter().copied().collect()
});

static REMOTE_MESSAGES_MAP: LazyLock<
    HashMap<RemoteMessageRegistrationID<'static>, RemoteMessageFns>,
> = LazyLock::new(|| {
    let mut existing_ids = HashSet::new();
    for (id, _) in REMOTE_MESSAGES {
        if !existing_ids.insert(id) {
            panic!(
                "duplicate remote message detected for actor '{}' and message '{}'",
                id.actor_remote_id, id.message_remote_id
            );
        }
    }
    REMOTE_MESSAGES.iter().copied().collect()
});

type AskResult = Result<Vec<u8>, RemoteSendError<Vec<u8>>>;
type TellResult = Result<(), RemoteSendError>;
type LinkResult = Result<(), RemoteSendError>;
type UnlinkResult = Result<(), RemoteSendError>;
type SignalLinkDiedResult = Result<(), RemoteSendError>;

/// Identifier for a request within the swarm behavior.
///
/// Requests can be either local (handled within the same peer) or outbound
/// (sent to remote peers via libp2p's request-response protocol).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RequestId {
    /// A local request handled within the same peer.
    Local(u64),
    /// An outbound request sent to a remote peer.
    Outbound(request_response::OutboundRequestId),
}

impl RequestId {
    fn unwrap_outbound(self) -> request_response::OutboundRequestId {
        match self {
            RequestId::Local(_) => panic!("called unwrap_outbound on a local request id"),
            RequestId::Outbound(id) => id,
        }
    }
}

impl PartialEq<request_response::OutboundRequestId> for RequestId {
    fn eq(&self, other: &request_response::OutboundRequestId) -> bool {
        match self {
            RequestId::Local(_) => false,
            RequestId::Outbound(id) => id.eq(other),
        }
    }
}

impl PartialEq<RequestId> for request_response::OutboundRequestId {
    fn eq(&self, other: &RequestId) -> bool {
        match other {
            RequestId::Local(_) => false,
            RequestId::Outbound(other) => self.eq(other),
        }
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::Local(id) => write!(f, "{id}"),
            RequestId::Outbound(id) => id.fmt(f),
        }
    }
}

enum ReplyChannel {
    Event(RequestId),
    Local(oneshot::Sender<SwarmResponse>),
    Remote(request_response::ResponseChannel<SwarmResponse>),
}

include!("messaging/protocol.rs");
include!("messaging/events.rs");
include!("messaging/behaviour.rs");
include!("messaging/handlers.rs");
include!("messaging/tests.rs");

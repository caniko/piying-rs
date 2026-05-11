//! Actor registration and discovery system for distributed actor networks.
//!
//! This module implements a distributed registry that allows actors to register themselves
//! under human-readable names and be discovered by other actors across the network. It uses
//! Kademlia DHT (Distributed Hash Table) for decentralized storage and lookup of actor
//! registrations, ensuring high availability and fault tolerance.
//!
//! # Key Responsibilities
//!
//! - **Actor Registration**: Enables actors to register themselves under string-based names,
//!   making them discoverable by other actors across the network
//! - **Distributed Discovery**: Provides lookup capabilities to find all actors registered
//!   under a given name, regardless of which peer they're running on
//! - **Metadata Storage**: Stores actor metadata including unique identifiers, peer locations,
//!   and other registration details in a distributed manner
//! - **Network Resilience**: Uses Kademlia's distributed nature to ensure actor registrations
//!   remain available even if some network nodes go offline
//! - **Local Caching**: Maintains local caches of discovered actors to reduce lookup latency
//!   and network overhead for frequently accessed actors
//!
//! # Architecture
//!
//! The registry leverages libp2p's Kademlia implementation to create a distributed hash table
//! where actor names serve as keys and actor metadata serves as values. Each actor registration
//! involves two operations: advertising the actor name as a "provider" and storing the actor's
//! detailed metadata as a record in the DHT.
//!
//! This dual approach enables efficient discovery where clients first find all peers providing
//! a given actor name, then retrieve the specific metadata for each instance. This supports
//! scenarios where multiple actors may be registered under the same logical name across
//! different peers for load balancing or redundancy.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque, hash_map::Entry},
    fmt,
    num::NonZero,
    str::{self, FromStr},
    sync::Arc,
    task,
    time::{Duration, Instant},
};

use libp2p::{
    Multiaddr, PeerId, StreamProtocol,
    kad::{self, StoreInserts, store::RecordStore},
    swarm::{
        ConnectionDenied, ConnectionId, DialError, DialFailure, FromSwarm, NetworkBehaviour,
        NewExternalAddrOfPeer, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
        behaviour::ConnectionEstablished,
    },
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    actor::{ActorId, ActorIdFromBytesError},
    error::RegistryError,
};

/// Protocol id for thespis registry. With the `session-isolation` feature on
/// and `THESPAN_SESSION_ID` set, the id is suffixed at first access; otherwise
/// it stays equal to `/thespis/registry/1.0.0` and is byte-identical to a
/// `const StreamProtocol::new`. Cheap to clone (`StreamProtocol` is `Cow`).
fn proto_name() -> StreamProtocol {
    use std::sync::LazyLock;
    static NAME: LazyLock<StreamProtocol> = LazyLock::new(|| {
        StreamProtocol::try_from_owned(super::session::applied_protocol("/thespis/registry/1.0.0"))
            .expect("registry protocol id must start with '/'")
    });
    NAME.clone()
}

type RegisterResult = Result<(), RegistryError>;
pub(super) type LookupResult = Result<ActorRegistration<'static>, RegistryError>;
type LookupLocalResult = Result<Option<ActorRegistration<'static>>, RegistryError>;

pub(super) type RegisterReply = oneshot::Sender<RegisterResult>;
pub(super) type LookupReply = mpsc::UnboundedSender<LookupResult>;
pub(super) type LookupLocalReply = oneshot::Sender<LookupLocalResult>;
pub(super) type UnregisterReply = oneshot::Sender<()>;

include!("registry/events.rs");
include!("registry/behaviour.rs");
include!("registry/network_behaviour.rs");
include!("registry/types.rs");

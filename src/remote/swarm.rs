#[cfg(feature = "remote-registry")]
use std::task::Poll;
use std::{
    borrow::Cow, cell::RefCell, marker::PhantomData, pin, str, sync::Arc, task, time::Duration,
};

use arc_swap::ArcSwapOption;

use futures::{Future, FutureExt, ready};
#[cfg(feature = "remote-registry")]
use futures::{Stream, StreamExt};
use libp2p::PeerId;
use tokio::sync::{mpsc, oneshot};

use crate::{
    Actor,
    actor::ActorId,
    error::{ActorStopReason, Infallible, RemoteSendError},
};
#[cfg(feature = "remote-registry")]
use crate::{
    actor::{ActorRef, RemoteActorRef},
    error::RegistryError,
};

#[cfg(feature = "remote-registry")]
use super::{
    DowncastRegsiteredActorRefError, REMOTE_REGISTRY, RemoteRegistryActorRef,
    registry::{
        ActorRegistration, LookupLocalReply, LookupReply, LookupResult, RegisterReply,
        UnregisterReply,
    },
};
use super::{RemoteActor, messaging::SwarmResponse};

static ACTOR_SWARM: ArcSwapOption<ActorSwarm> = ArcSwapOption::const_empty();

thread_local! {
    static THREAD_LOCAL_SWARM: RefCell<Option<Arc<ActorSwarm>>> = const { RefCell::new(None) };
}

/// `ActorSwarm` is the core component for remote actors within Piying.
///
/// It is responsible for managing a swarm of distributed nodes using libp2p,
/// enabling peer discovery, actor registration, and remote message routing.
///
/// ## Key Features
///
/// - **Swarm Management**: Initializes and manages the libp2p swarm, allowing nodes to discover
///   and communicate in a peer-to-peer network.
/// - **Actor Registration**: Actors can be registered under a unique name, making them discoverable
///   and accessible across the network.
/// - **Message Routing**: Handles reliable message delivery to remote actors using Kademlia DHT.
///
/// The `ActorSwarm` is the essential component for enabling distributed actor communication
/// and message passing across decentralized nodes.
#[derive(Debug)]
pub(crate) struct ActorSwarm {
    swarm_tx: SwarmSender,
    local_peer_id: PeerId,
}

include!("swarm/actor_swarm.rs");
#[cfg(feature = "remote-registry")]
include!("swarm/lookup_stream.rs");
include!("swarm/commands.rs");
include!("swarm/tests.rs");

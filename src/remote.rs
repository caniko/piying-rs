//! # Remote Actors in Piying
//!
//! The `remote` module in Piying provides tools for managing distributed actors across nodes,
//! enabling actors to communicate seamlessly in a peer-to-peer (P2P) network. By leveraging
//! the [libp2p](https://libp2p.io) library, Piying allows you to send messages between actors
//! on different nodes as though they were local.
//!
//! ## Feature Layers
//!
//! - `remote`: messaging core (request-response). Bring your own libp2p swarm.
//! - `remote-registry`: adds a Kademlia name registry (`register`/`lookup` across the network).
//! - `remote-tcp`: adds `bootstrap()`/`bootstrap_on()` over TCP; implies `remote-registry`.
//! - `remote-quic`: adds the QUIC transport to the bootstrap swarm; implies `remote-tcp`.
//! - `remote-mdns`: adds mDNS local-network discovery to the bootstrap swarm; implies
//!   `remote-tcp`.
//! - `remote-bootstrap`: `remote-quic` + `remote-mdns`, the full bootstrap swarm.
//!
//! ## Key Features
//!
//! - **Composable Architecture**: The [`Behaviour`] struct implements libp2p's `NetworkBehaviour`,
//!   allowing seamless integration with existing libp2p applications and other protocols.
//! - **Custom Transport**: The [`run_swarm()`] function accepts a pre-built swarm with any
//!   transport while handling the event loop for you.
//! - **Reliable Messaging**: Request-response protocols deliver messages between nodes.
//! - **Actor Registration & Discovery** (`remote-registry`): Actors can be registered under
//!   unique names and looked up across the network using
//!   [`RemoteActorRef`](crate::actor::RemoteActorRef), backed by a Kademlia DHT.
//! - **Quick Bootstrap** (`remote-tcp` and up): The `bootstrap()` and `bootstrap_on()` functions
//!   provide one-line setup for development and simple deployments.
//!
//! ## Getting Started
//!
//! For quick prototyping and development (requires `remote-tcp`; add `remote-mdns` for
//! local-network discovery):
//!
//! ```ignore
//! use piying::remote;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // One line to bootstrap a distributed actor system
//!     let peer_id = remote::bootstrap()?;
//!
//!     // Now use actors normally
//!     // actor_ref.register("my_actor").await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! For production deployments with custom configuration:
//!
//! ```ignore
//! use piying::remote;
//! use libp2p::swarm::NetworkBehaviour;
//!
//! #[derive(NetworkBehaviour)]
//! struct MyBehaviour {
//!     piying: remote::Behaviour,
//!     // Add other libp2p behaviors as needed
//! }
//!
//! // Create custom libp2p swarm with full control over
//! // transports, discovery, and protocol composition
//! ```

use std::{
    any,
    collections::HashMap,
    str,
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, TryLockError},
};

#[cfg(all(
    feature = "remote-tcp",
    feature = "serde-codec",
    not(feature = "rkyv-codec")
))]
use std::error;

use futures::StreamExt;
use libp2p::{PeerId, Swarm, swarm::NetworkBehaviour};

#[cfg(feature = "remote-registry")]
use crate::error::RegistryError;
use crate::{
    Actor,
    actor::{ActorId, ActorRef, Links, WeakActorRef},
    error::RemoteSendError,
    mailbox::SignalMailbox,
};

#[cfg(not(any(feature = "serde-codec", feature = "rkyv-codec")))]
compile_error!("The `remote` feature requires either `serde-codec` or `rkyv-codec`");

#[doc(hidden)]
pub mod _internal;
mod behaviour;
pub mod codec;
#[allow(missing_docs)] // rkyv::Archive derive generates undocumented archived types
pub mod messaging;
#[cfg(feature = "remote-registry")]
pub mod registry;
pub mod session;
mod swarm;
pub mod wire;

pub use behaviour::*;
pub use session::applied_protocol;
pub use swarm::*;

/// Process-wide local actor registry.
///
/// This is deliberately a [`std::sync::Mutex`]: it is shared by every tokio
/// runtime in the process, and a fair async mutex hands ownership to the head
/// waiter. If that waiter's runtime thread is blocked, nobody releases the lock
/// and every other runtime hangs on teardown. Every critical section here is
/// short and synchronous, so no waiter can own the lock while its thread stalls.
pub(crate) static REMOTE_REGISTRY: LazyLock<Mutex<HashMap<ActorId, RemoteRegistryActorRef>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

type RegistryGuard = MutexGuard<'static, HashMap<ActorId, RemoteRegistryActorRef>>;

/// Lock the registry, recovering from poisoning.
///
/// The guard must never be held across an `.await`.
pub(crate) fn lock_registry() -> RegistryGuard {
    REMOTE_REGISTRY
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Try to lock the registry without blocking.
///
/// Returns `None` when another thread holds the lock. A poisoned lock is
/// recovered rather than reported.
fn try_lock_registry() -> Option<RegistryGuard> {
    match REMOTE_REGISTRY.try_lock() {
        Ok(registry) => Some(registry),
        Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
        Err(TryLockError::WouldBlock) => None,
    }
}

/// Register an actor in the local REMOTE_REGISTRY under a well-known ActorId.
/// This allows `RemoteActorRef::for_peer()` to find the actor without DHT lookup.
pub async fn register_actor_local<A: Actor>(actor_ref: &ActorRef<A>, id: ActorId) {
    let entry = RemoteRegistryActorRef::new(actor_ref.clone(), None);
    lock_registry().insert(id, entry);
}

/// Remove an actor from the local REMOTE_REGISTRY by its well-known ActorId.
/// Returns `true` if the entry was present and removed.
pub fn unregister_actor_local(id: &ActorId) -> bool {
    // Use try_lock to avoid blocking (this may be called from a Drop impl).
    try_lock_registry().is_some_and(|mut registry| registry.remove(id).is_some())
}

pub(crate) struct RemoteRegistryActorRef {
    actor_ref: BoxRegisteredActorRef,
    #[cfg_attr(not(feature = "remote-registry"), allow(dead_code))]
    pub(crate) name: Option<Arc<str>>,
    pub(crate) signal_mailbox: Box<dyn SignalMailbox>,
    pub(crate) links: Links,
}

impl RemoteRegistryActorRef {
    pub(crate) fn new<A: Actor>(actor_ref: ActorRef<A>, name: Option<Arc<str>>) -> Self {
        let signal_mailbox = actor_ref.weak_signal_mailbox();
        let links = actor_ref.links.clone();
        Self {
            actor_ref: BoxRegisteredActorRef::Strong(Box::new(actor_ref)),
            name,
            signal_mailbox,
            links,
        }
    }

    pub(crate) fn new_weak<A: Actor>(actor_ref: WeakActorRef<A>, name: Option<Arc<str>>) -> Self {
        let signal_mailbox = actor_ref.weak_signal_mailbox();
        let links = actor_ref.links.clone();
        Self {
            actor_ref: BoxRegisteredActorRef::Weak(Box::new(actor_ref)),
            name,
            signal_mailbox,
            links,
        }
    }

    pub(crate) fn downcast<A: Actor>(
        &self,
    ) -> Result<ActorRef<A>, DowncastRegsiteredActorRefError> {
        match &self.actor_ref {
            BoxRegisteredActorRef::Strong(any) => any
                .downcast_ref::<ActorRef<A>>()
                .ok_or(DowncastRegsiteredActorRefError::BadActorType)
                .cloned(),
            BoxRegisteredActorRef::Weak(any) => any
                .downcast_ref::<WeakActorRef<A>>()
                .ok_or(DowncastRegsiteredActorRefError::BadActorType)?
                .upgrade()
                .ok_or(DowncastRegsiteredActorRefError::ActorNotRunning),
        }
    }
}

pub(crate) enum DowncastRegsiteredActorRefError {
    BadActorType,
    ActorNotRunning,
}

impl<E> From<DowncastRegsiteredActorRefError> for RemoteSendError<E> {
    fn from(err: DowncastRegsiteredActorRefError) -> Self {
        match err {
            DowncastRegsiteredActorRefError::BadActorType => RemoteSendError::BadActorType,
            DowncastRegsiteredActorRefError::ActorNotRunning => RemoteSendError::ActorNotRunning,
        }
    }
}

pub(crate) enum BoxRegisteredActorRef {
    Strong(Box<dyn any::Any + Send + Sync>),
    Weak(Box<dyn any::Any + Send + Sync>),
}

/// `RemoteActor` is a trait for identifying actors remotely.
///
/// Each remote actor must implement this trait and provide a unique identifier string (`REMOTE_ID`).
/// The identifier is essential to distinguish between different actor types during remote communication.
///
/// ## Example with Derive
///
/// ```
/// use piying::{Actor, RemoteActor};
///
/// #[derive(Actor, RemoteActor)]
/// pub struct MyActor;
/// ```
///
/// ## Example Manual Implementation
///
/// ```
/// use piying::remote::RemoteActor;
///
/// pub struct MyActor;
///
/// impl RemoteActor for MyActor {
///     const REMOTE_ID: &'static str = "my_actor_id";
/// }
/// ```
pub trait RemoteActor {
    /// The remote identifier string.
    const REMOTE_ID: &'static str;
}

/// `RemoteMessage` is a trait for identifying messages that are sent between remote actors.
///
/// Each remote message type must implement this trait and provide a unique identifier string (`REMOTE_ID`).
/// The unique ID ensures that each message type is recognized correctly during message passing between nodes.
///
/// This trait is typically implemented automatically with the [`#[remote_message]`](crate::remote_message) macro.
pub trait RemoteMessage<M> {
    /// The remote identifier string.
    const REMOTE_ID: &'static str;
}

/// Bootstrap a simple actor swarm for local development.
///
/// This convenience function creates and runs a libp2p swarm with:
/// - TCP transport, plus QUIC with the `remote-quic` feature
/// - mDNS peer discovery (local network only) with the `remote-mdns` feature
/// - Automatic listening on an OS-assigned port
///
/// Requires the `remote-tcp` and `serde-codec` features (uses CBOR for transport encoding).
/// `remote-bootstrap` enables TCP, QUIC and mDNS together.
///
/// For production use or custom configuration, use `piying::remote::Behaviour`
/// with your own libp2p swarm setup.
///
/// # Example
/// ```ignore
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // One line to get started!
///     remote::bootstrap()?;
///
///     // Now use remote actors normally
///     let actor_ref = MyActor::spawn_default();
///     actor_ref.register("my_actor").await?;
///     Ok(())
/// }
/// ```
#[cfg(all(
    feature = "remote-tcp",
    feature = "serde-codec",
    not(feature = "rkyv-codec")
))]
pub fn bootstrap() -> Result<PeerId, Box<dyn error::Error>> {
    bootstrap_on("/ip4/0.0.0.0/tcp/0")
}

/// Bootstrap with a specific listen address.
///
/// Requires the `remote-tcp` and `serde-codec` features. A `/udp/.../quic-v1` address
/// needs `remote-quic`.
#[cfg(all(
    feature = "remote-tcp",
    feature = "serde-codec",
    not(feature = "rkyv-codec")
))]
// mdns and dummy handlers emit `Infallible`, which makes derive-generated match arms unreachable.
#[allow(unreachable_code)]
pub fn bootstrap_on(addr: &str) -> Result<PeerId, Box<dyn error::Error>> {
    #[cfg(feature = "remote-mdns")]
    use libp2p::mdns;
    #[cfg(any(feature = "remote-mdns", feature = "tracing"))]
    use libp2p::swarm::SwarmEvent;
    use libp2p::{SwarmBuilder, noise, tcp, yamux};

    #[cfg(feature = "remote-mdns")]
    type Discovery = mdns::tokio::Behaviour;
    #[cfg(not(feature = "remote-mdns"))]
    type Discovery = libp2p::swarm::dummy::Behaviour;

    #[derive(NetworkBehaviour)]
    struct BootstrapBehaviour {
        piying: Behaviour,
        mdns: Discovery,
    }

    let builder = SwarmBuilder::with_new_identity().with_tokio().with_tcp(
        tcp::Config::default(),
        noise::Config::new,
        yamux::Config::default,
    )?;
    #[cfg(feature = "remote-quic")]
    let builder = builder.with_quic();
    let mut swarm = builder
        .with_behaviour(|key| {
            let local_peer_id = key.public().to_peer_id();
            let piying = Behaviour::new(local_peer_id, messaging::Config::default());
            #[cfg(feature = "remote-mdns")]
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;
            #[cfg(not(feature = "remote-mdns"))]
            let mdns = libp2p::swarm::dummy::Behaviour;

            Ok(BootstrapBehaviour { piying, mdns })
        })?
        .build();

    swarm.behaviour().piying.try_init_global()?;

    swarm.listen_on(addr.parse()?)?;

    let local_peer_id = *swarm.local_peer_id();

    tokio::spawn(async move {
        loop {
            match swarm.select_next_some().await {
                #[cfg(feature = "remote-mdns")]
                SwarmEvent::Behaviour(BootstrapBehaviourEvent::Mdns(mdns::Event::Discovered(
                    list,
                ))) => {
                    for (peer_id, multiaddr) in list {
                        #[cfg(feature = "tracing")]
                        tracing::info!("mDNS discovered a new peer: {peer_id}");
                        swarm.add_peer_address(peer_id, multiaddr);
                    }
                }
                #[cfg(feature = "remote-mdns")]
                SwarmEvent::Behaviour(BootstrapBehaviourEvent::Mdns(mdns::Event::Expired(
                    list,
                ))) => {
                    for (peer_id, _multiaddr) in list {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("mDNS discover peer has expired: {peer_id}");
                        let _ = swarm.disconnect_peer_id(peer_id);
                    }
                }
                #[cfg(feature = "tracing")]
                SwarmEvent::NewListenAddr { address, .. } => {
                    tracing::info!("ActorSwarm listening on {address}");
                }
                _ => {}
            }
        }
    });

    Ok(local_peer_id)
}

/// Run a pre-built libp2p swarm as the actor swarm event loop.
///
/// This is the most flexible way to use piying's remote actors with custom
/// transports. You build the `Swarm` yourself (with any transport, encryption,
/// and multiplexing) and include [`Behaviour`] in your composed `NetworkBehaviour`.
///
/// # Prerequisites
///
/// Before calling this function, you must:
/// 1. Build a `Swarm` containing [`Behaviour`] in its `NetworkBehaviour`
/// 2. Call [`Behaviour::try_init_global()`] on the piying behaviour
/// 3. Call `swarm.listen_on(addr)` if you want the swarm to accept connections
///
/// # Example
///
/// ```ignore
/// use piying::remote::{self, codec::PiyingRkyvCodec};
/// use libp2p::{swarm::NetworkBehaviour, noise, tcp, yamux};
///
/// #[derive(NetworkBehaviour)]
/// struct MyBehaviour {
///     piying: remote::Behaviour<PiyingRkyvCodec>,
/// }
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut swarm = libp2p::SwarmBuilder::with_new_identity()
///     .with_tokio()
///     .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
///     .with_behaviour(|key| {
///         let peer_id = key.public().to_peer_id();
///         let config = remote::messaging::Config::default();
///         let codec = PiyingRkyvCodec::new(&config);
///         Ok(MyBehaviour {
///             piying: remote::Behaviour::with_codec(peer_id, config, codec),
///         })
///     })?
///     .build();
///
/// swarm.behaviour().piying.try_init_global()?;
/// swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
///
/// let peer_id = remote::run_swarm(swarm);
/// # Ok(())
/// # }
/// ```
pub fn run_swarm<B>(mut swarm: Swarm<B>) -> PeerId
where
    B: NetworkBehaviour + Send + 'static,
    <B as NetworkBehaviour>::ToSwarm: Send,
{
    let local_peer_id = *swarm.local_peer_id();

    tokio::spawn(async move {
        loop {
            let _event = swarm.select_next_some().await;
        }
    });

    local_peer_id
}

/// Clear all entries from the local actor registry.
///
/// Useful for test isolation when multiple tests run in the same process
/// and share the global registry. After clearing, any previously registered
/// actors will no longer be found by incoming remote messages or
/// [`RemoteActorRef::for_peer()`](crate::actor::RemoteActorRef::for_peer) lookups until they are re-registered.
pub async fn clear_registry() {
    lock_registry().clear();
}

/// Synchronous version of [`clear_registry`].
///
/// Intended for test harness cleanup that runs outside an async context.
pub fn clear_registry_sync() {
    lock_registry().clear();
}

/// Synchronous version of [`register_actor_local`].
///
/// Intended for pre-run hooks that execute outside an async context
/// (e.g., before the network actor starts blocking).
pub fn register_actor_local_sync<A: Actor>(actor_ref: &ActorRef<A>, well_known_id: ActorId) {
    lock_registry().insert(
        well_known_id,
        RemoteRegistryActorRef::new(actor_ref.clone(), None),
    );
}

/// Check whether an actor with the given ID is registered in the local registry.
///
/// Uses `try_lock` to avoid blocking. Returns `false` if the lock is
/// contended (conservative: caller should fall back to swarm routing).
pub fn is_registered_locally(actor_id: ActorId) -> bool {
    try_lock_registry().is_some_and(|registry| registry.contains_key(&actor_id))
}

/// Unregisters an actor within the swarm.
///
/// This will only unregister an actor previously registered by the current node.
#[cfg(feature = "remote-registry")]
pub async fn unregister(name: impl Into<Arc<str>>) -> Result<(), RegistryError> {
    ActorSwarm::get()
        .ok_or(RegistryError::SwarmNotBootstrapped)?
        .unregister(name.into())
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use super::*;

    /// Poisoning must not disable the registry: teardown paths call into it and
    /// a `.lock().unwrap()` here would turn one unrelated panic into a
    /// process-wide failure.
    #[test]
    fn registry_lock_recovers_from_poison() {
        let _ = std::thread::spawn(|| {
            let _guard = lock_registry();
            panic!("poison the registry");
        })
        .join();

        let id = ActorId::generate();
        // Other tests may hold the lock briefly; a poisoned lock must still be
        // acquirable once they release it.
        let recovered = (0..1000).any(|_| {
            try_lock_registry().is_some() || {
                std::thread::sleep(Duration::from_millis(1));
                false
            }
        });
        assert!(recovered, "try_lock must recover a poisoned lock");
        assert!(!is_registered_locally(id));
        assert!(!unregister_actor_local(&id));
        assert!(!lock_registry().contains_key(&id));
    }

    /// The blocking lock is only ever held for a short synchronous section, so
    /// the non-blocking helpers must report contention instead of waiting for
    /// it. A regression here stalls actor teardown behind any busy critical
    /// section.
    #[test]
    fn registry_try_lock_does_not_wait_for_the_lock() {
        let guard = lock_registry();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let id = ActorId::generate();
            tx.send((is_registered_locally(id), unregister_actor_local(&id)))
                .unwrap();
        });

        let (registered, removed) = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("try_lock helpers blocked on a held registry lock");
        assert!(!registered);
        assert!(!removed);
        drop(guard);
    }
}

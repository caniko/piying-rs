use std::task;

use either::Either;
use futures::{StreamExt, stream::FuturesUnordered};
use libp2p::{
    Multiaddr, PeerId,
    core::{Endpoint, transport::PortUse},
    swarm::{
        ConnectionClosed, ConnectionDenied, ConnectionHandler, ConnectionHandlerSelect,
        ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent, THandlerOutEvent,
        ToSwarm,
    },
};
use tokio::sync::mpsc;

use crate::error::{ActorStopReason, SwarmAlreadyBootstrappedError};

use super::{
    ActorSwarm, REMOTE_REGISTRY, RemoteRegistryActorRef, SwarmCommand, SwarmSender, messaging,
    registry,
};

/// A network behaviour that combines messaging and registry capabilities for remote actor communication.
///
/// This behaviour integrates Thespis's remote actor functionality with libp2p, providing:
/// - Actor registration and discovery through a Kademlia-based registry
/// - Remote message passing between actors across the network
/// - Automatic lifecycle management for remote connections
///
/// When the `serde-codec` feature is enabled, the codec type parameter `C` defaults
/// to CBOR. Otherwise, a custom codec must be specified explicitly via
/// [`Behaviour::with_codec`].
///
/// # Example
///
/// ```rust
/// use thespis::remote;
/// use libp2p::{swarm::NetworkBehaviour, PeerId};
///
/// #[derive(NetworkBehaviour)]
/// struct MyBehaviour {
///     thespis: remote::Behaviour,
///     // other behaviours...
/// }
///
/// let peer_id = PeerId::random();
/// let behaviour = remote::Behaviour::new(peer_id, remote::messaging::Config::default());
/// ```
#[allow(missing_debug_implementations)]
#[cfg(feature = "serde-codec")]
pub struct Behaviour<
    C: libp2p::request_response::Codec + Clone + Send + 'static = messaging::DefaultCodec,
> {
    /// Messaging behaviour for sending and receiving actor messages.
    pub messaging: messaging::Behaviour<C>,
    /// Registry behaviour for actor registration and discovery.
    pub registry: registry::Behaviour,
    local_peer_id: PeerId,
    cmd_tx: mpsc::UnboundedSender<SwarmCommand>,
    cmd_rx: mpsc::UnboundedReceiver<SwarmCommand>,
}

/// A network behaviour that combines messaging and registry capabilities for remote actor communication.
///
/// Supply a codec implementing [`libp2p::request_response::Codec`] via [`Behaviour::with_codec`].
/// Enable the `serde-codec` feature for a default CBOR codec.
#[allow(missing_debug_implementations)]
#[cfg(not(feature = "serde-codec"))]
pub struct Behaviour<C: libp2p::request_response::Codec + Clone + Send + 'static> {
    /// Messaging behaviour for sending and receiving actor messages.
    pub messaging: messaging::Behaviour<C>,
    /// Registry behaviour for actor registration and discovery.
    pub registry: registry::Behaviour,
    local_peer_id: PeerId,
    cmd_tx: mpsc::UnboundedSender<SwarmCommand>,
    cmd_rx: mpsc::UnboundedReceiver<SwarmCommand>,
}

#[cfg(feature = "serde-codec")]
impl Behaviour<messaging::DefaultCodec> {
    /// Creates a new remote behaviour with the default CBOR codec.
    ///
    /// # Arguments
    ///
    /// * `local_peer_id` - The peer ID of the local node
    /// * `messaging_config` - Configuration for the messaging subsystem
    ///
    /// # Example
    ///
    /// ```rust
    /// use thespis::remote;
    /// use libp2p::PeerId;
    ///
    /// let peer_id = PeerId::random();
    /// let config = remote::messaging::Config::default();
    /// let behaviour = remote::Behaviour::new(peer_id, config);
    /// ```
    pub fn new(local_peer_id: PeerId, messaging_config: messaging::Config) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let messaging = messaging::Behaviour::new(local_peer_id, messaging_config);
        let registry = registry::Behaviour::new(local_peer_id);

        Behaviour {
            messaging,
            registry,
            local_peer_id,
            cmd_tx,
            cmd_rx,
        }
    }
}

impl<C> Behaviour<C>
where
    C: libp2p::request_response::Codec<
            Protocol = libp2p::StreamProtocol,
            Request = messaging::SwarmRequest,
            Response = messaging::SwarmResponse,
        > + Clone
        + Send
        + 'static,
{
    /// Creates a new remote behaviour with a custom codec.
    pub fn with_codec(
        local_peer_id: PeerId,
        messaging_config: messaging::Config,
        codec: C,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let messaging = messaging::Behaviour::with_codec(local_peer_id, messaging_config, codec);
        let registry = registry::Behaviour::new(local_peer_id);

        Behaviour {
            messaging,
            registry,
            local_peer_id,
            cmd_tx,
            cmd_rx,
        }
    }

    /// Initializes the global actor swarm for this behaviour, panicking if its already been initialized.
    pub fn init_global(&self) {
        self.try_init_global().unwrap()
    }

    /// Initializes the global actor swarm for this behaviour, returning an error if its already been initialized.
    pub fn try_init_global(&self) -> Result<(), SwarmAlreadyBootstrappedError> {
        ActorSwarm::set(self.cmd_tx.clone(), self.local_peer_id)
            .map_err(|_| SwarmAlreadyBootstrappedError)?;
        Ok(())
    }

    /// Returns a clone of the swarm command sender.
    ///
    /// Use this to propagate the sender explicitly to consumers that need
    /// to create [`RemoteActorRef`](crate::actor::RemoteActorRef) instances
    /// without reading the global [`ActorSwarm`].
    pub fn swarm_sender(&self) -> SwarmSender {
        SwarmSender(self.cmd_tx.clone())
    }

    fn handle_command(&mut self, cmd: SwarmCommand) -> bool {
        match cmd {
            SwarmCommand::Lookup { name, reply } => {
                self.registry.lookup_with_reply(name, Some(reply));
                true // We triggered a get_providers on kad, we need to call the waker
            }
            SwarmCommand::LookupLocal { name, reply } => {
                let _ = reply.send(self.registry.lookup_local(&name));
                false // Local lookups don't need to be polled again
            }
            SwarmCommand::Register {
                name,
                registration,
                reply,
            } => match self
                .registry
                .register_with_reply(name, registration, Some(reply))
            {
                Ok(_) => true, // We started a new lookup
                Err((Some(reply), err)) => {
                    let _ = reply.send(Err(err.into()));
                    false
                }
                Err((None, _)) => unreachable!("we should have the reply type here"),
            },
            SwarmCommand::Unregister { name, reply } => {
                self.registry.unregister(&name);
                let _ = reply.send(());
                false // We only delete the actor from our local registry
            }
            SwarmCommand::Ask {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
                reply,
            } => {
                self.messaging.ask_with_reply(
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    reply_timeout,
                    immediate,
                    Some(reply),
                );
                true
            }
            SwarmCommand::Tell {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
                reply,
            } => {
                self.messaging.tell_with_reply(
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    immediate,
                    reply,
                );
                true
            }
            SwarmCommand::Link {
                actor_id,
                actor_remote_id,
                sibling_id,
                sibling_remote_id,
                reply,
            } => {
                self.messaging.link_with_reply(
                    actor_id,
                    actor_remote_id,
                    sibling_id,
                    sibling_remote_id,
                    Some(reply),
                );
                true
            }
            SwarmCommand::Unlink {
                actor_id,
                sibling_id,
                sibling_remote_id,
                reply,
            } => {
                self.messaging.unlink_with_reply(
                    sibling_id,
                    sibling_remote_id,
                    actor_id,
                    Some(reply),
                );
                true
            }
            SwarmCommand::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
                reply,
            } => {
                self.messaging.signal_link_died_with_reply(
                    dead_actor_id,
                    notified_actor_id,
                    notified_actor_remote_id,
                    stop_reason,
                    Some(reply),
                );
                true
            }
        }
    }
}

include!("behaviour/events_and_network.rs");

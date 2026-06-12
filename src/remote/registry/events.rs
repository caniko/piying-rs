/// The events produced by the `Registry` behaviour.
///
/// See [`NetworkBehaviour::poll`].
#[derive(Debug)]
pub enum Event {
    /// Progress in looking up actors by name.
    LookupProgressed {
        /// The original provider query ID.
        provider_query_id: kad::QueryId,
        /// The metadata query ID.
        get_query_id: kad::QueryId,
        /// The actor registration found, or an error.
        result: Result<ActorRegistration<'static>, RegistryError>,
    },

    /// A lookup operation timed out while searching for providers.
    /// More metadata might still arrive from providers found before timeout.
    LookupTimeout {
        /// The provider query ID that timed out.
        provider_query_id: kad::QueryId,
    },

    /// A lookup operation completed. This is always the final event for a lookup.
    LookupCompleted {
        /// The completed provider query ID.
        provider_query_id: kad::QueryId,
    },

    /// An actor registration failed.
    RegistrationFailed {
        /// The provider query ID that failed.
        provider_query_id: kad::QueryId,
        /// The error that caused the failure.
        error: RegistryError,
    },

    /// An actor was successfully registered.
    RegisteredActor {
        /// The result of the provider registration.
        provider_result: kad::AddProviderResult,
        /// The provider query ID.
        provider_query_id: kad::QueryId,
        /// The result of storing metadata.
        metadata_result: kad::PutRecordResult,
        /// The metadata query ID.
        metadata_query_id: kad::QueryId,
    },

    /// The routing table has been updated with a new peer and / or
    /// address, thereby possibly evicting another peer.
    RoutingUpdated {
        /// The ID of the peer that was added or updated.
        peer: PeerId,
        /// Whether this is a new peer and was thus just added to the routing
        /// table, or whether it is an existing peer who's addresses changed.
        is_new_peer: bool,
        /// The full list of known addresses of `peer`.
        addresses: kad::Addresses,
        /// Returns the minimum inclusive and maximum inclusive distance for
        /// the bucket of the peer.
        bucket_range: (kad::KBucketDistance, kad::KBucketDistance),
        /// The ID of the peer that was evicted from the routing table to make
        /// room for the new peer, if any.
        old_peer: Option<PeerId>,
    },

    /// A peer has connected for whom no listen address is known.
    UnroutablePeer {
        /// Peer ID.
        peer: PeerId,
    },

    /// A connection to a peer has been established for whom a listen address
    /// is known but the peer has not been added to the routing table.
    RoutablePeer {
        /// Peer ID.
        peer: PeerId,
        /// Address.
        address: Multiaddr,
    },

    /// A connection to a peer has been established for whom a listen address
    /// is known but the peer is only pending insertion into the routing table
    /// if the least-recently disconnected peer is unresponsive, i.e. the peer
    /// may not make it into the routing table.
    PendingRoutablePeer {
        /// Peer ID.
        peer: PeerId,
        /// Address.
        address: Multiaddr,
    },
}


/// `Behaviour` is a `NetworkBehaviour` that implements the thespis registry behaviour
/// on top of the Kademlia protocol.
#[allow(missing_debug_implementations)]
pub struct Behaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    local_peer_id: PeerId,
    pending_peers: HashMap<(PeerId, Multiaddr), Instant>,
    pending_events: VecDeque<Event>,
    registration_queries: HashMap<kad::QueryId, RegistrationQuery>,
    lookup_queries: HashMap<kad::QueryId, LookupQuery>,
}

include!("behaviour/public_methods.rs");
include!("behaviour/kademlia_events.rs");
include!("behaviour/validation.rs");

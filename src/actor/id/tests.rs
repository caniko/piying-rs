#[cfg(test)]
mod tests {
    use std::hash::{DefaultHasher, Hasher};

    #[cfg(feature = "remote")]
    use libp2p::PeerId;

    use super::*;

    #[cfg(feature = "remote")]
    fn local_peer_id() -> PeerId {
        PeerId::from_bytes(&[
            0, 32, 77, 249, 14, 119, 133, 11, 205, 96, 61, 232, 63, 206, 126, 234, 204, 60, 241,
            93, 2, 68, 130, 67, 3, 193, 242, 23, 80, 189, 82, 144, 152, 206,
        ])
        .unwrap()
    }

    #[test]
    fn test_actor_id_partial_eq_local() {
        let id1 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        assert_eq!(id1, id2);

        let id1 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 1,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        assert_ne!(id1, id2);
    }

    #[test]
    #[cfg(feature = "remote")]
    fn test_actor_id_partial_eq_remote() {
        // Unbootstrapped
        use tokio::sync::mpsc;

        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        assert_eq!(id1, id2);

        // Bootstrapped
        let local_peer_id = local_peer_id();
        let _guard = ActorSwarm::set_thread_local(mpsc::unbounded_channel().0, local_peer_id);
        assert_eq!(id1.peer_id(), Some(local_peer_id));
        assert_eq!(id2.peer_id(), Some(local_peer_id));

        // Bootstrapped local ids should equal
        assert_eq!(id1, id2);

        // Bootstrapped local and remote id pointing to local peer id should equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        assert_eq!(id1, id2);

        // Peer IDs should equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        assert_eq!(id1, id2);

        // Different peer IDs should not equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(PeerId::random()),
        };
        assert_ne!(id1, id2);
    }

    fn hashes_eq(id1: &ActorId, id2: &ActorId) -> bool {
        let mut hasher = DefaultHasher::new();
        id1.hash(&mut hasher);
        let id1_hash = hasher.finish();

        let mut hasher = DefaultHasher::new();
        id2.hash(&mut hasher);
        let id2_hash = hasher.finish();

        id1_hash == id2_hash
    }

    #[test]
    fn test_actor_id_hash_local() {
        let id1 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };

        assert!(hashes_eq(&id1, &id2));

        let id1 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 1,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };

        assert!(!hashes_eq(&id1, &id2));
    }

    #[test]
    #[cfg(feature = "remote")]
    fn test_actor_id_hash_remote() {
        // Unbootstrapped
        use tokio::sync::mpsc;

        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };

        assert!(hashes_eq(&id1, &id2));

        // Bootstrapped
        let local_peer_id = local_peer_id();
        let _guard = ActorSwarm::set_thread_local(mpsc::unbounded_channel().0, local_peer_id);
        assert_eq!(id1.peer_id(), Some(local_peer_id));
        assert_eq!(id2.peer_id(), Some(local_peer_id));

        // Bootstrapped local ids should equal
        assert_eq!(id1, id2);

        // Bootstrapped local and remote id pointing to local peer id should equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };

        assert!(hashes_eq(&id1, &id2));

        // Peer IDs should equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };

        assert!(hashes_eq(&id1, &id2));

        // Different peer IDs should not equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(PeerId::random()),
        };

        assert!(!hashes_eq(&id1, &id2));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    // ACTOR_SWARM is a process-wide singleton. Tests run concurrently (both
    // within this module and from id.rs), so any test that sets the global
    // can be immediately overwritten by another thread.
    //
    // Strategy:
    //  - Local-instance tests construct ActorSwarm directly and verify accessor
    //    methods without touching the global. These are fully deterministic.
    //  - Global tests only assert structural properties (Some after set, Ok
    //    return) — never specific PeerId values.

    fn make_swarm() -> (ActorSwarm, mpsc::UnboundedReceiver<SwarmCommand>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let swarm = ActorSwarm {
            swarm_tx: SwarmSender(tx),
            local_peer_id: PeerId::random(),
        };
        (swarm, rx)
    }

    // ── local instance: accessors ───────────────────────────────────

    #[test]
    fn local_peer_id_returns_stored_value() {
        let (swarm, _rx) = make_swarm();
        let pid = swarm.local_peer_id();
        // PeerId is Copy; reading it twice should be identical.
        assert_eq!(pid, swarm.local_peer_id());
    }

    #[test]
    fn sender_returns_functional_handle() {
        let (swarm, mut rx) = make_swarm();
        let sender = swarm.sender().clone();

        let (reply_tx, _reply_rx) = oneshot::channel();
        sender.send(SwarmCommand::Unregister {
            name: Arc::from("test"),
            reply: reply_tx,
        });

        let cmd = rx.try_recv().expect("sender should deliver the command");
        assert!(matches!(cmd, SwarmCommand::Unregister { .. }));
    }

    #[test]
    fn cloned_sender_delivers_commands() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sender = SwarmSender(tx);
        let cloned = sender.clone();

        let (reply_tx, _reply_rx) = oneshot::channel();
        cloned.send(SwarmCommand::Unregister {
            name: Arc::from("test-actor"),
            reply: reply_tx,
        });

        let cmd = rx
            .try_recv()
            .expect("cloned sender should deliver the command");
        assert!(matches!(cmd, SwarmCommand::Unregister { .. }));
    }

    // ── global: set() ───────────────────────────────────────────────

    #[test]
    fn set_always_succeeds() {
        // ArcSwap::store can never fail, unlike the old RwLock which could
        // poison. Verify the Result is always Ok.
        for _ in 0..5 {
            let (tx, _rx) = mpsc::unbounded_channel();
            assert!(ActorSwarm::set(tx, PeerId::random()).is_ok());
        }
    }

    // ── global: get() / with() return Some after set ────────────────

    #[test]
    fn get_returns_some_after_set() {
        let (tx, _rx) = mpsc::unbounded_channel();
        ActorSwarm::set(tx, PeerId::random()).unwrap();
        // Another test may overwrite the value, but the global will still
        // be Some (no test clears it).
        assert!(ActorSwarm::get().is_some());
    }

    #[test]
    fn with_returns_some_after_set() {
        let (tx, _rx) = mpsc::unbounded_channel();
        ActorSwarm::set(tx, PeerId::random()).unwrap();
        assert!(ActorSwarm::with(|_| ()).is_some());
    }

    #[test]
    fn with_propagates_closure_return_value() {
        let (tx, _rx) = mpsc::unbounded_channel();
        ActorSwarm::set(tx, PeerId::random()).unwrap();

        // The closure's return type is faithfully propagated.
        assert_eq!(ActorSwarm::with(|_| 42u64), Some(42));
        assert_eq!(ActorSwarm::with(|_| "hello"), Some("hello"));
    }

    #[test]
    fn with_and_get_both_return_some() {
        let (tx, _rx) = mpsc::unbounded_channel();
        ActorSwarm::set(tx, PeerId::random()).unwrap();

        // We can't assert the same PeerId across calls (concurrent tests
        // may overwrite), but both must be Some.
        assert!(ActorSwarm::with(|s| s.local_peer_id()).is_some());
        assert!(ActorSwarm::get().is_some());
    }

    // ── thread-local override ───────────────────────────────────────

    #[test]
    fn thread_local_overrides_global() {
        // Set a global swarm
        let (global_tx, _rx) = mpsc::unbounded_channel();
        let global_peer = PeerId::random();
        ActorSwarm::set(global_tx, global_peer).unwrap();

        // Set a thread-local override with a different peer
        let (local_tx, _rx) = mpsc::unbounded_channel();
        let local_peer = PeerId::random();
        let guard = ActorSwarm::set_thread_local(local_tx, local_peer);

        // Thread-local should take precedence
        assert_eq!(ActorSwarm::with(|s| s.local_peer_id()), Some(local_peer));
        assert_eq!(
            ActorSwarm::get().map(|s| s.local_peer_id()),
            Some(local_peer)
        );

        // Drop guard — should fall back to global
        drop(guard);
        // Global may have been overwritten by another test, but should still be Some
        assert!(ActorSwarm::get().is_some());
    }

    #[test]
    fn guard_drop_clears_thread_local() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let peer = PeerId::random();
        let guard = ActorSwarm::set_thread_local(tx, peer);

        // Thread-local is set
        assert_eq!(ActorSwarm::with(|s| s.local_peer_id()), Some(peer));

        // Drop guard
        drop(guard);

        // Thread-local should be cleared — get/with should not return
        // our peer (may return a global set by another test, or None)
        let current = ActorSwarm::with(|s| s.local_peer_id());
        assert_ne!(current, Some(peer));
    }
}

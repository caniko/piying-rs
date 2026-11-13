// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "rkyv-codec")]
mod rkyv_tests {
    use std::time::Duration;

    use crate::remote::codec::{Decode, Encode};
    use crate::remote::wire::{
        WireActorId, WireActorStopReason, WireRemoteSendError, WireStopReasonLeaf,
    };

    use super::{SwarmRequest, SwarmResponse};

    fn test_actor_id() -> WireActorId {
        WireActorId {
            sequence_id: 1,
            peer_id_bytes: vec![0xAB, 0xCD],
        }
    }

    #[test]
    fn swarm_request_ask_roundtrip() {
        let original = SwarmRequest::Ask {
            actor_id: test_actor_id(),
            actor_remote_id: "my_actor".into(),
            message_remote_id: "my_message".into(),
            payload: vec![1, 2, 3],
            mailbox_timeout: Some(Duration::from_secs(5)),
            reply_timeout: None,
            immediate: true,
        };
        let bytes = original.encode().unwrap();
        let decoded = SwarmRequest::decode(&bytes).unwrap();
        match decoded {
            SwarmRequest::Ask {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            } => {
                assert_eq!(actor_id.sequence_id, 1);
                assert_eq!(actor_remote_id, "my_actor");
                assert_eq!(message_remote_id, "my_message");
                assert_eq!(payload, vec![1, 2, 3]);
                assert_eq!(mailbox_timeout, Some(Duration::from_secs(5)));
                assert_eq!(reply_timeout, None);
                assert!(immediate);
            }
            other => panic!("expected Ask, got {other:?}"),
        }
    }

    #[test]
    fn swarm_request_tell_roundtrip() {
        let original = SwarmRequest::Tell {
            actor_id: test_actor_id(),
            actor_remote_id: "actor".into(),
            message_remote_id: "msg".into(),
            payload: vec![],
            mailbox_timeout: None,
            immediate: false,
        };
        let bytes = original.encode().unwrap();
        let decoded = SwarmRequest::decode(&bytes).unwrap();
        match decoded {
            SwarmRequest::Tell {
                mailbox_timeout,
                immediate,
                ..
            } => {
                assert_eq!(mailbox_timeout, None);
                assert!(!immediate);
            }
            other => panic!("expected Tell, got {other:?}"),
        }
    }

    #[test]
    fn swarm_request_link_roundtrip() {
        let original = SwarmRequest::Link {
            actor_id: WireActorId {
                sequence_id: 1,
                peer_id_bytes: vec![10],
            },
            actor_remote_id: "actor_a".into(),
            sibling_id: WireActorId {
                sequence_id: 2,
                peer_id_bytes: vec![20],
            },
            sibling_remote_id: "actor_b".into(),
        };
        let bytes = original.encode().unwrap();
        let decoded = SwarmRequest::decode(&bytes).unwrap();
        match decoded {
            SwarmRequest::Link {
                actor_id,
                sibling_id,
                sibling_remote_id,
                ..
            } => {
                assert_eq!(actor_id.sequence_id, 1);
                assert_eq!(sibling_id.sequence_id, 2);
                assert_eq!(sibling_remote_id, "actor_b");
            }
            other => panic!("expected Link, got {other:?}"),
        }
    }

    #[test]
    fn swarm_request_signal_link_died_roundtrip() {
        let original = SwarmRequest::SignalLinkDied {
            dead_actor_id: test_actor_id(),
            notified_actor_id: WireActorId {
                sequence_id: 99,
                peer_id_bytes: vec![0xFF],
            },
            notified_actor_remote_id: "notified".into(),
            stop_reason: WireActorStopReason {
                link_chain: vec![],
                leaf: WireStopReasonLeaf::Killed,
            },
        };
        let bytes = original.encode().unwrap();
        let decoded = SwarmRequest::decode(&bytes).unwrap();
        match decoded {
            SwarmRequest::SignalLinkDied {
                notified_actor_id,
                stop_reason,
                ..
            } => {
                assert_eq!(notified_actor_id.sequence_id, 99);
                assert!(matches!(stop_reason.leaf, WireStopReasonLeaf::Killed));
            }
            other => panic!("expected SignalLinkDied, got {other:?}"),
        }
    }

    #[test]
    fn swarm_response_ask_ok_roundtrip() {
        let original = SwarmResponse::Ask(Ok(vec![42, 43]));
        let bytes = original.encode().unwrap();
        let decoded = SwarmResponse::decode(&bytes).unwrap();
        match decoded {
            SwarmResponse::Ask(Ok(payload)) => assert_eq!(payload, vec![42, 43]),
            other => panic!("expected Ask(Ok), got {other:?}"),
        }
    }

    #[test]
    fn swarm_response_ask_err_roundtrip() {
        let original = SwarmResponse::Ask(Err(WireRemoteSendError::MailboxFull));
        let bytes = original.encode().unwrap();
        let decoded = SwarmResponse::decode(&bytes).unwrap();
        match decoded {
            SwarmResponse::Ask(Err(WireRemoteSendError::MailboxFull)) => {}
            other => panic!("expected Ask(Err(MailboxFull)), got {other:?}"),
        }
    }

    #[test]
    fn swarm_response_outbound_failure_roundtrip() {
        let original = SwarmResponse::OutboundFailure(WireRemoteSendError::NetworkTimeout);
        let bytes = original.encode().unwrap();
        let decoded = SwarmResponse::decode(&bytes).unwrap();
        match decoded {
            SwarmResponse::OutboundFailure(WireRemoteSendError::NetworkTimeout) => {}
            other => panic!("expected OutboundFailure(NetworkTimeout), got {other:?}"),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "rkyv-codec")]
mod pending_cap_tests {
    use std::task::Poll;

    use libp2p::{PeerId, swarm::NetworkBehaviour};
    use tokio::sync::oneshot;

    use super::{Behaviour, Config, SwarmResponse};
    use crate::actor::ActorId;
    use crate::remote::codec::PiyingRkyvCodec;
    use crate::remote::wire::WireRemoteSendError;

    #[tokio::test]
    async fn tell_past_pending_limit_fails_fast() {
        let config = Config::default().with_max_pending_requests(1);
        let mut behaviour =
            Behaviour::with_codec(PeerId::random(), config, PiyingRkyvCodec::new(&config));
        let remote = ActorId::new_with_peer_id(1, PeerId::random());
        let mut tell = |reply| {
            behaviour.tell_with_reply(remote, "a".into(), "m".into(), vec![], None, true, reply)
        };

        assert!(tell(None).is_some(), "first request is queued for the peer");
        let (tx, mut rx) = oneshot::channel();
        tell(Some(tx));
        assert_eq!(
            behaviour.requests.len(),
            1,
            "rejected request must not be tracked"
        );

        for _ in 0..100 {
            futures::future::poll_fn(|cx| {
                let _ = behaviour.poll(cx);
                Poll::Ready(())
            })
            .await;
            if let Ok(response) = rx.try_recv() {
                assert!(matches!(
                    response,
                    SwarmResponse::Tell(Err(WireRemoteSendError::MailboxFull))
                ));
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("rejected tell never got a reply");
    }

    #[tokio::test]
    async fn dial_failure_clears_only_requests_libp2p_dropped() {
        use libp2p::swarm::{
            ConnectionId, DialError, FromSwarm, behaviour::DialFailure, dial_opts::PeerCondition,
        };

        let config = Config::default();
        let mut behaviour =
            Behaviour::with_codec(PeerId::random(), config, PiyingRkyvCodec::new(&config));
        let target = PeerId::random();
        let remote = ActorId::new_with_peer_id(1, target);
        behaviour.tell_with_reply(remote, "a".into(), "m".into(), vec![], None, true, None);

        let mut fail_and_drain = |error: &DialError| {
            behaviour.on_swarm_event(FromSwarm::DialFailure(DialFailure {
                peer_id: Some(target),
                error,
                connection_id: ConnectionId::new_unchecked(0),
            }));
            let waker = futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            for _ in 0..10 {
                let _ = behaviour.poll(&mut cx);
            }
            behaviour.requests.len()
        };
        // libp2p keeps the request queued, so it must keep counting toward the cap.
        let redundant = DialError::DialPeerConditionFalse(PeerCondition::DisconnectedAndNotDialing);
        assert_eq!(fail_and_drain(&redundant), 1);
        // A real dial failure drops it in libp2p, which reports OutboundFailure.
        assert_eq!(fail_and_drain(&DialError::Aborted), 0);
    }

    #[tokio::test]
    async fn rejected_event_names_the_destination_peer() {
        let config = Config::default().with_max_pending_requests(0);
        let mut behaviour =
            Behaviour::with_codec(PeerId::random(), config, PiyingRkyvCodec::new(&config));
        let target = PeerId::random();
        let remote = ActorId::new_with_peer_id(1, target);
        let request_id = behaviour
            .tell_with_reply(remote, "a".into(), "m".into(), vec![], None, true, None)
            .expect("event-reply requests get an id");

        for _ in 0..100 {
            let polled = futures::future::poll_fn(|cx| Poll::Ready(behaviour.poll(cx))).await;
            if let Poll::Ready(libp2p::swarm::ToSwarm::GenerateEvent(event)) = polled {
                let super::Event::TellResult {
                    peer,
                    request_id: id,
                    result,
                    ..
                } = event
                else {
                    panic!("expected TellResult");
                };
                assert_eq!((peer, id), (target, request_id));
                assert!(result.is_err());
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("rejected tell never produced an event");
    }
}

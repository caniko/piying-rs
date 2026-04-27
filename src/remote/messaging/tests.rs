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

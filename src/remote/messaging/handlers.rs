impl Event {
    fn from_swarm_resp(
        resp: SwarmResponse,
        peer: PeerId,
        connection_id: Option<ConnectionId>,
        request_id: RequestId,
    ) -> Self {
        match resp {
            SwarmResponse::Ask(result) => Event::AskResult {
                peer,
                connection_id,
                request_id,
                result: result.map_err(|e| e.into_bytes_error()),
            },
            SwarmResponse::Tell(result) => Event::TellResult {
                peer,
                connection_id,
                request_id,
                result: result.map_err(|e| e.into_infallible()),
            },
            SwarmResponse::Link(result) => Event::LinkResult {
                peer,
                connection_id,
                request_id,
                result: result.map_err(|e| e.into_infallible()),
            },
            SwarmResponse::Unlink(result) => Event::UnlinkResult {
                peer,
                connection_id,
                request_id,
                result: result.map_err(|e| e.into_infallible()),
            },
            SwarmResponse::SignalLinkDied(result) => Event::SignalLinkDiedResult {
                peer,
                connection_id,
                request_id,
                result: result.map_err(|e| e.into_infallible()),
            },
            SwarmResponse::OutboundFailure(error) => Event::OutboundFailure {
                peer,
                connection_id: connection_id.unwrap(),
                request_id: request_id.unwrap_outbound(),
                error: error.into_infallible(),
            },
        }
    }
}

async fn ask(
    actor_id: ActorId,
    actor_remote_id: Cow<'static, str>,
    message_remote_id: Cow<'static, str>,
    payload: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>> {
    let Some(fns) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_remote_id: &actor_remote_id,
        message_remote_id: &message_remote_id,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        });
    };
    if immediate {
        (fns.try_ask)(actor_id, payload, reply_timeout).await
    } else {
        (fns.ask)(actor_id, payload, mailbox_timeout, reply_timeout).await
    }
}

async fn tell(
    actor_id: ActorId,
    actor_remote_id: Cow<'static, str>,
    message_remote_id: Cow<'static, str>,
    payload: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), RemoteSendError> {
    let Some(fns) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_remote_id: &actor_remote_id,
        message_remote_id: &message_remote_id,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        });
    };
    if immediate {
        (fns.try_tell)(actor_id, payload).await
    } else {
        (fns.tell)(actor_id, payload, mailbox_timeout).await
    }
}

async fn link(
    actor_id: ActorId,
    actor_remote_id: Cow<'static, str>,
    sibling_id: ActorId,
    sibling_remote_id: Cow<'static, str>,
) -> Result<(), RemoteSendError<Infallible>> {
    let Some(fns) = REMOTE_ACTORS_MAP.get(&*actor_remote_id) else {
        return Err(RemoteSendError::UnknownActor { actor_remote_id });
    };

    (fns.link)(actor_id, sibling_id, sibling_remote_id).await
}

async fn unlink(
    actor_id: ActorId,
    actor_remote_id: Cow<'static, str>,
    sibling_id: ActorId,
) -> Result<(), RemoteSendError<Infallible>> {
    let Some(fns) = REMOTE_ACTORS_MAP.get(&*actor_remote_id) else {
        return Err(RemoteSendError::UnknownActor { actor_remote_id });
    };

    (fns.unlink)(actor_id, sibling_id).await
}

async fn signal_link_died(
    dead_actor_id: ActorId,
    notified_actor_id: ActorId,
    notified_actor_remote_id: Cow<'static, str>,
    stop_reason: ActorStopReason,
) -> Result<(), RemoteSendError<Infallible>> {
    let Some(fns) = REMOTE_ACTORS_MAP.get(&*notified_actor_remote_id) else {
        return Err(RemoteSendError::UnknownActor {
            actor_remote_id: notified_actor_remote_id,
        });
    };

    (fns.signal_link_died)(dead_actor_id, notified_actor_id, stop_reason).await
}


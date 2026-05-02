impl<C> Behaviour<C>
where
    C: request_response::Codec<
            Protocol = StreamProtocol,
            Request = SwarmRequest,
            Response = SwarmResponse,
        > + Clone
        + Send
        + 'static,
{
    pub(super) fn signal_link_died_with_reply(
        &mut self,
        dead_actor_id: ActorId,
        notified_actor_id: ActorId,
        notified_actor_remote_id: Cow<'static, str>,
        stop_reason: ActorStopReason,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = notified_actor_id
            .peer_id()
            .expect("swarm should be bootstrapped");
        self.request_with_reply(
            &peer_id,
            reply,
            (
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
            ),
            |(dead_actor_id, notified_actor_id, notified_actor_remote_id, stop_reason)| {
                signal_link_died(
                    dead_actor_id,
                    notified_actor_id,
                    notified_actor_remote_id,
                    stop_reason,
                )
                .map(|r| {
                    SwarmResponse::SignalLinkDied(
                        r.map_err(|e| WireRemoteSendError::from_infallible(&e)),
                    )
                })
            },
            move |(dead_actor_id, notified_actor_id, notified_actor_remote_id, stop_reason)| {
                SwarmRequest::SignalLinkDied {
                    dead_actor_id: WireActorId::from_runtime(&dead_actor_id),
                    notified_actor_id: WireActorId::from_runtime(&notified_actor_id),
                    notified_actor_remote_id: notified_actor_remote_id.to_string(),
                    stop_reason: WireActorStopReason::from_runtime(&stop_reason),
                }
            },
        )
    }

    fn new_local_request_id(&mut self) -> RequestId {
        let id = RequestId::Local(self.next_id);
        self.next_id += 1;
        id
    }

    fn request_with_reply<L, LF, R, T>(
        &mut self,
        peer_id: &PeerId,
        reply: Option<oneshot::Sender<SwarmResponse>>,
        shared_data: T,
        local: L,
        remote: R,
    ) -> Option<RequestId>
    where
        L: FnOnce(T) -> LF,
        LF: Future<Output = SwarmResponse> + Send + 'static,
        R: FnOnce(T) -> SwarmRequest,
    {
        if peer_id == &self.local_peer_id {
            let (request_id, channel) = match reply {
                Some(tx) => (None, ReplyChannel::Local(tx)),
                None => {
                    let request_id = self.new_local_request_id();
                    (Some(request_id), ReplyChannel::Event(request_id))
                }
            };

            self.join_set
                .spawn(local(shared_data).map(|resp| (channel, resp)));

            request_id
        } else {
            let request = remote(shared_data);
            let summary = request.summary();
            let request_id = RequestId::Outbound(self.request_response.send_request(peer_id, request));
            self.requests.insert(
                request_id,
                RequestContext {
                    peer_id: *peer_id,
                    summary,
                    reply,
                },
            );

            Some(request_id)
        }
    }

    fn handle_request_response_event(
        &mut self,
        ev: request_response::Event<SwarmRequest, SwarmResponse>,
    ) -> (bool, Option<Event>) {
        match ev {
            // Incoming message
            request_response::Event::Message {
                peer,
                connection_id,
                message,
            } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    self.handle_incoming_request(request, channel);
                    (true, None)
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    let ev =
                        self.handle_incoming_response(peer, connection_id, request_id, response);
                    (false, ev)
                }
            },
            request_response::Event::OutboundFailure {
                peer,
                connection_id,
                request_id,
                error,
            } => {
                let remote_err: RemoteSendError = error.into();
                match self.requests.remove(&RequestId::Outbound(request_id)) {
                    Some(RequestContext {
                        summary,
                        reply: Some(tx),
                        ..
                    }) => {
                        #[cfg(not(feature = "tracing"))]
                        let _ = &summary;
                        #[cfg(feature = "tracing")]
                        tracing::warn!(%peer, %connection_id, %request_id, %summary, error = %remote_err, "thespis outbound request failed");
                        let _ = tx.send(SwarmResponse::OutboundFailure(
                            WireRemoteSendError::from_infallible(&remote_err),
                        ));
                        (false, None)
                    }
                    Some(RequestContext {
                        summary,
                        reply: None,
                        ..
                    }) => {
                        #[cfg(not(feature = "tracing"))]
                        let _ = &summary;
                        #[cfg(feature = "tracing")]
                        tracing::warn!(%peer, %connection_id, %request_id, %summary, error = %remote_err, "thespis outbound request failed");
                        (
                            false,
                            Some(Event::OutboundFailure {
                                peer,
                                connection_id,
                                request_id,
                                error: remote_err,
                            }),
                        )
                    }
                    None => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(%peer, %connection_id, %request_id, error = %remote_err, "thespis outbound request failed for unknown request context");
                        (
                            false,
                            Some(Event::OutboundFailure {
                                peer,
                                connection_id,
                                request_id,
                                error: remote_err,
                            }),
                        )
                    }
                }
            }
            request_response::Event::InboundFailure {
                peer,
                connection_id,
                request_id,
                error,
            } => (
                false,
                Some(Event::InboundFailure {
                    peer,
                    connection_id,
                    request_id,
                    error,
                }),
            ),
            request_response::Event::ResponseSent {
                peer,
                connection_id,
                request_id,
            } => (
                false,
                Some(Event::ResponseSent {
                    peer,
                    connection_id,
                    request_id,
                }),
            ),
        }
    }

    fn handle_incoming_request(
        &mut self,
        req: SwarmRequest,
        channel: request_response::ResponseChannel<SwarmResponse>,
    ) {
        let channel = ReplyChannel::Remote(channel);
        match req {
            SwarmRequest::Ask {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            } => {
                self.join_set.spawn(async move {
                    let Ok(actor_id) = actor_id.into_runtime() else {
                        return (
                            channel,
                            SwarmResponse::Ask(Err(WireRemoteSendError::ActorNotRunning)),
                        );
                    };
                    let r = ask(
                        actor_id,
                        Cow::Owned(actor_remote_id),
                        Cow::Owned(message_remote_id),
                        payload,
                        mailbox_timeout,
                        reply_timeout,
                        immediate,
                    )
                    .await;
                    (
                        channel,
                        SwarmResponse::Ask(
                            r.map_err(|e| WireRemoteSendError::from_bytes_error(&e)),
                        ),
                    )
                });
            }
            SwarmRequest::Tell {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            } => {
                self.join_set.spawn(async move {
                    let Ok(actor_id) = actor_id.into_runtime() else {
                        return (
                            channel,
                            SwarmResponse::Tell(Err(WireRemoteSendError::ActorNotRunning)),
                        );
                    };
                    let r = tell(
                        actor_id,
                        Cow::Owned(actor_remote_id),
                        Cow::Owned(message_remote_id),
                        payload,
                        mailbox_timeout,
                        immediate,
                    )
                    .await;
                    (
                        channel,
                        SwarmResponse::Tell(
                            r.map_err(|e| WireRemoteSendError::from_infallible(&e)),
                        ),
                    )
                });
            }
            SwarmRequest::Link {
                actor_id,
                actor_remote_id,
                sibling_id,
                sibling_remote_id,
            } => {
                self.join_set.spawn(async move {
                    let Ok(actor_id) = actor_id.into_runtime() else {
                        return (
                            channel,
                            SwarmResponse::Link(Err(WireRemoteSendError::ActorNotRunning)),
                        );
                    };
                    let Ok(sibling_id) = sibling_id.into_runtime() else {
                        return (
                            channel,
                            SwarmResponse::Link(Err(WireRemoteSendError::ActorNotRunning)),
                        );
                    };
                    let r = link(
                        actor_id,
                        Cow::Owned(actor_remote_id),
                        sibling_id,
                        Cow::Owned(sibling_remote_id),
                    )
                    .await;
                    (
                        channel,
                        SwarmResponse::Link(
                            r.map_err(|e| WireRemoteSendError::from_infallible(&e)),
                        ),
                    )
                });
            }
            SwarmRequest::Unlink {
                actor_id,
                actor_remote_id,
                sibling_id,
            } => {
                self.join_set.spawn(async move {
                    let Ok(actor_id) = actor_id.into_runtime() else {
                        return (
                            channel,
                            SwarmResponse::Unlink(Err(WireRemoteSendError::ActorNotRunning)),
                        );
                    };
                    let Ok(sibling_id) = sibling_id.into_runtime() else {
                        return (
                            channel,
                            SwarmResponse::Unlink(Err(WireRemoteSendError::ActorNotRunning)),
                        );
                    };
                    let r = unlink(actor_id, Cow::Owned(actor_remote_id), sibling_id).await;
                    (
                        channel,
                        SwarmResponse::Unlink(
                            r.map_err(|e| WireRemoteSendError::from_infallible(&e)),
                        ),
                    )
                });
            }
            SwarmRequest::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
            } => {
                self.join_set.spawn(async move {
                    let Ok(dead_actor_id) = dead_actor_id.into_runtime() else {
                        return (
                            channel,
                            SwarmResponse::SignalLinkDied(Err(
                                WireRemoteSendError::ActorNotRunning,
                            )),
                        );
                    };
                    let Ok(notified_actor_id) = notified_actor_id.into_runtime() else {
                        return (
                            channel,
                            SwarmResponse::SignalLinkDied(Err(
                                WireRemoteSendError::ActorNotRunning,
                            )),
                        );
                    };
                    let Ok(stop_reason) = stop_reason.into_runtime() else {
                        return (
                            channel,
                            SwarmResponse::SignalLinkDied(Err(
                                WireRemoteSendError::ActorNotRunning,
                            )),
                        );
                    };
                    let r = signal_link_died(
                        dead_actor_id,
                        notified_actor_id,
                        Cow::Owned(notified_actor_remote_id),
                        stop_reason,
                    )
                    .await;
                    (
                        channel,
                        SwarmResponse::SignalLinkDied(
                            r.map_err(|e| WireRemoteSendError::from_infallible(&e)),
                        ),
                    )
                });
            }
        }
    }

    fn handle_incoming_response(
        &mut self,
        peer: PeerId,
        connection_id: ConnectionId,
        req_id: request_response::OutboundRequestId,
        res: SwarmResponse,
    ) -> Option<Event> {
        match self.requests.remove(&RequestId::Outbound(req_id)) {
            Some(RequestContext {
                summary,
                reply: Some(tx),
                ..
            }) => {
                #[cfg(not(feature = "tracing"))]
                let _ = &summary;
                #[cfg(feature = "tracing")]
                tracing::trace!(%peer, %connection_id, %req_id, %summary, response = %res.summary(), "thespis response received");
                // Reply to channel
                let _ = tx.send(res);
                None
            }
            Some(RequestContext {
                summary,
                reply: None,
                ..
            }) => {
                #[cfg(not(feature = "tracing"))]
                let _ = &summary;
                #[cfg(feature = "tracing")]
                tracing::trace!(%peer, %connection_id, %req_id, %summary, response = %res.summary(), "thespis response received");
                // Emit event
                Some(Event::from_swarm_resp(
                    res,
                    peer,
                    Some(connection_id),
                    RequestId::Outbound(req_id),
                ))
            }
            None => {
                // Unrecognized request id
                #[cfg(feature = "tracing")]
                tracing::warn!(%peer, %connection_id, %req_id, ?res, "unrecognised request id for response");
                None
            }
        }
    }
}

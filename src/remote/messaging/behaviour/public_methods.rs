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
    /// Creates a new messaging behaviour with a custom codec.
    pub fn with_codec(local_peer_id: PeerId, config: Config, codec: C) -> Self {
        let request_response = request_response::Behaviour::with_codec(
            codec,
            [(proto_name(), request_response::ProtocolSupport::Full)],
            config.into(),
        );

        Behaviour {
            request_response,
            local_peer_id,
            next_id: 0,
            requests: HashMap::new(),
            join_set: JoinSet::new(),
        }
    }

    /// Sends an ask request to a remote actor.
    ///
    /// This is a low-level method that sends a request expecting a reply and
    /// generates events. Use `RemoteActorRef::ask` for higher-level messaging
    /// that doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The target actor's ID
    /// * `actor_remote_id` - The target actor's remote type ID
    /// * `message_remote_id` - The message's remote type ID
    /// * `payload` - The serialized message payload
    /// * `mailbox_timeout` - Optional timeout for mailbox delivery
    /// * `reply_timeout` - Optional timeout for receiving a reply
    /// * `immediate` - Whether to fail if the mailbox is full
    ///
    /// # Returns
    ///
    /// The request ID for tracking the ask progress.
    #[allow(clippy::too_many_arguments)]
    pub fn ask(
        &mut self,
        // Actor ID.
        actor_id: ActorId,
        // Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        // Message remote ID.
        message_remote_id: Cow<'static, str>,
        // Payload.
        payload: Vec<u8>,
        // Mailbox timeout.
        mailbox_timeout: Option<Duration>,
        // Reply timeout.
        reply_timeout: Option<Duration>,
        // Fail if mailbox is full.
        immediate: bool,
    ) -> RequestId {
        self.ask_with_reply(
            actor_id,
            actor_remote_id,
            message_remote_id,
            payload,
            mailbox_timeout,
            reply_timeout,
            immediate,
            None,
        )
        .unwrap()
    }

    /// Sends a tell message to a remote actor.
    ///
    /// This is a low-level method that sends a one-way message and generates
    /// events. Use `RemoteActorRef::tell` for higher-level messaging that
    /// doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The target actor's ID
    /// * `actor_remote_id` - The target actor's remote type ID
    /// * `message_remote_id` - The message's remote type ID
    /// * `payload` - The serialized message payload
    /// * `mailbox_timeout` - Optional timeout for mailbox delivery
    /// * `immediate` - Whether to fail if the mailbox is full
    ///
    /// # Returns
    ///
    /// The request ID for tracking the tell progress.
    pub fn tell(
        &mut self,
        // Actor ID.
        actor_id: ActorId,
        // Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        // Message remote ID.
        message_remote_id: Cow<'static, str>,
        // Payload.
        payload: Vec<u8>,
        // Mailbox timeout.
        mailbox_timeout: Option<Duration>,
        // Fail if mailbox is full.
        immediate: bool,
    ) -> RequestId {
        self.tell_with_reply(
            actor_id,
            actor_remote_id,
            message_remote_id,
            payload,
            mailbox_timeout,
            immediate,
            None,
        )
        .unwrap()
    }

    /// Creates a link between two actors across the network.
    ///
    /// This is a low-level method that establishes supervision relationships
    /// and generates events. Use `ActorRef::link` for higher-level linking
    /// that doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The first actor's ID
    /// * `actor_remote_id` - The first actor's remote type ID
    /// * `sibling_id` - The second actor's ID to link with
    /// * `sibling_remote_id` - The second actor's remote type ID
    ///
    /// # Returns
    ///
    /// The request ID for tracking the link progress.
    pub fn link(
        &mut self,
        // Actor A ID.
        actor_id: ActorId,
        // Actor A remote ID.
        actor_remote_id: Cow<'static, str>,
        // Actor B ID.
        sibling_id: ActorId,
        // Actor B remote ID.
        sibling_remote_id: Cow<'static, str>,
    ) -> RequestId {
        self.link_with_reply(
            actor_id,
            actor_remote_id,
            sibling_id,
            sibling_remote_id,
            None,
        )
        .unwrap()
    }

    /// Removes a link between two actors across the network.
    ///
    /// This is a low-level method that removes supervision relationships
    /// and generates events. Use `ActorRef::unlink` for higher-level unlinking
    /// that doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The first actor's ID
    /// * `actor_remote_id` - The first actor's remote type ID
    /// * `sibling_id` - The second actor's ID to unlink from
    ///
    /// # Returns
    ///
    /// The request ID for tracking the unlink progress.
    pub fn unlink(
        &mut self,
        // Actor ID.
        actor_id: ActorId,
        // Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        // Sibling ID.
        sibling_id: ActorId,
    ) -> RequestId {
        self.unlink_with_reply(actor_id, actor_remote_id, sibling_id, None)
            .unwrap()
    }

    /// Signals that a linked actor has died to another actor.
    ///
    /// This is a low-level method that notifies actors of link failures
    /// and generates events. This is typically called automatically by
    /// the actor system when links are broken.
    ///
    /// # Arguments
    ///
    /// * `dead_actor_id` - The ID of the actor that died
    /// * `notified_actor_id` - The ID of the actor to notify
    /// * `notified_actor_remote_id` - The remote type ID of the actor to notify
    /// * `stop_reason` - The reason the actor stopped
    ///
    /// # Returns
    ///
    /// The request ID for tracking the signal progress.
    pub fn signal_link_died(
        &mut self,
        // The actor which died.
        dead_actor_id: ActorId,
        // The actor to notify.
        notified_actor_id: ActorId,
        // Actor remote iD
        notified_actor_remote_id: Cow<'static, str>,
        // The reason the actor died.
        stop_reason: ActorStopReason,
    ) -> RequestId {
        self.signal_link_died_with_reply(
            dead_actor_id,
            notified_actor_id,
            notified_actor_remote_id,
            stop_reason,
            None,
        )
        .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn ask_with_reply(
        &mut self,
        actor_id: ActorId,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = actor_id.peer_id().expect("swarm should be bootstrapped");
        self.request_with_reply(
            &peer_id,
            reply,
            (
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            ),
            |(
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            )| {
                ask(
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    reply_timeout,
                    immediate,
                )
                .map(|r| {
                    SwarmResponse::Ask(r.map_err(|e| WireRemoteSendError::from_bytes_error(&e)))
                })
            },
            move |(
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            )| SwarmRequest::Ask {
                actor_id: WireActorId::from_runtime(&actor_id),
                actor_remote_id: actor_remote_id.to_string(),
                message_remote_id: message_remote_id.to_string(),
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tell_with_reply(
        &mut self,
        actor_id: ActorId,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = actor_id.peer_id().expect("swarm should be bootstrapped");
        self.request_with_reply(
            &peer_id,
            reply,
            (
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            ),
            |(
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            )| {
                tell(
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    immediate,
                )
                .map(|r| {
                    SwarmResponse::Tell(r.map_err(|e| WireRemoteSendError::from_infallible(&e)))
                })
            },
            move |(
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            )| SwarmRequest::Tell {
                actor_id: WireActorId::from_runtime(&actor_id),
                actor_remote_id: actor_remote_id.to_string(),
                message_remote_id: message_remote_id.to_string(),
                payload,
                mailbox_timeout,
                immediate,
            },
        )
    }

    pub(super) fn link_with_reply(
        &mut self,
        actor_id: ActorId,
        actor_remote_id: Cow<'static, str>,
        sibling_id: ActorId,
        sibling_remote_id: Cow<'static, str>,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = actor_id.peer_id().expect("swarm should be bootstrapped");
        self.request_with_reply(
            &peer_id,
            reply,
            (actor_id, actor_remote_id, sibling_id, sibling_remote_id),
            |(actor_id, actor_remote_id, sibling_id, sibling_remote_id)| {
                link(actor_id, actor_remote_id, sibling_id, sibling_remote_id).map(|r| {
                    SwarmResponse::Link(r.map_err(|e| WireRemoteSendError::from_infallible(&e)))
                })
            },
            move |(actor_id, actor_remote_id, sibling_id, sibling_remote_id)| SwarmRequest::Link {
                actor_id: WireActorId::from_runtime(&actor_id),
                actor_remote_id: actor_remote_id.to_string(),
                sibling_id: WireActorId::from_runtime(&sibling_id),
                sibling_remote_id: sibling_remote_id.to_string(),
            },
        )
    }

    pub(super) fn unlink_with_reply(
        &mut self,
        actor_id: ActorId,
        actor_remote_id: Cow<'static, str>,
        sibling_id: ActorId,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = actor_id.peer_id().expect("swarm should be bootstrapped");
        self.request_with_reply(
            &peer_id,
            reply,
            (actor_id, actor_remote_id, sibling_id),
            |(actor_id, actor_remote_id, sibling_id)| {
                unlink(actor_id, actor_remote_id, sibling_id).map(|r| {
                    SwarmResponse::Unlink(r.map_err(|e| WireRemoteSendError::from_infallible(&e)))
                })
            },
            move |(actor_id, actor_remote_id, sibling_id)| SwarmRequest::Unlink {
                actor_id: WireActorId::from_runtime(&actor_id),
                actor_remote_id: actor_remote_id.to_string(),
                sibling_id: WireActorId::from_runtime(&sibling_id),
            },
        )
    }
}

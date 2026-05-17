/// The configuration for a `messaging::Behaviour` protocol.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    request_timeout: Duration,
    max_concurrent_streams: usize,
    request_size_maximum: u64,
    response_size_maximum: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(10),
            max_concurrent_streams: 1024,
            request_size_maximum: 1024 * 1024,
            response_size_maximum: 10 * 1024 * 1024,
        }
    }
}

impl Config {
    /// Sets the timeout for inbound and outbound requests.
    pub fn with_request_timeout(mut self, v: Duration) -> Self {
        self.request_timeout = v;
        self
    }

    /// Sets the upper bound for the number of concurrent inbound + outbound streams.
    pub fn with_max_concurrent_streams(mut self, num_streams: usize) -> Self {
        self.max_concurrent_streams = num_streams;
        self
    }

    /// Sets the limit for request size in bytes.
    pub fn with_request_size_maximum(mut self, bytes: u64) -> Self {
        self.request_size_maximum = bytes;
        self
    }

    /// Sets the limit for response size in bytes.
    pub fn with_response_size_maximum(mut self, bytes: u64) -> Self {
        self.response_size_maximum = bytes;
        self
    }

    /// Returns the request size limit in bytes.
    pub fn request_size_maximum(&self) -> u64 {
        self.request_size_maximum
    }

    /// Returns the response size limit in bytes.
    pub fn response_size_maximum(&self) -> u64 {
        self.response_size_maximum
    }
}

impl From<Config> for request_response::Config {
    fn from(config: Config) -> Self {
        request_response::Config::default()
            .with_request_timeout(config.request_timeout)
            .with_max_concurrent_streams(config.max_concurrent_streams)
    }
}

#[cfg(feature = "serde-codec")]
impl<Req, Resp> From<Config> for request_response::cbor::codec::Codec<Req, Resp> {
    fn from(config: Config) -> Self {
        request_response::cbor::codec::Codec::default()
            .set_request_size_maximum(config.request_size_maximum)
            .set_response_size_maximum(config.response_size_maximum)
    }
}

/// The default codec used when no custom codec is specified.
///
/// Only available with the `serde-codec` feature.
#[cfg(feature = "serde-codec")]
pub type DefaultCodec = request_response::cbor::codec::Codec<SwarmRequest, SwarmResponse>;

/// `Behaviour` is a `NetworkBehaviour` that implements the thespis messaging behaviour
/// on top of the request response protocol.
///
/// When the `serde-codec` feature is enabled, the codec type parameter `C` defaults
/// to CBOR (via libp2p's built-in serde codec). Otherwise, a custom codec must be
/// specified explicitly via [`Behaviour::with_codec`].
#[allow(missing_debug_implementations)]
#[cfg(feature = "serde-codec")]
pub struct Behaviour<C: request_response::Codec + Clone + Send + 'static = DefaultCodec> {
    request_response: request_response::Behaviour<C>,
    local_peer_id: PeerId,
    next_id: u64,
    requests: HashMap<RequestId, RequestContext>,
    join_set: JoinSet<(ReplyChannel, SwarmResponse)>,
}

/// `Behaviour` is a `NetworkBehaviour` that implements the thespis messaging behaviour
/// on top of the request response protocol.
///
/// Supply a codec implementing [`request_response::Codec`] via [`Behaviour::with_codec`].
/// Enable the `serde-codec` feature for a default CBOR codec.
#[allow(missing_debug_implementations)]
#[cfg(not(feature = "serde-codec"))]
pub struct Behaviour<C: request_response::Codec + Clone + Send + 'static> {
    request_response: request_response::Behaviour<C>,
    local_peer_id: PeerId,
    next_id: u64,
    requests: HashMap<RequestId, RequestContext>,
    join_set: JoinSet<(ReplyChannel, SwarmResponse)>,
}

struct RequestContext {
    peer_id: PeerId,
    summary: String,
    reply: Option<oneshot::Sender<SwarmResponse>>,
}

#[cfg(feature = "serde-codec")]
impl Behaviour<DefaultCodec> {
    /// Creates a new messaging behaviour with the default CBOR codec.
    pub fn new(local_peer_id: PeerId, config: Config) -> Self {
        let codec: DefaultCodec = config.into();
        Self::with_codec(local_peer_id, config, codec)
    }
}

include!("behaviour/public_methods.rs");
include!("behaviour/request_handlers.rs");
impl<C> NetworkBehaviour for Behaviour<C>
where
    C: request_response::Codec<
            Protocol = StreamProtocol,
            Request = SwarmRequest,
            Response = SwarmResponse,
        > + Clone
        + Send
        + 'static,
{
    type ConnectionHandler = THandler<request_response::Behaviour<C>>;
    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: libp2p::PeerId,
        local_addr: &libp2p::Multiaddr,
        remote_addr: &libp2p::Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.request_response.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: libp2p::PeerId,
        addr: &libp2p::Multiaddr,
        role_override: libp2p::core::Endpoint,
        port_use: libp2p::core::transport::PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.request_response
            .handle_established_outbound_connection(
                connection_id,
                peer,
                addr,
                role_override,
                port_use,
            )
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        if let FromSwarm::DialFailure(DialFailure {
            peer_id: Some(peer_id),
            ..
        }) = event
        {
            // Remove requests for this peer id
            let dead_requests = self
                .requests
                .extract_if(|_, context| context.peer_id == peer_id);
            for (request_id, context) in dead_requests {
                #[cfg(not(feature = "tracing"))]
                let _ = request_id;
                #[cfg(feature = "tracing")]
                tracing::warn!(%peer_id, %request_id, summary = %context.summary, "thespis dial failure for pending request");
                if let Some(tx) = context.reply {
                    let _ = tx.send(SwarmResponse::OutboundFailure(
                        WireRemoteSendError::DialFailure,
                    ));
                }
            }
        }

        self.request_response.on_swarm_event(event)
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.request_response
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn poll(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        loop {
            // First, check for completed futures from join_set
            match self.join_set.poll_join_next(cx) {
                task::Poll::Ready(Some(Ok((ch, res)))) => {
                    match ch {
                        ReplyChannel::Event(request_id) => {
                            // We have an event to return immediately
                            return task::Poll::Ready(ToSwarm::GenerateEvent(
                                Event::from_swarm_resp(res, self.local_peer_id, None, request_id),
                            ));
                        }
                        ReplyChannel::Local(tx) => {
                            let _ = tx.send(res);
                            // Continue loop - no event to return but might have more work
                            continue;
                        }
                        ReplyChannel::Remote(ch) => {
                            let _ = self.request_response.send_response(ch, res);
                            // Continue loop - sending response might trigger more request_response events
                            continue;
                        }
                    }
                }
                task::Poll::Ready(Some(Err(err))) => {
                    panic!("ask request futures should never fail: {err}");
                }
                task::Poll::Ready(None) => {
                    // No completed futures, continue to poll request_response
                }
                task::Poll::Pending => {
                    // No completed futures ready, continue to poll request_response
                }
            }

            // Poll request_response for events
            match self.request_response.poll(cx) {
                task::Poll::Ready(ToSwarm::GenerateEvent(ev)) => {
                    let (wake, ev) = self.handle_request_response_event(ev);

                    if let Some(ev) = ev {
                        // We have an event to return
                        if wake {
                            cx.waker().wake_by_ref();
                        }
                        return task::Poll::Ready(ToSwarm::GenerateEvent(ev));
                    }

                    // No event to return, but if wake was requested, continue loop
                    // to check for more request_response events or join_set completions
                    if wake {
                        continue;
                    }

                    // No event and no wake needed, continue loop to check for more
                    // request_response events (in case it has more queued)
                    continue;
                }
                task::Poll::Ready(other_ev) => {
                    // Non-GenerateEvent from request_response
                    return task::Poll::Ready(
                        other_ev.map_out(|_| unreachable!("we handled GenerateEvent above")),
                    );
                }
                task::Poll::Pending => {
                    // request_response has no more work ready
                    // Check one more time if join_set has completions
                    // (in case something completed while we were processing request_response events)
                    match self.join_set.poll_join_next(cx) {
                        task::Poll::Ready(Some(Ok((ch, res)))) => match ch {
                            ReplyChannel::Event(request_id) => {
                                return task::Poll::Ready(ToSwarm::GenerateEvent(
                                    Event::from_swarm_resp(
                                        res,
                                        self.local_peer_id,
                                        None,
                                        request_id,
                                    ),
                                ));
                            }
                            ReplyChannel::Local(tx) => {
                                let _ = tx.send(res);
                                continue; // Might have triggered more work
                            }
                            ReplyChannel::Remote(ch) => {
                                let _ = self.request_response.send_response(ch, res);
                                continue; // Might have triggered more request_response events
                            }
                        },
                        task::Poll::Ready(Some(Err(err))) => {
                            panic!("ask request futures should never fail: {err}");
                        }
                        task::Poll::Ready(None) | task::Poll::Pending => {
                            // Nothing more ready from either source
                            return task::Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

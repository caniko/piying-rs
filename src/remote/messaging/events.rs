/// The events produced by the `Messaging` behaviour.
///
/// See [`NetworkBehaviour::poll`].
#[derive(Debug)]
pub enum Event {
    /// Result of an ask request to a remote actor.
    AskResult {
        /// The peer that handled the request.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the ask operation.
        result: AskResult,
    },

    /// Result of a tell message to a remote actor.
    TellResult {
        /// The peer that handled the message.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the tell operation.
        result: TellResult,
    },

    /// Result of a link operation between actors.
    LinkResult {
        /// The peer that handled the link.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the link operation.
        result: LinkResult,
    },

    /// Result of an unlink operation between actors.
    UnlinkResult {
        /// The peer that handled the unlink.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the unlink operation.
        result: UnlinkResult,
    },

    /// Result of signaling that a linked actor died.
    SignalLinkDiedResult {
        /// The peer that handled the signal.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the signal operation.
        result: SignalLinkDiedResult,
    },

    /// An outbound request failed.
    OutboundFailure {
        /// The peer to whom the request was sent.
        peer: PeerId,
        /// The connection used.
        connection_id: ConnectionId,
        /// The (local) ID of the failed request.
        request_id: request_response::OutboundRequestId,
        /// The error that occurred.
        error: RemoteSendError,
    },

    /// An inbound request failed.
    InboundFailure {
        /// The peer from whom the request was received.
        peer: PeerId,
        /// The connection used.
        connection_id: ConnectionId,
        /// The ID of the failed inbound request.
        request_id: request_response::InboundRequestId,
        /// The error that occurred.
        error: request_response::InboundFailure,
    },

    /// A response to an inbound request has been sent.
    ///
    /// When this event is received, the response has been flushed on
    /// the underlying transport connection.
    ResponseSent {
        /// The peer to whom the response was sent.
        peer: PeerId,
        /// The connection used.
        connection_id: ConnectionId,
        /// The ID of the inbound request whose response was sent.
        request_id: request_response::InboundRequestId,
    },
}


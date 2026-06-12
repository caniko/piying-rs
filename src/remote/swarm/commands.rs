/// RAII guard that clears the thread-local [`ActorSwarm`] on drop.
///
/// Not `Send` — the guard must be dropped on the same thread that created it.
#[derive(Debug)]
pub struct SwarmGuard {
    _not_send: PhantomData<*const ()>,
}

impl Drop for SwarmGuard {
    fn drop(&mut self) {
        THREAD_LOCAL_SWARM.with(|tls| *tls.borrow_mut() = None);
    }
}

/// A clonable handle for sending commands to the swarm event loop.
#[derive(Clone, Debug)]
pub struct SwarmSender(pub(crate) mpsc::UnboundedSender<SwarmCommand>);

impl SwarmSender {
    pub(crate) fn send(&self, cmd: SwarmCommand) {
        if self.0.send(cmd).is_err() {
            #[cfg(feature = "tracing")]
            tracing::warn!("SwarmSender: swarm channel closed (likely test isolation)");
        }
    }

    fn send_with_reply<T>(
        &self,
        cmd_fn: impl FnOnce(oneshot::Sender<T>) -> SwarmCommand,
    ) -> SwarmFuture<T> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = cmd_fn(reply_tx);
        self.send(cmd);

        SwarmFuture(reply_rx)
    }
}

/// A swarm command.
#[derive(Debug)]
pub(crate) enum SwarmCommand {
    /// Lookup providers for an actor by name in the kademlia network.
    Lookup {
        /// Registered name.
        name: Arc<str>,
        /// Reply sender.
        reply: LookupReply,
    },
    /// Lookup an actor by name on the local node only.
    LookupLocal {
        /// Actor name.
        name: Arc<str>,
        /// Reply sender.
        reply: LookupLocalReply,
    },
    /// Register an actor under a name.
    Register {
        /// Actor name.
        name: Arc<str>,
        /// Registration information.
        registration: ActorRegistration<'static>,
        /// Reply sender.
        reply: RegisterReply,
    },
    /// Stop providing a key.
    Unregister {
        /// Actor name.
        name: Arc<str>,
        /// Reply sender.
        reply: UnregisterReply,
    },
    /// An actor ask request.
    Ask {
        /// Actor ID.
        actor_id: ActorId,
        /// Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        /// Message remote ID.
        message_remote_id: Cow<'static, str>,
        /// Payload.
        payload: Vec<u8>,
        /// Mailbox timeout.
        mailbox_timeout: Option<Duration>,
        /// Reply timeout.
        reply_timeout: Option<Duration>,
        /// Fail if mailbox is full.
        immediate: bool,
        /// Reply sender.
        reply: oneshot::Sender<SwarmResponse>,
    },
    /// An actor tell request.
    Tell {
        /// Actor ID.
        actor_id: ActorId,
        /// Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        /// Message remote ID.
        message_remote_id: Cow<'static, str>,
        /// Payload.
        payload: Vec<u8>,
        /// Mailbox timeout.
        mailbox_timeout: Option<Duration>,
        /// Fail if mailbox is full.
        immediate: bool,
        /// Reply sender.
        reply: Option<oneshot::Sender<SwarmResponse>>,
    },
    /// An actor link request.
    Link {
        /// Actor A ID.
        actor_id: ActorId,
        /// Actor A remote ID.
        actor_remote_id: Cow<'static, str>,
        /// Actor B ID.
        sibling_id: ActorId,
        /// Actor B remote ID.
        sibling_remote_id: Cow<'static, str>,
        /// Reply sender.
        reply: oneshot::Sender<SwarmResponse>,
    },
    /// An actor unlink request.
    Unlink {
        /// Actor A ID.
        actor_id: ActorId,
        /// Actor B ID.
        sibling_id: ActorId,
        /// Actor B remote ID.
        sibling_remote_id: Cow<'static, str>,
        /// Reply sender.
        reply: oneshot::Sender<SwarmResponse>,
    },
    /// Notifies a linked actor has died.
    SignalLinkDied {
        /// The actor which died.
        dead_actor_id: ActorId,
        /// The actor to notify.
        notified_actor_id: ActorId,
        /// Actor remote iD
        notified_actor_remote_id: Cow<'static, str>,
        /// The reason the actor died.
        stop_reason: ActorStopReason,
        /// Reply sender.
        reply: oneshot::Sender<SwarmResponse>,
    },
}

/// `SwarmFuture` represents a future that contains the response from a remote actor.
///
/// This future is returned when sending a message to a remote actor via the actor swarm.
/// If the response is not needed, the future can simply be dropped without awaiting it.
#[derive(Debug)]
struct SwarmFuture<T>(oneshot::Receiver<T>);

impl<T> Future for SwarmFuture<T> {
    type Output = T;

    fn poll(mut self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        task::Poll::Ready(
            ready!(self.0.poll_unpin(cx))
                .expect("the oneshot sender should never be dropped before being sent to"),
        )
    }
}


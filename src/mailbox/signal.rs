/// A signal which can be sent to an actors mailbox.
#[allow(missing_debug_implementations)]
pub enum Signal<A: Actor> {
    /// The actor has finished starting up.
    StartupFinished,
    /// A message.
    Message {
        /// The boxed message.
        message: BoxMessage<A>,
        /// The actor ref, to keep the actor from stopping due to RAII semantics.
        actor_ref: ActorRef<A>,
        /// The reply sender.
        reply: Option<BoxReplySender>,
        /// If the message sent from within the actor's tokio task/thread
        sent_within_actor: bool,
    },
    /// A linked actor has died.
    LinkDied {
        /// The dead actor's ID.
        id: ActorId,
        /// The reason the actor stopped.
        reason: ActorStopReason,
    },
    /// Signals the actor to stop.
    Stop,
}

impl<A: Actor> Signal<A> {
    pub(crate) fn downcast_message<M>(self) -> Option<M>
    where
        M: 'static,
    {
        match self {
            Signal::Message { message, .. } => message.as_any().downcast().ok().map(|v| *v),
            _ => None,
        }
    }
}

#[doc(hidden)]
pub trait SignalMailbox: DynClone + Send + Sync {
    fn signal_startup_finished(&self) -> Result<(), SendError>;
    fn signal_link_died(
        &self,
        id: ActorId,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>>;
}

impl<A> SignalMailbox for MailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => {
                tx.try_send(Signal::StartupFinished)
                    .map_err(|err| match err {
                        mpsc::error::TrySendError::Full(_) => SendError::MailboxFull(()),
                        mpsc::error::TrySendError::Closed(_) => SendError::ActorNotRunning(()),
                    })
            }
            MailboxSenderInner::Unbounded(tx) => tx
                .send(Signal::StartupFinished)
                .map_err(|_| SendError::ActorNotRunning(())),
        }
    }

    fn signal_link_died(
        &self,
        id: ActorId,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => async move {
                tx.send(Signal::LinkDied { id, reason })
                    .await
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
            MailboxSenderInner::Unbounded(tx) => async move {
                tx.send(Signal::LinkDied { id, reason })
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
        }
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => async move {
                tx.send(Signal::Stop)
                    .await
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
            MailboxSenderInner::Unbounded(tx) => async move {
                tx.send(Signal::Stop)
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
        }
    }
}

impl<A> SignalMailbox for WeakMailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        match self.upgrade() {
            Some(tx) => tx.signal_startup_finished(),
            None => Err(SendError::ActorNotRunning(())),
        }
    }

    fn signal_link_died(
        &self,
        id: ActorId,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(tx) => tx.signal_link_died(id, reason).await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(tx) => tx.signal_stop().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }
}

dyn_clone::clone_trait_object!(SignalMailbox);

/// A type-erased actor reference for bidirectional communication with a single message type.
///
/// Supports both `tell` and `ask` operations, with response types determined by the
/// message's `Reply` implementation. This provides a focused API while hiding the
/// concrete actor type.
///
/// Created by [`ActorRef::reply_recipient`].
pub struct ReplyRecipient<M: Send + 'static, Ok: Send + 'static, Err: ReplyError = Infallible> {
    pub(crate) handler: Box<dyn ReplyMessageHandler<M, Ok, Err>>,
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> ReplyRecipient<M, Ok, Err> {
    fn new<A, AR>(actor_ref: ActorRef<A>) -> Self
    where
        AR: Reply<Ok = Ok, Error = Err>,
        A: Actor + Message<M, Reply = AR>,
    {
        ReplyRecipient {
            handler: Box::new(actor_ref),
        }
    }

    /// Converts this reply recipient into a regular recipient, losing `ask` capability.
    ///
    /// Returns a [`Recipient<M>`] that only supports `tell` operations. This is useful
    /// when you need to pass the recipient to code that doesn't require bidirectional
    /// communication.
    pub fn erase_reply(self) -> Recipient<M> {
        Recipient {
            handler: self.handler.upcast(),
        }
    }

    /// Returns the unique identifier of the actor.
    #[inline]
    pub fn id(&self) -> ActorId {
        self.handler.id()
    }

    /// Returns whether the actor is currently alive.
    #[inline]
    pub fn is_alive(&self) -> bool {
        self.handler.is_alive()
    }

    /// Converts the `ReplyRecipient` to a [`WeakReplyRecipient`] that does not count
    /// towards RAII semantics, i.e. if all `ActorRef`/`ReplyRecipient` instances of the
    /// actor were dropped and only `WeakActorRef`/`WeakReplyRecipient` instances remain,
    /// the actor is stopped.
    #[must_use = "Downgrade creates a WeakReplyRecipient without destroying the original non-weak recipient."]
    #[inline]
    pub fn downgrade(&self) -> WeakReplyRecipient<M, Ok, Err> {
        self.handler.reply_downgrade()
    }

    /// Returns the number of [`ActorRef`]/[`ReplyRecipient`] handles.
    #[inline]
    pub fn strong_count(&self) -> usize {
        self.handler.strong_count()
    }

    /// Returns the number of [`WeakActorRef`]/[`WeakReplyRecipient`] handles.
    #[inline]
    pub fn weak_count(&self) -> usize {
        self.handler.weak_count()
    }

    /// Returns `true` if the current task is the actor itself.
    ///
    /// See [`ActorRef::is_current`].
    #[inline]
    pub fn is_current(&self) -> bool {
        self.handler.is_current()
    }

    /// Signals the actor to stop after processing all messages currently in its mailbox.
    ///
    /// See [`ActorRef::stop_gracefully`].
    #[inline]
    pub async fn stop_gracefully(&self) -> Result<(), SendError> {
        self.handler.stop_gracefully().await
    }

    /// Kills the actor immediately.
    ///
    /// See [`ActorRef::kill`].
    #[inline]
    pub fn kill(&self) {
        self.handler.kill()
    }

    /// Waits for the actor to finish startup and become ready to process messages.
    ///
    /// See [`ActorRef::wait_for_startup`].
    #[inline]
    pub async fn wait_for_startup(&self) {
        self.handler.wait_for_startup().await
    }

    /// Waits for the actor to finish processing and stop.
    ///
    /// See [`ActorRef::wait_for_shutdown`].
    #[inline]
    pub async fn wait_for_shutdown(&self) {
        self.handler.wait_for_shutdown().await
    }

    /// Sends a message to the actor without waiting for a reply.
    ///
    /// See [`ActorRef::tell`].
    #[track_caller]
    pub fn tell(&self, msg: M) -> ReplyRecipientTellRequest<'_, M, Ok, Err, WithoutRequestTimeout> {
        ReplyRecipientTellRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }

    /// Sends a message to the actor waits for a reply.
    ///
    /// See [`ActorRef::ask`].
    #[track_caller]
    pub fn ask(&self, msg: M) -> ReplyRecipientAskRequest<'_, M, Ok, Err, WithoutRequestTimeout> {
        ReplyRecipientAskRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Clone for ReplyRecipient<M, Ok, Err> {
    fn clone(&self) -> Self {
        ReplyRecipient {
            handler: dyn_clone::clone_box(&*self.handler),
        }
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> fmt::Debug
    for ReplyRecipient<M, Ok, Err>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("ReplyRecipient");
        d.field("id", &self.handler.id());
        d.finish()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> PartialEq
    for ReplyRecipient<M, Ok, Err>
{
    fn eq(&self, other: &Self) -> bool {
        self.handler.id() == other.handler.id()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Eq for ReplyRecipient<M, Ok, Err> {}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> PartialOrd
    for ReplyRecipient<M, Ok, Err>
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Ord for ReplyRecipient<M, Ok, Err> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.handler.id().cmp(&other.handler.id())
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Hash for ReplyRecipient<M, Ok, Err> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler.id().hash(state);
    }
}

/// A type erased actor ref, accepting only a single message type.
///
/// This is returned by [ActorRef::recipient].
pub struct Recipient<M: Send + 'static> {
    pub(crate) handler: Box<dyn MessageHandler<M>>,
}

impl<M: Send + 'static> Recipient<M> {
    fn new<A>(actor_ref: ActorRef<A>) -> Self
    where
        A: Actor + Message<M>,
    {
        Recipient {
            handler: Box::new(actor_ref),
        }
    }

    /// Returns the unique identifier of the actor.
    #[inline]
    pub fn id(&self) -> ActorId {
        self.handler.id()
    }

    /// Returns whether the actor is currently alive.
    #[inline]
    pub fn is_alive(&self) -> bool {
        self.handler.is_alive()
    }

    /// Converts the `Recipient` to a [`WeakRecipient`] that does not count
    /// towards RAII semantics, i.e. if all `ActorRef`/`Recipient` instances of the
    /// actor were dropped and only `WeakActorRef`/`WeakRecipient` instances remain,
    /// the actor is stopped.
    #[must_use = "Downgrade creates a WeakRecipient without destroying the original non-weak recipient."]
    #[inline]
    pub fn downgrade(&self) -> WeakRecipient<M> {
        self.handler.downgrade()
    }

    /// Returns the number of [`ActorRef`]/[`Recipient`] handles.
    #[inline]
    pub fn strong_count(&self) -> usize {
        self.handler.strong_count()
    }

    /// Returns the number of [`WeakActorRef`]/[`WeakRecipient`] handles.
    #[inline]
    pub fn weak_count(&self) -> usize {
        self.handler.weak_count()
    }

    /// Returns `true` if the current task is the actor itself.
    ///
    /// See [`ActorRef::is_current`].
    #[inline]
    pub fn is_current(&self) -> bool {
        self.handler.is_current()
    }

    /// Signals the actor to stop after processing all messages currently in its mailbox.
    ///
    /// See [`ActorRef::stop_gracefully`].
    #[inline]
    pub async fn stop_gracefully(&self) -> Result<(), SendError> {
        self.handler.stop_gracefully().await
    }

    /// Kills the actor immediately.
    ///
    /// See [`ActorRef::kill`].
    #[inline]
    pub fn kill(&self) {
        self.handler.kill()
    }

    /// Waits for the actor to finish startup and become ready to process messages.
    ///
    /// See [`ActorRef::wait_for_startup`].
    #[inline]
    pub async fn wait_for_startup(&self) {
        self.handler.wait_for_startup().await
    }

    /// Waits for the actor to finish processing and stop.
    ///
    /// See [`ActorRef::wait_for_shutdown`].
    #[inline]
    pub async fn wait_for_shutdown(&self) {
        self.handler.wait_for_shutdown().await
    }

    /// Sends a message to the actor without waiting for a reply.
    ///
    /// See [`ActorRef::tell`].
    #[track_caller]
    pub fn tell(&self, msg: M) -> RecipientTellRequest<'_, M, WithoutRequestTimeout> {
        RecipientTellRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }
}

impl<M: Send + 'static> Clone for Recipient<M> {
    fn clone(&self) -> Self {
        Recipient {
            handler: dyn_clone::clone_box(&*self.handler),
        }
    }
}

impl<M: Send + 'static> fmt::Debug for Recipient<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Recipient");
        d.field("id", &self.handler.id());
        d.finish()
    }
}

impl<M: Send + 'static> PartialEq for Recipient<M> {
    fn eq(&self, other: &Self) -> bool {
        self.handler.id() == other.handler.id()
    }
}

impl<M: Send + 'static> Eq for Recipient<M> {}

impl<M: Send + 'static> PartialOrd for Recipient<M> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M: Send + 'static> Ord for Recipient<M> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.handler.id().cmp(&other.handler.id())
    }
}

impl<M: Send + 'static> Hash for Recipient<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler.id().hash(state);
    }
}


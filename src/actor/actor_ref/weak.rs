/// A actor ref that does not prevent the actor from being stopped.
///
/// If all [`ActorRef`] instances of an actor were dropped and only
/// `WeakActorRef` instances remain, the actor is stopped.
///
/// In order to send messages to an actor, the `WeakActorRef` needs to be upgraded using
/// [`WeakActorRef::upgrade`], which returns `Option<ActorRef>`. It returns `None`
/// if all `ActorRef`s have been dropped, and otherwise it returns an `ActorRef`.
pub struct WeakActorRef<A: Actor> {
    id: ActorId,
    mailbox_sender: WeakMailboxSender<A>,
    abort_handle: AbortHandle,
    pub(crate) links: Links,
    pub(crate) startup_result: Arc<SetOnce<Result<(), PanicError>>>,
    pub(crate) shutdown_result: Arc<SetOnce<Result<(), PanicError>>>,
}

impl<A: Actor> WeakActorRef<A> {
    /// Returns the actor identifier.
    pub fn id(&self) -> ActorId {
        self.id
    }

    /// Returns whether the actor is currently alive.
    #[inline]
    pub fn is_alive(&self) -> bool {
        !self.shutdown_result.initialized()
    }

    /// Tries to convert a `WeakActorRef` into a [`ActorRef`]. This will return `Some`
    /// if there are other `ActorRef` instances alive, otherwise `None` is returned.
    #[must_use]
    pub fn upgrade(&self) -> Option<ActorRef<A>> {
        self.mailbox_sender.upgrade().map(|mailbox| ActorRef {
            id: self.id,
            mailbox_sender: mailbox,
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_result: self.startup_result.clone(),
            shutdown_result: self.shutdown_result.clone(),
        })
    }

    /// Returns the number of [`ActorRef`] handles.
    pub fn strong_count(&self) -> usize {
        self.mailbox_sender.strong_count()
    }

    /// Returns the number of [`WeakActorRef`] handles.
    pub fn weak_count(&self) -> usize {
        self.mailbox_sender.weak_count()
    }

    /// Returns `true` if the current task is the actor itself.
    ///
    /// See [`ActorRef::is_current`] for full details.
    #[inline]
    pub fn is_current(&self) -> bool {
        CURRENT_ACTOR_ID
            .try_with(Clone::clone)
            .map(|current_actor_id| current_actor_id == self.id)
            .unwrap_or(false)
    }

    /// Kills the actor immediately.
    ///
    /// See [`ActorRef::kill`] for full details on behavior.
    #[inline]
    pub fn kill(&self) {
        self.abort_handle.abort()
    }

    /// Waits for the actor to finish startup and become ready to process messages.
    ///
    /// See [`ActorRef::wait_for_startup`] for full details and examples.
    #[inline]
    pub async fn wait_for_startup(&self) {
        self.startup_result.wait().await;
    }

    /// Waits for the actor to finish startup, returning the startup result with a clone of the error.
    ///
    /// See [`ActorRef::wait_for_startup_result`] for full details and examples.
    pub async fn wait_for_startup_result(&self) -> Result<(), HookError<A::Error>>
    where
        A::Error: Clone,
    {
        match self.startup_result.wait().await {
            Ok(()) => Ok(()),
            Err(err) => Err(err
                .with_downcast_ref(|err: &A::Error| HookError::Error(err.clone()))
                .unwrap_or_else(|| HookError::Panicked(err.clone()))),
        }
    }

    /// Waits for the actor to finish startup, returning the startup result with a closure containing the error.
    ///
    /// See [`ActorRef::wait_for_startup_with_result`] for full details and examples.
    pub async fn wait_for_startup_with_result<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Result<(), HookError<&A::Error>>) -> R,
    {
        match self.startup_result.wait().await {
            Ok(()) => f(Ok(())),
            Err(err) => {
                let mut f = Some(f);
                let result = err.with_downcast_ref(|e: &A::Error| {
                    (f.take().expect("taken exactly once in downcast branch"))(Err(
                        HookError::Error(e),
                    ))
                });
                match result {
                    Some(r) => r,
                    None => (f
                        .take()
                        .expect("not taken: downcast branch was not entered"))(
                        Err(HookError::Panicked(err.clone())),
                    ),
                }
            }
        }
    }

    /// Waits for the actor to finish processing and stop running.
    ///
    /// See [`ActorRef::wait_for_shutdown`] for full details and examples.
    #[inline]
    pub async fn wait_for_shutdown(&self) {
        self.shutdown_result.wait().await;
    }

    /// Waits for the actor to finish shutdown, returning the shutdown result with a clone of the error.
    ///
    /// See [`ActorRef::wait_for_shutdown_result`] for full details and examples.
    pub async fn wait_for_shutdown_result(&self) -> Result<(), HookError<A::Error>>
    where
        A::Error: Clone,
    {
        match self.shutdown_result.wait().await {
            Ok(()) => Ok(()),
            Err(err) => Err(err
                .with_downcast_ref(|err: &A::Error| HookError::Error(err.clone()))
                .unwrap_or_else(|| HookError::Panicked(err.clone()))),
        }
    }

    /// Waits for the actor to finish shutdown, returning the shutdown result with a clone of the error.
    ///
    /// See [`ActorRef::wait_for_shutdown_with_result`] for full details and examples.
    pub async fn wait_for_shutdown_with_result<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Result<(), HookError<&A::Error>>) -> R,
    {
        match self.shutdown_result.wait().await {
            Ok(()) => f(Ok(())),
            Err(err) => {
                let mut f = Some(f);
                let result = err.with_downcast_ref(|e: &A::Error| {
                    (f.take().expect("taken exactly once in downcast branch"))(Err(
                        HookError::Error(e),
                    ))
                });
                match result {
                    Some(r) => r,
                    None => (f
                        .take()
                        .expect("not taken: downcast branch was not entered"))(
                        Err(HookError::Panicked(err.clone())),
                    ),
                }
            }
        }
    }

    /// Unlinks two previously linked sibling actors.
    ///
    /// See [`ActorRef::unlink`] for full details and examples.
    #[inline]
    pub async fn unlink<B: Actor>(&self, sibling_ref: &ActorRef<B>) {
        if self.id == sibling_ref.id {
            return;
        }

        if self.id < sibling_ref.id {
            let mut this_links = self.links.lock().await;
            let mut sibling_links = sibling_ref.links.lock().await;

            this_links.remove(&sibling_ref.id);
            sibling_links.remove(&self.id);
        } else {
            let mut sibling_links = sibling_ref.links.lock().await;
            let mut this_links = self.links.lock().await;

            this_links.remove(&sibling_ref.id);
            sibling_links.remove(&self.id);
        }
    }

    /// Blockingly unlinks two previously linked sibling actors.
    ///
    /// See [`ActorRef::blocking_unlink`] for full details and examples.
    #[inline]
    pub fn blocking_unlink<B: Actor>(&self, sibling_ref: &ActorRef<B>) {
        if self.id == sibling_ref.id {
            return;
        }

        if self.id < sibling_ref.id {
            let mut this_links = self.links.blocking_lock();
            let mut sibling_links = sibling_ref.links.blocking_lock();

            this_links.remove(&sibling_ref.id);
            sibling_links.remove(&self.id);
        } else {
            let mut sibling_links = sibling_ref.links.blocking_lock();
            let mut this_links = self.links.blocking_lock();

            this_links.remove(&sibling_ref.id);
            sibling_links.remove(&self.id);
        }
    }

    /// Unlinks the local actor with a previously linked remote actor.
    ///
    /// See [`ActorRef::unlink_remote`] for full details and examples.
    #[cfg(feature = "remote")]
    pub async fn unlink_remote<B>(
        &self,
        sibling_ref: &RemoteActorRef<B>,
    ) -> Result<(), error::RemoteSendError<error::Infallible>>
    where
        A: remote::RemoteActor,
        B: Actor + remote::RemoteActor,
    {
        if self.id == sibling_ref.id {
            return Ok(());
        }

        self.links.lock().await.remove(&sibling_ref.id);
        remote::ActorSwarm::get()
            .ok_or(error::RemoteSendError::SwarmNotBootstrapped)?
            .unlink::<B>(self.id, sibling_ref.id)
            .await
    }

    /// Returns a reference to the weak mailbox sender.
    ///
    /// See [`ActorRef::mailbox_sender`] for full details.
    #[inline]
    pub fn mailbox_sender(&self) -> &WeakMailboxSender<A> {
        &self.mailbox_sender
    }

    #[cfg(feature = "remote")]
    #[inline]
    pub(crate) fn weak_signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.mailbox_sender.clone())
    }
}

impl<A: Actor> Clone for WeakActorRef<A> {
    fn clone(&self) -> Self {
        WeakActorRef {
            id: self.id,
            mailbox_sender: self.mailbox_sender.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_result: self.startup_result.clone(),
            shutdown_result: self.shutdown_result.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for WeakActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("WeakActorRef");
        d.field("id", &self.id);
        match self.links.try_lock() {
            Ok(guard) => {
                d.field("links", &guard.keys());
            }
            Err(_) => {
                d.field("links", &format_args!("<locked>"));
            }
        }
        d.finish()
    }
}

impl<A: Actor> PartialEq for WeakActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<A: Actor> Eq for WeakActorRef<A> {}

impl<A: Actor> PartialOrd for WeakActorRef<A> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<A: Actor> Ord for WeakActorRef<A> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<A: Actor> Hash for WeakActorRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// A weak recipient that does not prevent the actor from being stopped.
pub struct WeakRecipient<M: Send + 'static> {
    handler: Box<dyn WeakMessageHandler<M>>,
}

impl<M: Send + 'static> WeakRecipient<M> {
    fn new<A>(weak_actor_ref: WeakActorRef<A>) -> Self
    where
        A: Actor + Message<M>,
    {
        WeakRecipient {
            handler: Box::new(weak_actor_ref),
        }
    }

    /// Returns the actor identifier.
    pub fn id(&self) -> ActorId {
        self.handler.id()
    }

    /// Tries to convert a `WeakRecipient` into a [`Recipient`]. This will return `Some`
    /// if there are other `ActorRef`/`Recipient` instances alive, otherwise `None` is returned.
    #[must_use]
    pub fn upgrade(&self) -> Option<Recipient<M>> {
        self.handler.upgrade()
    }

    /// Returns the number of [`ActorRef`]/[`Recipient`] handles.
    pub fn strong_count(&self) -> usize {
        self.handler.strong_count()
    }

    /// Returns the number of [`WeakActorRef`]/[`WeakRecipient`] handles.
    pub fn weak_count(&self) -> usize {
        self.handler.weak_count()
    }
}

impl<M: Send + 'static> Clone for WeakRecipient<M> {
    fn clone(&self) -> Self {
        WeakRecipient {
            handler: dyn_clone::clone_box(&*self.handler),
        }
    }
}

impl<M: Send + 'static> fmt::Debug for WeakRecipient<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("WeakRecipient");
        d.field("id", &self.handler.id());
        d.finish()
    }
}

impl<M: Send + 'static> PartialEq for WeakRecipient<M> {
    fn eq(&self, other: &Self) -> bool {
        self.handler.id() == other.handler.id()
    }
}

impl<M: Send + 'static> Eq for WeakRecipient<M> {}

impl<M: Send + 'static> PartialOrd for WeakRecipient<M> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M: Send + 'static> Ord for WeakRecipient<M> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.handler.id().cmp(&other.handler.id())
    }
}

impl<M: Send + 'static> Hash for WeakRecipient<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler.id().hash(state);
    }
}

/// A weak recipient that does not prevent the actor from being stopped.
pub struct WeakReplyRecipient<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> {
    handler: Box<dyn WeakReplyMessageHandler<M, Ok, Err>>,
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> WeakReplyRecipient<M, Ok, Err> {
    fn new<A, AR>(weak_actor_ref: WeakActorRef<A>) -> Self
    where
        AR: Reply<Ok = Ok, Error = Err>,
        A: Actor + Message<M, Reply = AR>,
    {
        WeakReplyRecipient {
            handler: Box::new(weak_actor_ref),
        }
    }

    /// Returns the actor identifier.
    pub fn id(&self) -> ActorId {
        self.handler.id()
    }

    /// Tries to convert a `WeakReplyRecipient` into a [`ReplyRecipient`]. This will return `Some`
    /// if there are other `ActorRef`/`ReplyRecipient` instances alive, otherwise `None` is returned.
    #[must_use]
    pub fn upgrade(&self) -> Option<ReplyRecipient<M, Ok, Err>> {
        self.handler.reply_upgrade()
    }

    /// Returns the number of [`ActorRef`]/[`ReplyRecipient`] handles.
    pub fn strong_count(&self) -> usize {
        self.handler.strong_count()
    }

    /// Returns the number of [`WeakActorRef`]/[`WeakReplyRecipient`] handles.
    pub fn weak_count(&self) -> usize {
        self.handler.weak_count()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Clone
    for WeakReplyRecipient<M, Ok, Err>
{
    fn clone(&self) -> Self {
        WeakReplyRecipient {
            handler: dyn_clone::clone_box(&*self.handler),
        }
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> fmt::Debug
    for WeakReplyRecipient<M, Ok, Err>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("WeakReplyRecipient");
        d.field("id", &self.handler.id());
        d.finish()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> PartialEq
    for WeakReplyRecipient<M, Ok, Err>
{
    fn eq(&self, other: &Self) -> bool {
        self.handler.id() == other.handler.id()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Eq for WeakReplyRecipient<M, Ok, Err> {}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> PartialOrd
    for WeakReplyRecipient<M, Ok, Err>
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Ord
    for WeakReplyRecipient<M, Ok, Err>
{
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.handler.id().cmp(&other.handler.id())
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Hash
    for WeakReplyRecipient<M, Ok, Err>
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler.id().hash(state);
    }
}

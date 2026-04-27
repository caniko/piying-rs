impl<A> ActorRef<A>
where
    A: Actor,
{
    #[inline]
    pub(crate) fn new(
        mailbox: MailboxSender<A>,
        abort_handle: AbortHandle,
        links: Links,
        startup_result: Arc<SetOnce<Result<(), PanicError>>>,
        shutdown_result: Arc<SetOnce<Result<(), PanicError>>>,
    ) -> Self {
        ActorRef {
            id: ActorId::generate(),
            mailbox_sender: mailbox,
            abort_handle,
            links,
            startup_result,
            shutdown_result,
        }
    }

    /// Returns the unique identifier of the actor.
    #[inline]
    pub fn id(&self) -> ActorId {
        self.id
    }

    /// Returns whether the actor is currently alive.
    #[inline]
    pub fn is_alive(&self) -> bool {
        !self.mailbox_sender.is_closed()
    }

    /// Registers the actor under a given name in the actor registry.
    ///
    /// This makes the actor discoverable by parts of the app by name.
    #[cfg(not(feature = "remote"))]
    pub fn register(
        &self,
        name: impl Into<std::borrow::Cow<'static, str>>,
    ) -> Result<(), error::RegistryError> {
        let was_inserted = crate::registry::ACTOR_REGISTRY
            .lock()
            .unwrap()
            .insert(name, self.clone());
        if !was_inserted {
            Err(error::RegistryError::NameAlreadyRegistered)
        } else {
            Ok(())
        }
    }

    /// Registers the actor under a given name within the actor swarm.
    ///
    /// This makes the actor discoverable by other nodes in the distributed system.
    #[cfg(feature = "remote")]
    pub async fn register(&self, name: impl Into<Arc<str>>) -> Result<(), error::RegistryError>
    where
        A: remote::RemoteActor + 'static,
    {
        remote::ActorSwarm::get()
            .ok_or(error::RegistryError::SwarmNotBootstrapped)?
            .register(self.clone(), name.into())
            .await
    }

    /// Looks up an actor registered locally by its name.
    ///
    /// Returns `Some` if the actor exists, or `None` if no actor with the given name is registered.
    #[cfg(not(feature = "remote"))]
    pub fn lookup<Q>(name: &Q) -> Result<Option<Self>, error::RegistryError>
    where
        Q: std::hash::Hash + Eq + ?Sized,
        std::borrow::Cow<'static, str>: std::borrow::Borrow<Q>,
    {
        crate::registry::ACTOR_REGISTRY.lock().unwrap().get(name)
    }

    /// Looks up an actor registered locally by its name.
    ///
    /// Returns `Some` if the actor exists, or `None` if no actor with the given name is registered.
    #[cfg(feature = "remote")]
    pub async fn lookup(name: impl Into<Arc<str>>) -> Result<Option<Self>, error::RegistryError>
    where
        A: remote::RemoteActor + 'static,
    {
        remote::ActorSwarm::get()
            .ok_or(error::RegistryError::SwarmNotBootstrapped)?
            .lookup_local(name.into())
            .await
    }

    /// Creates a message-specific recipient for this actor.
    ///
    /// This allows creating a more specific reference that hides the concrete
    /// actor type while preserving the ability to send messages via `tell`.
    ///
    /// The recipient maintains the same message handling behavior as the
    /// original actor reference, but with a more focused API.
    ///
    /// For bidirectional communication that supports `ask` requests,
    /// see [`ActorRef::reply_recipient`].
    #[must_use]
    pub fn recipient<M>(self) -> Recipient<M>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        Recipient::new(self)
    }

    /// Creates a message-specific recipient for this actor with bidirectional communication.
    ///
    /// This allows creating a more specific reference that hides the concrete
    /// actor type while preserving the ability to send messages via both `tell` and `ask`.
    ///
    /// The recipient maintains the same message handling behavior as the
    /// original actor reference, but with a more focused API. The `Ok` and `Err`
    /// types are determined by the message's `Reply` implementation.
    ///
    /// For unidirectional communication that only supports `tell`,
    /// see [`ActorRef::recipient`].
    #[must_use]
    pub fn reply_recipient<M>(
        self,
    ) -> ReplyRecipient<M, <A::Reply as Reply>::Ok, <A::Reply as Reply>::Error>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        ReplyRecipient::new(self)
    }

    /// Converts the `ActorRef` to a [`WeakActorRef`] that does not count
    /// towards RAII semantics, i.e. if all `ActorRef` instances of the
    /// actor were dropped and only `WeakActorRef` instances remain,
    /// the actor is stopped.
    #[must_use = "Downgrade creates a WeakActorRef without destroying the original non-weak actor ref."]
    #[inline]
    pub fn downgrade(&self) -> WeakActorRef<A> {
        WeakActorRef {
            id: self.id,
            mailbox_sender: self.mailbox_sender.downgrade(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_result: self.startup_result.clone(),
            shutdown_result: self.shutdown_result.clone(),
        }
    }

    pub(crate) fn into_downgrade(self) -> WeakActorRef<A> {
        WeakActorRef {
            id: self.id,
            mailbox_sender: self.mailbox_sender.downgrade(),
            abort_handle: self.abort_handle,
            links: self.links,
            startup_result: self.startup_result,
            shutdown_result: self.shutdown_result,
        }
    }

    /// Returns the number of [`ActorRef`] handles.
    #[inline]
    pub fn strong_count(&self) -> usize {
        self.mailbox_sender.strong_count()
    }

    /// Returns the number of [`WeakActorRef`] handles.
    #[inline]
    pub fn weak_count(&self) -> usize {
        self.mailbox_sender.weak_count()
    }

    /// Returns `true` if the current task is the actor itself.
    ///
    /// This is useful when checking if certain code is being executed from within the actor's own context.
    #[inline]
    pub fn is_current(&self) -> bool {
        CURRENT_ACTOR_ID
            .try_with(Clone::clone)
            .map(|current_actor_id| current_actor_id == self.id)
            .unwrap_or(false)
    }

    /// Signals the actor to stop after processing all messages currently in its mailbox.
    ///
    /// This method ensures that the actor finishes processing any messages that were already in the queue
    /// before it shuts down. Any new messages sent after the stop signal will be ignored.
    #[inline]
    pub async fn stop_gracefully(&self) -> Result<(), SendError> {
        self.mailbox_sender
            .send(Signal::Stop)
            .await
            .map_err(|_| SendError::ActorNotRunning(()))
    }

    /// Kills the actor immediately.
    ///
    /// This method aborts the actor immediately. Messages in the mailbox will be ignored and dropped.
    ///
    /// The actors on_stop hook will still be called.
    ///
    /// Note: If the actor is in the middle of processing a message, it will abort processing of that message.
    #[inline]
    pub fn kill(&self) {
        self.abort_handle.abort()
    }

    /// Waits for the actor to finish startup and become ready to process messages.
    ///
    /// This method ensures the actors on_start lifecycle hook has been fully processed.
    /// If `wait_for_startup` is called after the actor has already started up, this will return immediately.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use thespis::actor::{Actor, ActorRef, Spawn};
    /// use thespis::error::Infallible;
    /// use tokio::time::sleep;
    ///
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = Infallible;
    ///
    ///     async fn on_start(
    ///         state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         sleep(Duration::from_secs(2)).await; // Some io operation
    ///         Ok(state)
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// actor_ref.wait_for_startup().await;
    /// println!("Actor ready to handle messages!");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub async fn wait_for_startup(&self) {
        self.startup_result.wait().await;
    }

    /// Waits for the actor to finish startup, returning the startup result with a clone of the error.
    ///
    /// This method ensures the actors on_start lifecycle hook has been fully processed.
    /// If `wait_for_startup_result` is called after the actor has already started up, this will return immediately.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::ParseIntError;
    ///
    /// use thespis::actor::{Actor, ActorRef, Spawn};
    ///
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = ParseIntError;
    ///
    ///     async fn on_start(
    ///         _state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         "invalid int".parse().map(|_: i32| MyActor) // Will always error
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let startup_result = actor_ref.wait_for_startup_result().await;
    /// assert!(startup_result.is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
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

    /// Waits for the actor to finish startup, returning the startup result with a clousre containing the error.
    ///
    /// This method ensures the actors on_start lifecycle hook has been fully processed.
    /// If `wait_for_startup_with_result` is called after the actor has already started up, this will return immediately.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::actor::{Actor, ActorRef, Spawn};
    ///
    /// struct MyActor;
    ///
    /// #[derive(Debug)]
    /// struct NonCloneError;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = NonCloneError;
    ///
    ///     async fn on_start(
    ///         _state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         Err(NonCloneError) // Will always error
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// actor_ref.wait_for_startup_with_result(|res| {
    ///     assert!(res.is_err());
    /// }).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
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
    /// This method suspends execution until the actor has stopped, ensuring that any ongoing
    /// processing is completed and the actor has fully terminated. This is particularly useful
    /// in scenarios where it's necessary to wait for an actor to clean up its resources or
    /// complete its final tasks before proceeding.
    ///
    /// Note: This method does not initiate the stop process; it only waits for the actor to
    /// stop. You should signal the actor to stop using [`stop_gracefully`](ActorRef::stop_gracefully) or [`kill`](ActorRef::kill)
    /// before calling this method.
    #[inline]
    pub async fn wait_for_shutdown(&self) {
        self.mailbox_sender.closed().await
    }

    /// Waits for the actor to finish shutdown, returning the shutdown result with a clone of the error.
    ///
    /// This method ensures the actor's on_stop lifecycle hook has been fully processed.
    /// If `wait_for_shutdown_result` is called after the actor has already shut down, this will return immediately.
    ///
    /// Note: This method does not initiate the stop process; it only waits for the actor to
    /// stop and returns the result. You should signal the actor to stop using [`stop_gracefully`](ActorRef::stop_gracefully) or [`kill`](ActorRef::kill)
    /// before calling this method.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::ParseIntError;
    ///
    /// use thespis::actor::{Actor, ActorRef, Spawn, WeakActorRef};
    /// use thespis::error::ActorStopReason;
    ///
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = ParseIntError;
    ///
    ///     async fn on_start(
    ///         state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         Ok(state)
    ///     }
    ///
    ///     async fn on_stop(&mut self, actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> Result<(), Self::Error> {
    ///         "invalid int".parse().map(|_: i32| ()) // Will always error
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// actor_ref.stop_gracefully().await;
    /// let shutdown_result = actor_ref.wait_for_shutdown_result().await;
    /// assert!(shutdown_result.is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn wait_for_shutdown_result(&self) -> Result<(), HookError<A::Error>>
    where
        A::Error: Clone,
    {
        self.mailbox_sender.closed().await;
        match self.shutdown_result.wait().await {
            Ok(()) => Ok(()),
            Err(err) => Err(err
                .with_downcast_ref(|err: &A::Error| HookError::Error(err.clone()))
                .unwrap_or_else(|| HookError::Panicked(err.clone()))),
        }
    }

    /// Waits for the actor to finish shutdown, returning the shutdown result with a clone of the error.
    ///
    /// This method ensures the actor's on_stop lifecycle hook has been fully processed.
    /// If `wait_for_shutdown_result` is called after the actor has already shut down, this will return immediately.
    ///
    /// Note: This method does not initiate the stop process; it only waits for the actor to
    /// stop and returns the result. You should signal the actor to stop using [`stop_gracefully`](ActorRef::stop_gracefully) or [`kill`](ActorRef::kill)
    /// before calling this method.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::actor::{Actor, ActorRef, Spawn, WeakActorRef};
    /// use thespis::error::ActorStopReason;
    ///
    /// struct MyActor;
    ///
    /// #[derive(Debug)]
    /// struct NonCloneError;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = NonCloneError;
    ///
    ///     async fn on_start(
    ///         state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         Ok(state)
    ///     }
    ///
    ///     async fn on_stop(&mut self, actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> Result<(), Self::Error> {
    ///         Err(NonCloneError) // Will always error
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// actor_ref.stop_gracefully().await;
    /// actor_ref.wait_for_shutdown_with_result(|res| {
    ///     assert!(res.is_err());
    /// }).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn wait_for_shutdown_with_result<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Result<(), HookError<&A::Error>>) -> R,
    {
        self.mailbox_sender.closed().await;
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
}

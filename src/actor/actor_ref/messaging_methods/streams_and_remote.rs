impl<A> ActorRef<A>
where
    A: Actor,
{
    /// Attaches a stream of messages to the actor, forwarding each item in the stream.
    ///
    /// The stream will continue until it is completed or the actor is stopped. A `JoinHandle` is returned,
    /// which can be used to cancel the stream. The `start_value` and `finish_value` can provide additional
    /// context for the stream but are optional.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    /// use thespis::message::{Context, Message, StreamMessage};
    ///
    /// #[derive(thespis::Actor)]
    /// struct MyActor;
    ///
    /// impl Message<StreamMessage<u32, (), ()>> for MyActor {
    ///     type Reply = ();
    ///
    ///     async fn handle(&mut self, msg: StreamMessage<u32, (), ()>, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
    ///         match msg {
    ///             StreamMessage::Next(num) => {
    ///                 println!("Received item: {num}");
    ///             }
    ///             StreamMessage::Started(()) => {
    ///                 println!("Stream attached!");
    ///             }
    ///             StreamMessage::Finished(()) => {
    ///                 println!("Stream finished!");
    ///             }
    ///         }
    ///     }
    /// }
    /// #
    /// # tokio_test::block_on(async {
    /// let stream = futures::stream::iter(vec![17, 19, 24]);
    ///
    /// let actor_ref = MyActor::spawn(MyActor);
    /// actor_ref.attach_stream(stream, (), ()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn attach_stream<M, S, T, F>(
        &self,
        mut stream: S,
        start_value: T,
        finish_value: F,
    ) -> JoinHandle<Result<S, SendError<StreamMessage<M, T, F>>>>
    where
        A: Message<StreamMessage<M, T, F>>,
        S: Stream<Item = M> + Send + Unpin + 'static,
        M: Send + 'static,
        T: Send + 'static,
        F: Send + 'static,
    {
        let actor_ref = self.clone();
        tokio::spawn(async move {
            actor_ref
                .tell(StreamMessage::Started(start_value))
                .send()
                .await?;

            loop {
                tokio::select! {
                    msg = stream.next() => {
                        match msg {
                            Some(msg) => {
                                actor_ref.tell(StreamMessage::Next(msg)).send().await?;
                            }
                            None => break,
                        }
                    }
                    _ = actor_ref.wait_for_shutdown() => {
                        return Ok(stream);
                    }
                }
            }

            actor_ref
                .tell(StreamMessage::Finished(finish_value))
                .send()
                .await?;

            Ok(stream)
        })
    }

    /// Returns a reference to the mailbox sender.
    pub fn mailbox_sender(&self) -> &MailboxSender<A> {
        &self.mailbox_sender
    }

    /// Converts this ActorRef to a RemoteActorRef, registering it in the actor registry.
    ///
    /// This method is async because it needs to acquire a lock on the shared registry.
    #[cfg(feature = "remote")]
    pub async fn into_remote_ref(&self) -> RemoteActorRef<A>
    where
        A: remote::RemoteActor,
    {
        let swarm_tx = remote::ActorSwarm::with(|s| s.sender().clone()).unwrap();
        let remote_ref = RemoteActorRef::new(self.id(), swarm_tx);

        remote::REMOTE_REGISTRY
            .lock()
            .await
            .entry(self.id())
            .or_insert_with(|| remote::RemoteRegistryActorRef::new_weak(self.downgrade(), None));

        remote_ref
    }

    /// Blocking version of `into_remote_ref` for use in synchronous contexts.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    #[cfg(feature = "remote")]
    pub fn into_remote_ref_blocking(&self) -> RemoteActorRef<A>
    where
        A: remote::RemoteActor,
    {
        let swarm_tx = remote::ActorSwarm::with(|s| s.sender().clone()).unwrap();
        let remote_ref = RemoteActorRef::new(self.id(), swarm_tx);

        remote::REMOTE_REGISTRY
            .blocking_lock()
            .entry(self.id())
            .or_insert_with(|| remote::RemoteRegistryActorRef::new_weak(self.downgrade(), None));

        remote_ref
    }

    #[inline]
    pub(crate) fn weak_signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.mailbox_sender.downgrade())
    }
}

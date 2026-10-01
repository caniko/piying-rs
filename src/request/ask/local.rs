/// A request to send a message to an actor, waiting for a reply.
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct AskRequest<'a, A, M, Tm, Tr>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    actor_ref: &'a ActorRef<A>,
    msg: M,
    mailbox_timeout: Tm,
    reply_timeout: Tr,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

impl<'a, A, M, Tm, Tr> AskRequest<'a, A, M, Tm, Tr>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    pub(crate) fn new(
        actor_ref: &'a ActorRef<A>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))] called_at: &'static std::panic::Location<
            'static,
        >,
    ) -> Self
    where
        Tm: Default,
        Tr: Default,
    {
        AskRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            reply_timeout: Tr::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }

    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> AskRequest<'a, A, M, WithRequestTimeout, Tr> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> AskRequest<'a, A, M, WithRequestTimeout, Tr> {
        AskRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            reply_timeout: self.reply_timeout,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sets the timeout for waiting for a reply from the actor.
    pub fn reply_timeout(self, duration: Duration) -> AskRequest<'a, A, M, Tm, WithRequestTimeout> {
        self.reply_timeout_opt(Some(duration))
    }

    pub(crate) fn reply_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> AskRequest<'a, A, M, Tm, WithRequestTimeout> {
        AskRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: self.mailbox_timeout,
            reply_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sends the message.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>
    where
        Tm: Into<Option<Duration>>,
        Tr: Into<Option<Duration>>,
    {
        #[cfg(all(debug_assertions, feature = "tracing"))]
        warn_deadlock(
            self.actor_ref,
            "An actor is sending an `ask` request to itself, which will likely lead to a deadlock. To avoid this, use a `tell` request instead.",
            self.called_at,
        );

        let (reply, rx) = oneshot::channel();
        let signal = Signal::message(self.actor_ref, self.msg, Some(reply));

        let tx = self.actor_ref.mailbox_sender();
        match self.mailbox_timeout.into() {
            Some(timeout) => {
                tx.send_timeout(signal, timeout).await?;
            }
            None => {
                tx.send(signal).await?;
            }
        }

        receive_reply::<M, A::Reply, _>(rx, self.reply_timeout).await
    }

    /// Enqueues the message into the actors mailbox, returning a pending reply which needs to be awaited.
    ///
    /// The actor will not progress until the pending reply has been awaited or dropped.
    /// This may lead to deadlocks if used incorrectly.
    ///
    /// # Example
    ///
    /// ```
    /// # use piying::Actor;
    /// # use piying::actor::Spawn;
    /// #
    /// # #[derive(piying::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl piying::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut piying::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// # let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// let pending = actor_ref.ask(Msg).enqueue().await?;
    /// // Do some other tasks
    /// let reply = pending.await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn enqueue(self) -> Result<PendingReply<M, A::Reply>, SendError>
    where
        Tm: Into<Option<Duration>> + Send + 'static,
        Tr: Into<Option<Duration>> + Send + 'static,
    {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::message(self.actor_ref, self.msg, Some(reply));

        let tx = self.actor_ref.mailbox_sender();
        match self.mailbox_timeout.into() {
            Some(timeout) => {
                tx.send_timeout(signal, timeout)
                    .await
                    .map_err(SendError::<M>::from)
                    .map_err(|err| err.map_msg(|_| ()))?;
            }
            None => {
                tx.send(signal)
                    .await
                    .map_err(SendError::<M>::from)
                    .map_err(|err| err.map_msg(|_| ()))?;
            }
        }

        let fut = receive_reply::<M, A::Reply, _>(rx, self.reply_timeout).boxed();

        Ok(PendingReply { fut })
    }
}

impl<A, M, Tm> AskRequest<'_, A, M, Tm, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Sends a message with the reply being sent back to a channel.
    pub async fn forward(
        self,
        sender: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    >
    where
        Tm: Into<Option<Duration>>,
    {
        let signal = Signal::message(self.actor_ref, self.msg, Some(sender.boxed()));

        let tx = self.actor_ref.mailbox_sender();
        match self.mailbox_timeout.into() {
            Some(timeout) => {
                tx.send_timeout(signal, timeout)
                    .await
                    .map_err(|err| match err {
                        mpsc::error::SendTimeoutError::Timeout(signal) => {
                            SendError::Timeout(Some(recover_forwarded_message(signal)))
                        }
                        mpsc::error::SendTimeoutError::Closed(signal) => {
                            SendError::ActorNotRunning(recover_forwarded_message(signal))
                        }
                    })?;
            }
            None => {
                tx.send(signal)
                    .await
                    .map_err(|err| SendError::ActorNotRunning(recover_forwarded_message(err.0)))?;
            }
        }

        Ok(())
    }
}

impl<A, M> AskRequest<'_, A, M, WithoutRequestTimeout, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Tries to send a message without waiting for mailbox capacity,
    /// with the reply being sent back to a channel.
    #[allow(clippy::type_complexity)]
    pub fn try_forward(
        self,
        sender: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    > {
        let signal = Signal::message(self.actor_ref, self.msg, Some(sender.boxed()));

        let tx = self.actor_ref.mailbox_sender();
        tx.try_send(signal).map_err(|err| match err {
            mpsc::error::TrySendError::Full(signal) => {
                SendError::MailboxFull(recover_forwarded_message(signal))
            }
            mpsc::error::TrySendError::Closed(signal) => {
                SendError::ActorNotRunning(recover_forwarded_message(signal))
            }
        })?;

        Ok(())
    }
}

impl<'a, A, M, Tr> AskRequest<'a, A, M, WithoutRequestTimeout, Tr>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub async fn try_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>
    where
        Tr: Into<Option<Duration>>,
    {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::message(self.actor_ref, self.msg, Some(reply));

        let tx = self.actor_ref.mailbox_sender();
        tx.try_send(signal)?;

        receive_reply::<M, A::Reply, _>(rx, self.reply_timeout).await
    }

    /// Tries to enqueue the message into the actors mailbox without waiting for mailbox capacity,
    /// returning a pending reply which needs to be awaited.
    ///
    /// The actor will not progress until the pending reply has been awaited or dropped.
    /// This may lead to deadlocks if used incorrectly.
    ///
    /// # Example
    ///
    /// ```
    /// # use piying::Actor;
    /// # use piying::actor::Spawn;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl piying::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut piying::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// # let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// let pending = actor_ref.ask(Msg).try_enqueue()?;
    /// // Do some other tasks
    /// let reply = pending.await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub fn try_enqueue(self) -> Result<PendingReply<M, A::Reply>, SendError>
    where
        Tr: Into<Option<Duration>> + Send + 'static,
    {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::message(self.actor_ref, self.msg, Some(reply));

        let tx = self.actor_ref.mailbox_sender();
        tx.try_send(signal)
            .map_err(SendError::<M>::from)
            .map_err(|err| err.map_msg(|_| ()))?;

        let fut = receive_reply::<M, A::Reply, _>(rx, self.reply_timeout).boxed();

        Ok(PendingReply { fut })
    }
}

impl<'a, A, M> AskRequest<'a, A, M, WithoutRequestTimeout, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Sends the message in a blocking context.
    #[allow(clippy::type_complexity)]
    pub fn blocking_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::message(self.actor_ref, self.msg, Some(reply));

        let tx = self.actor_ref.mailbox_sender();
        tx.blocking_send(signal)?;

        downcast_reply::<M, A::Reply>(rx.blocking_recv()?)
    }

    /// Sends a message in a blocking context with the reply being sent back to a channel.
    #[allow(clippy::type_complexity)]
    pub fn blocking_forward(
        self,
        sender: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    > {
        let signal = Signal::message(self.actor_ref, self.msg, Some(sender.boxed()));

        let tx = self.actor_ref.mailbox_sender();
        tx.blocking_send(signal)
            .map_err(|err| SendError::ActorNotRunning(recover_forwarded_message(err.0)))?;

        Ok(())
    }

    /// Enqueues the message into the actors mailbox in a blocking context,
    /// returning a pending reply which needs to be awaited.
    ///
    /// The actor will not progress until the pending reply has been received or dropped.
    /// This may lead to deadlocks if used incorrectly.
    ///
    /// # Example
    ///
    /// ```
    /// # use piying::Actor;
    /// # use piying::actor::Spawn;
    /// #
    /// # #[derive(piying::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl piying::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut piying::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// # let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// # std::thread::spawn(move || {
    /// # let f = move || {
    /// let pending = actor_ref.ask(Msg).blocking_enqueue()?;
    /// // Do some other tasks
    /// let reply = pending.recv()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # };
    /// # f().unwrap();
    /// # });
    /// # });
    /// ```
    pub fn blocking_enqueue(self) -> Result<BlockingPendingReply<'a, M, A::Reply>, SendError> {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::message(self.actor_ref, self.msg, Some(reply));

        let tx = self.actor_ref.mailbox_sender();
        tx.blocking_send(signal)
            .map_err(SendError::<M>::from)
            .map_err(|err| err.map_msg(|_| ()))?;

        let f = Box::new(move || downcast_reply::<M, A::Reply>(rx.blocking_recv()?));
        Ok(BlockingPendingReply { f })
    }
}

impl<'a, A, M, Tm, Tr> IntoFuture for AskRequest<'a, A, M, Tm, Tr>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: Into<Option<Duration>> + Send + 'static,
    Tr: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<<A::Reply as Reply>::Ok, error::SendError<M, <A::Reply as Reply>::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.send().boxed()
    }
}

fn recover_forwarded_message<A: Actor, M: 'static, R>(signal: Signal<A>) -> (M, ReplySender<R>) {
    let Signal::Message {
        message,
        reply: Some(tx),
        ..
    } = signal
    else {
        unreachable!("forwarded requests always contain a message and reply sender");
    };
    (
        *message
            .as_any()
            .downcast()
            .expect("forwarded message type matches the request"),
        ReplySender::new(tx),
    )
}

#[cfg(test)]
mod forwarding_tests {
    use super::*;
    use crate::{actor::PreparedActor, error::Infallible, mailbox, message::Context};

    struct TestActor;

    impl Actor for TestActor {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(args)
        }
    }

    impl Message<u32> for TestActor {
        type Reply = ();

        async fn handle(&mut self, _: u32, _: &mut Context<Self, Self::Reply>) {}
    }

    #[tokio::test]
    async fn rejected_forward_returns_the_message_and_usable_reply_sender() {
        let prepared = PreparedActor::new(mailbox::unbounded::<TestActor>());
        let actor = prepared.actor_ref();
        actor.stop_gracefully().await.unwrap();

        for mode in 0..4 {
            let (tx, rx) = oneshot::channel();
            let sender = ReplySender::new(tx);
            let result = match mode {
                0 => actor.ask(mode).forward(sender).await,
                1 => {
                    actor
                        .ask(mode)
                        .mailbox_timeout(Duration::from_secs(1))
                        .forward(sender)
                        .await
                }
                2 => actor.ask(mode).try_forward(sender),
                _ => actor.ask(mode).blocking_forward(sender),
            };
            let Err(SendError::ActorNotRunning((msg, sender))) = result else {
                panic!("expected the rejected message and reply sender");
            };
            assert_eq!(msg, mode);
            sender.send(());
            assert!(rx.await.unwrap().is_ok());
        }
    }
}

/// A collection of links to other actors that are notified when the actor dies.
///
/// Links are used for parent-child or sibling relationships, allowing actors to observe each other's lifecycle.
#[derive(Clone, Default)]
#[allow(missing_debug_implementations)]
pub(crate) struct Links(Arc<Mutex<HashMap<ActorId, Link>>>);

impl ops::Deref for Links {
    type Target = Mutex<HashMap<ActorId, Link>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub(crate) enum Link {
    Local(Box<dyn SignalMailbox>),
    #[cfg(feature = "remote")]
    Remote(std::borrow::Cow<'static, str>),
}

pub(crate) trait MessageHandler<M: Send + 'static>:
    DynClone + Send + Sync + 'static
{
    fn id(&self) -> ActorId;
    fn is_alive(&self) -> bool;
    fn downgrade(&self) -> WeakRecipient<M>;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
    fn is_current(&self) -> bool;
    fn stop_gracefully(&self) -> BoxFuture<'_, Result<(), SendError>>;
    fn kill(&self);
    fn wait_for_startup(&self) -> BoxFuture<'_, ()>;
    fn wait_for_shutdown(&self) -> BoxFuture<'_, ()>;

    #[allow(clippy::type_complexity)]
    fn tell(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<(), SendError<M>>>;
    fn try_tell(&self, msg: M) -> Result<(), SendError<M>>;
    fn blocking_tell(&self, msg: M) -> Result<(), SendError<M>>;
}

impl<A, M> MessageHandler<M> for ActorRef<A>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    #[inline]
    fn id(&self) -> ActorId {
        self.id
    }

    #[inline]
    fn is_alive(&self) -> bool {
        self.is_alive()
    }

    #[inline]
    fn downgrade(&self) -> WeakRecipient<M> {
        WeakRecipient::new(self.downgrade())
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.weak_count()
    }

    #[inline]
    fn is_current(&self) -> bool {
        self.is_current()
    }

    #[inline]
    fn stop_gracefully(&self) -> BoxFuture<'_, Result<(), SendError>> {
        self.stop_gracefully().boxed()
    }

    #[inline]
    fn kill(&self) {
        self.kill()
    }

    #[inline]
    fn wait_for_startup(&self) -> BoxFuture<'_, ()> {
        self.wait_for_startup().boxed()
    }

    #[inline]
    fn wait_for_shutdown(&self) -> BoxFuture<'_, ()> {
        self.wait_for_shutdown().boxed()
    }

    fn tell(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<(), SendError<M>>> {
        self.tell(msg)
            .mailbox_timeout_opt(mailbox_timeout)
            .send()
            .map_err(|err| {
                err.map_err(|_| unreachable!("tell requests don't handle the reply errors"))
            })
            .boxed()
    }

    fn try_tell(&self, msg: M) -> Result<(), SendError<M>> {
        self.tell(msg).try_send().map_err(|err| {
            err.map_err(|_| unreachable!("tell requests don't handle the reply errors"))
        })
    }

    fn blocking_tell(&self, msg: M) -> Result<(), SendError<M>> {
        self.tell(msg).blocking_send().map_err(|err| {
            err.map_err(|_| unreachable!("tell requests don't handle the reply errors"))
        })
    }
}

pub(crate) trait ReplyMessageHandler<M: Send + 'static, Ok: Send + 'static, Err: ReplyError>:
    MessageHandler<M>
{
    #[allow(clippy::type_complexity)]
    fn ask(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<Ok, SendError<M, Err>>>;
    fn try_ask(&self, msg: M) -> BoxFuture<'_, Result<Ok, SendError<M, Err>>>;
    fn blocking_ask(&self, msg: M) -> Result<Ok, SendError<M, Err>>;

    fn reply_downgrade(&self) -> WeakReplyRecipient<M, Ok, Err>;
    fn upcast(self: Box<Self>) -> Box<dyn MessageHandler<M>>;
}

impl<A, M, AR, Ok, Err> ReplyMessageHandler<M, Ok, Err> for ActorRef<A>
where
    AR: Reply<Ok = Ok, Error = Err>,
    A: Actor + Message<M, Reply = AR>,
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    #[allow(clippy::type_complexity)]
    fn ask(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<Ok, SendError<M, Err>>> {
        self.ask(msg)
            .mailbox_timeout_opt(mailbox_timeout)
            .send()
            .boxed()
    }
    fn try_ask(&self, msg: M) -> BoxFuture<'_, Result<Ok, SendError<M, Err>>> {
        Box::pin(self.ask(msg).try_send())
    }
    fn blocking_ask(&self, msg: M) -> Result<Ok, SendError<M, Err>> {
        self.ask(msg).blocking_send()
    }

    #[inline]
    fn reply_downgrade(&self) -> WeakReplyRecipient<M, Ok, Err> {
        WeakReplyRecipient::new(self.downgrade())
    }

    fn upcast(self: Box<Self>) -> Box<dyn MessageHandler<M>> {
        self
    }
}

trait WeakMessageHandler<M: Send + 'static>: DynClone + Send + Sync + 'static {
    fn id(&self) -> ActorId;
    fn upgrade(&self) -> Option<Recipient<M>>;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

impl<A, M> WeakMessageHandler<M> for WeakActorRef<A>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    #[inline]
    fn id(&self) -> ActorId {
        self.id
    }

    #[inline]
    fn upgrade(&self) -> Option<Recipient<M>> {
        self.upgrade().map(Recipient::new)
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.weak_count()
    }
}

trait WeakReplyMessageHandler<M: Send + 'static, Ok: Send + 'static, Err: ReplyError>:
    WeakMessageHandler<M>
{
    fn reply_upgrade(&self) -> Option<ReplyRecipient<M, Ok, Err>>;
}

impl<A, M, AR, Ok, Err> WeakReplyMessageHandler<M, Ok, Err> for WeakActorRef<A>
where
    AR: Reply<Ok = Ok, Error = Err>,
    A: Actor + Message<M, Reply = AR>,
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    #[inline]
    fn reply_upgrade(&self) -> Option<ReplyRecipient<M, Ok, Err>> {
        self.upgrade().map(ReplyRecipient::new)
    }
}

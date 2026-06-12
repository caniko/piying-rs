/// A request to send a message to a typed actor without any reply.
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct ReplyRecipientTellRequest<'a, M, Ok, Err, Tm>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    actor_ref: &'a ReplyRecipient<M, Ok, Err>,
    msg: M,
    mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

impl<'a, M, Ok, Err, Tm> ReplyRecipientTellRequest<'a, M, Ok, Err, Tm>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    pub(crate) fn new(
        actor_ref: &'a ReplyRecipient<M, Ok, Err>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))] called_at: &'static std::panic::Location<
            'static,
        >,
    ) -> Self
    where
        Tm: Default,
    {
        ReplyRecipientTellRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }

    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> ReplyRecipientTellRequest<'a, M, Ok, Err, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> ReplyRecipientTellRequest<'a, M, Ok, Err, WithRequestTimeout> {
        ReplyRecipientTellRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sends the message.
    pub async fn send(self) -> Result<(), SendError<M>>
    where
        Tm: Into<Option<Duration>>,
    {
        self.actor_ref
            .handler
            .tell(self.msg, self.mailbox_timeout.into())
            .await
    }
}

impl<M, Ok, Err> ReplyRecipientTellRequest<'_, M, Ok, Err, WithoutRequestTimeout>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub fn try_send(self) -> Result<(), SendError<M>> {
        self.actor_ref.handler.try_tell(self.msg)
    }

    /// Sends the message in a blocking context.
    pub fn blocking_send(self) -> Result<(), SendError<M>> {
        self.actor_ref.handler.blocking_tell(self.msg)
    }
}

impl<'a, M, Ok, Err, Tm> IntoFuture for ReplyRecipientTellRequest<'a, M, Ok, Err, Tm>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
    Tm: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<(), SendError<M>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.actor_ref
            .handler
            .tell(self.msg, self.mailbox_timeout.into())
    }
}

#[cfg(feature = "remote")]
pub use remote::RemoteTellRequest;


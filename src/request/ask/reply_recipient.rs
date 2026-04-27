/// A request to send a message to a typed actor with reply.
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct ReplyRecipientAskRequest<'a, M, Ok, Err, Tm>
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

impl<'a, M, Ok, Err, Tm> ReplyRecipientAskRequest<'a, M, Ok, Err, Tm>
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
        ReplyRecipientAskRequest {
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
    ) -> ReplyRecipientAskRequest<'a, M, Ok, Err, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> ReplyRecipientAskRequest<'a, M, Ok, Err, WithRequestTimeout> {
        ReplyRecipientAskRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sends the message.
    pub async fn send(self) -> Result<Ok, SendError<M, Err>>
    where
        Tm: Into<Option<Duration>>,
    {
        self.actor_ref
            .handler
            .ask(self.msg, self.mailbox_timeout.into())
            .await
    }
}

impl<M, Ok, Err> ReplyRecipientAskRequest<'_, M, Ok, Err, WithoutRequestTimeout>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub async fn try_send(self) -> Result<Ok, SendError<M, Err>> {
        self.actor_ref.handler.try_ask(self.msg).await
    }

    /// Sends the message in a blocking context.
    pub fn blocking_send(self) -> Result<Ok, SendError<M, Err>> {
        self.actor_ref.handler.blocking_ask(self.msg)
    }
}

impl<'a, M, Ok, Err, Tm> IntoFuture for ReplyRecipientAskRequest<'a, M, Ok, Err, Tm>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
    Tm: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<Ok, SendError<M, Err>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.actor_ref
            .handler
            .ask(self.msg, self.mailbox_timeout.into())
    }
}


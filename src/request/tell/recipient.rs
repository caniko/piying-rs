/// A request to send a message to a typed actor without any reply.
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
{
    actor_ref: &'a Recipient<M>,
    msg: M,
    mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

impl<'a, M, Tm> RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
{
    pub(crate) fn new(
        actor_ref: &'a Recipient<M>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))] called_at: &'static std::panic::Location<
            'static,
        >,
    ) -> Self
    where
        Tm: Default,
    {
        RecipientTellRequest {
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
    ) -> RecipientTellRequest<'a, M, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> RecipientTellRequest<'a, M, WithRequestTimeout> {
        RecipientTellRequest {
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

impl<M> RecipientTellRequest<'_, M, WithoutRequestTimeout>
where
    M: Send + 'static,
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

impl<'a, M, Tm> IntoFuture for RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
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


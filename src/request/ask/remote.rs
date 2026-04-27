/// A request to send a message to an actor, waiting for a reply.
#[cfg(feature = "remote")]
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct RemoteAskRequest<'a, A, M, Tm, Tr>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: Send + 'static,
{
    actor_ref: &'a actor::RemoteActorRef<A>,
    msg: &'a M,
    mailbox_timeout: Tm,
    reply_timeout: Tr,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

#[cfg(feature = "remote")]
impl<'a, A, M, Tm, Tr> RemoteAskRequest<'a, A, M, Tm, Tr>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: Send + 'static,
{
    pub(crate) fn new(
        actor_ref: &'a actor::RemoteActorRef<A>,
        msg: &'a M,
        #[cfg(all(debug_assertions, feature = "tracing"))] called_at: &'static std::panic::Location<
            'static,
        >,
    ) -> Self
    where
        Tm: Default,
        Tr: Default,
    {
        RemoteAskRequest {
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
    ) -> RemoteAskRequest<'a, A, M, WithRequestTimeout, Tr> {
        RemoteAskRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(Some(duration)),
            reply_timeout: self.reply_timeout,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sets the timeout for waiting for a reply from the actor.
    pub fn reply_timeout(
        self,
        duration: Duration,
    ) -> RemoteAskRequest<'a, A, M, Tm, WithRequestTimeout> {
        RemoteAskRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: self.mailbox_timeout,
            reply_timeout: WithRequestTimeout(Some(duration)),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sends the message.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, error::RemoteSendError<<A::Reply as Reply>::Error>>
    where
        M: remote::codec::Encode,
        Tm: Into<Option<Duration>>,
        Tr: Into<Option<Duration>>,
        <A::Reply as Reply>::Ok: remote::codec::Decode,
        <A::Reply as Reply>::Error: remote::codec::Decode,
    {
        remote_ask(
            self.actor_ref,
            self.msg,
            self.mailbox_timeout.into(),
            self.reply_timeout.into(),
            false,
        )
        .await
    }

    /// Enqueues the message into the remote actors mailbox, returning a pending reply which needs to be awaited.
    pub fn enqueue(self) -> Result<RemotePendingReply<A::Reply>, remote::codec::CodecError>
    where
        M: remote::codec::Encode,
        Tm: Into<Option<Duration>>,
        Tr: Into<Option<Duration>>,
        <A::Reply as Reply>::Ok: remote::codec::Decode,
        <A::Reply as Reply>::Error: remote::codec::Decode,
    {
        remote_ask_enqueue(
            self.actor_ref,
            self.msg,
            self.mailbox_timeout.into(),
            self.reply_timeout.into(),
            false,
        )
    }
}

#[cfg(feature = "remote")]
impl<A, M, Tr> RemoteAskRequest<'_, A, M, WithoutRequestTimeout, Tr>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: remote::codec::Encode + Send + 'static,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub async fn try_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, error::RemoteSendError<<A::Reply as Reply>::Error>>
    where
        Tr: Into<Option<Duration>>,
        <A::Reply as Reply>::Ok: remote::codec::Decode,
        <A::Reply as Reply>::Error: remote::codec::Decode,
    {
        remote_ask(
            self.actor_ref,
            self.msg,
            None,
            self.reply_timeout.into(),
            true,
        )
        .await
    }

    /// Tries to enqueue the message into the actors mailbox without waiting for mailbox capacity,
    /// returning a pending reply which needs to be awaited.
    pub fn try_enqueue(self) -> Result<RemotePendingReply<A::Reply>, remote::codec::CodecError>
    where
        M: remote::codec::Encode,
        Tr: Into<Option<Duration>>,
        <A::Reply as Reply>::Ok: remote::codec::Decode,
        <A::Reply as Reply>::Error: remote::codec::Decode,
    {
        remote_ask_enqueue(
            self.actor_ref,
            self.msg,
            None,
            self.reply_timeout.into(),
            true,
        )
    }
}

#[cfg(feature = "remote")]
impl<'a, A, M, Tm, Tr> IntoFuture for RemoteAskRequest<'a, A, M, Tm, Tr>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: remote::codec::Encode + Send + Sync + 'static,
    Tm: Into<Option<Duration>> + Send + 'static,
    Tr: Into<Option<Duration>> + Send + 'static,
    <A::Reply as Reply>::Ok: remote::codec::Decode,
    <A::Reply as Reply>::Error: remote::codec::Decode,
{
    type Output =
        Result<<A::Reply as Reply>::Ok, error::RemoteSendError<<A::Reply as Reply>::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.send().boxed()
    }
}

#[cfg(feature = "remote")]
fn remote_ask_enqueue<'a, A, M>(
    actor_ref: &'a actor::RemoteActorRef<A>,
    msg: &'a M,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<RemotePendingReply<A::Reply>, remote::codec::CodecError>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: remote::codec::Encode + Send + 'static,
    <A::Reply as Reply>::Ok: remote::codec::Decode,
    <A::Reply as Reply>::Error: remote::codec::Decode,
{
    use remote::*;
    use std::borrow::Cow;

    let actor_id = actor_ref.id();
    let (reply_tx, reply_rx) = oneshot::channel();
    actor_ref.send_to_swarm(remote::SwarmCommand::Ask {
        actor_id,
        actor_remote_id: Cow::Borrowed(<A as remote::RemoteActor>::REMOTE_ID),
        message_remote_id: Cow::Borrowed(<A as remote::RemoteMessage<M>>::REMOTE_ID),
        payload: msg.encode()?,
        mailbox_timeout,
        reply_timeout,
        immediate,
        reply: reply_tx,
    });

    let fut = async move {
        match reply_rx.await.unwrap() {
            messaging::SwarmResponse::Ask(res) => match res {
                Ok(payload) => Ok(<A::Reply as Reply>::Ok::decode(&payload)
                    .map_err(error::RemoteSendError::DeserializeMessage)?),
                Err(err) => Err(err
                    .into_bytes_error()
                    .map_err(|err| match <A::Reply as Reply>::Error::decode(&err) {
                        Ok(err) => error::RemoteSendError::HandlerError(err),
                        Err(err) => error::RemoteSendError::DeserializeHandlerError(err),
                    })
                    .flatten()),
            },
            messaging::SwarmResponse::OutboundFailure(err) => Err(err
                .into_infallible()
                .map_err(|_| unreachable!("outbound failure doesn't contain handler errors"))),
            _ => panic!("unexpected response"),
        }
    };

    Ok(RemotePendingReply { fut: Box::pin(fut) })
}

#[cfg(feature = "remote")]
async fn remote_ask<'a, A, M>(
    actor_ref: &'a actor::RemoteActorRef<A>,
    msg: &'a M,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<<A::Reply as Reply>::Ok, error::RemoteSendError<<A::Reply as Reply>::Error>>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: remote::codec::Encode + Send + 'static,
    <A::Reply as Reply>::Ok: remote::codec::Decode,
    <A::Reply as Reply>::Error: remote::codec::Decode,
{
    use remote::*;
    use std::borrow::Cow;

    let actor_id = actor_ref.id();
    let (reply_tx, reply_rx) = oneshot::channel();
    actor_ref.send_to_swarm(remote::SwarmCommand::Ask {
        actor_id,
        actor_remote_id: Cow::Borrowed(<A as remote::RemoteActor>::REMOTE_ID),
        message_remote_id: Cow::Borrowed(<A as remote::RemoteMessage<M>>::REMOTE_ID),
        payload: msg
            .encode()
            .map_err(error::RemoteSendError::SerializeMessage)?,
        mailbox_timeout,
        reply_timeout,
        immediate,
        reply: reply_tx,
    });

    match reply_rx.await.unwrap() {
        messaging::SwarmResponse::Ask(res) => match res {
            Ok(payload) => Ok(<A::Reply as Reply>::Ok::decode(&payload)
                .map_err(error::RemoteSendError::DeserializeMessage)?),
            Err(err) => Err(err
                .into_bytes_error()
                .map_err(|err| match <A::Reply as Reply>::Error::decode(&err) {
                    Ok(err) => error::RemoteSendError::HandlerError(err),
                    Err(err) => error::RemoteSendError::DeserializeHandlerError(err),
                })
                .flatten()),
        },
        messaging::SwarmResponse::OutboundFailure(err) => Err(err
            .into_infallible()
            .map_err(|_| unreachable!("outbound failure doesn't contain handler errors"))),
        _ => panic!("unexpected response"),
    }
}


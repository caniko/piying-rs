#[cfg(feature = "remote")]
mod remote {
    use std::{borrow::Cow, time::Duration};

    use tokio::sync::oneshot;

    use crate::{
        Actor,
        actor::RemoteActorRef,
        error::RemoteSendError,
        message::Message,
        remote::{RemoteActor, RemoteMessage, SwarmCommand, codec::Encode, messaging},
        request::{WithRequestTimeout, WithoutRequestTimeout},
    };

    /// A request to send a message to a remote actor without any reply.
    ///
    /// This can be thought of as "fire and forget".
    #[allow(missing_debug_implementations)]
    #[must_use = "request won't be sent without awaiting, or calling a send method"]
    pub struct RemoteTellRequest<'a, A, M, Tm>
    where
        A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
        M: Send + 'static,
    {
        actor_ref: &'a RemoteActorRef<A>,
        msg: &'a M,
        mailbox_timeout: Tm,
        #[cfg(all(debug_assertions, feature = "tracing"))]
        called_at: &'static std::panic::Location<'static>,
    }

    impl<'a, A, M, Tm> RemoteTellRequest<'a, A, M, Tm>
    where
        A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
        M: Send + 'static,
        Tm: Default,
    {
        pub(crate) fn new(
            actor_ref: &'a RemoteActorRef<A>,
            msg: &'a M,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: &'static std::panic::Location<'static>,
        ) -> Self {
            RemoteTellRequest {
                actor_ref,
                msg,
                mailbox_timeout: Tm::default(),
                #[cfg(all(debug_assertions, feature = "tracing"))]
                called_at,
            }
        }
    }

    impl<'a, A, M, Tm> RemoteTellRequest<'a, A, M, Tm>
    where
        A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
        M: Encode,
    {
        /// Sets the timeout for waiting for the actors mailbox to have capacity.
        pub fn mailbox_timeout(
            self,
            duration: Duration,
        ) -> RemoteTellRequest<'a, A, M, WithRequestTimeout> {
            self.mailbox_timeout_opt(Some(duration))
        }

        pub(crate) fn mailbox_timeout_opt(
            self,
            duration: Option<Duration>,
        ) -> RemoteTellRequest<'a, A, M, WithRequestTimeout> {
            RemoteTellRequest {
                actor_ref: self.actor_ref,
                msg: self.msg,
                mailbox_timeout: WithRequestTimeout(duration),
                #[cfg(all(debug_assertions, feature = "tracing"))]
                called_at: self.called_at,
            }
        }
    }

    impl<A, M, Tm> RemoteTellRequest<'_, A, M, Tm>
    where
        A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
        M: Encode,
        Tm: Into<Option<Duration>>,
    {
        /// Sends the message fire-and-forget style (fast, no delivery confirmation).
        pub fn send(self) -> Result<(), RemoteSendError> {
            remote_tell(self.actor_ref, self.msg, self.mailbox_timeout.into(), false)
        }

        /// Sends the message and waits for delivery acknowledgment (reliable, slower).
        pub async fn send_ack(self) -> Result<(), RemoteSendError> {
            remote_tell_ack(self.actor_ref, self.msg, self.mailbox_timeout.into(), false).await
        }
    }

    impl<A, M> RemoteTellRequest<'_, A, M, WithoutRequestTimeout>
    where
        A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
        M: Encode,
    {
        /// Tries to send the message fire-and-forget style, failing immediately if mailbox is full.
        pub async fn try_send(self) -> Result<(), RemoteSendError> {
            remote_tell(self.actor_ref, self.msg, self.mailbox_timeout.into(), true)
        }

        /// Tries to send the message with acknowledgment, failing immediately if mailbox is full.
        pub async fn try_send_ack(self) -> Result<(), RemoteSendError> {
            remote_tell_ack(self.actor_ref, self.msg, self.mailbox_timeout.into(), true).await
        }
    }

    fn remote_tell<A, M>(
        actor_ref: &RemoteActorRef<A>,
        msg: &M,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<(), RemoteSendError>
    where
        A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
        M: Encode,
    {
        let actor_id = actor_ref.id();
        actor_ref.send_to_swarm(SwarmCommand::Tell {
            actor_id,
            actor_remote_id: Cow::Borrowed(<A as RemoteActor>::REMOTE_ID),
            message_remote_id: Cow::Borrowed(<A as RemoteMessage<M>>::REMOTE_ID),
            payload: msg.encode().map_err(RemoteSendError::SerializeMessage)?,
            mailbox_timeout,
            immediate,
            reply: None,
        });

        Ok(())
    }

    async fn remote_tell_ack<A, M>(
        actor_ref: &RemoteActorRef<A>,
        msg: &M,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<(), RemoteSendError>
    where
        A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
        M: Encode,
    {
        let actor_id = actor_ref.id();
        let (reply_tx, reply_rx) = oneshot::channel();
        actor_ref.send_to_swarm(SwarmCommand::Tell {
            actor_id,
            actor_remote_id: Cow::Borrowed(<A as RemoteActor>::REMOTE_ID),
            message_remote_id: Cow::Borrowed(<A as RemoteMessage<M>>::REMOTE_ID),
            payload: msg.encode().map_err(RemoteSendError::SerializeMessage)?,
            mailbox_timeout,
            immediate,
            reply: Some(reply_tx),
        });

        match reply_rx.await.unwrap() {
            messaging::SwarmResponse::Tell(res) => match res {
                Ok(()) => Ok(()),
                Err(err) => Err(err.into_infallible()),
            },
            messaging::SwarmResponse::OutboundFailure(err) => Err(err
                .into_infallible()
                .map_err(|_| unreachable!("outbound failure doesn't contain handler errors"))),
            _ => panic!("unexpected response"),
        }
    }
}


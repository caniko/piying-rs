use std::{future::IntoFuture, time::Duration};

use futures::{FutureExt, future::BoxFuture};
use tokio::task::JoinHandle;

use crate::{
    Actor,
    actor::{ActorRef, Recipient, ReplyRecipient},
    error::SendError,
    mailbox::Signal,
    message::Message,
    reply::ReplyError,
};

use super::{WithRequestTimeout, WithoutRequestTimeout};

/// A request to send a message to an actor without any reply.
///
/// This can be thought of as "fire and forget".
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct TellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    actor_ref: &'a ActorRef<A>,
    msg: M,
    mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

impl<'a, A, M, Tm> TellRequest<'a, A, M, Tm>
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
    {
        TellRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }

    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(self, duration: Duration) -> TellRequest<'a, A, M, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> TellRequest<'a, A, M, WithRequestTimeout> {
        TellRequest {
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
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: None,
            sent_within_actor: self.actor_ref.is_current(),
        };

        let tx = self.actor_ref.mailbox_sender();
        if tx.capacity().is_some() {
            #[cfg(all(debug_assertions, feature = "tracing"))]
            warn_deadlock(
                self.actor_ref,
                "An actor is sending a `tell` request to itself using a bounded mailbox, which may lead to a deadlock. To avoid this, use `.try_send()`.",
                self.called_at,
            );
        }

        match self.mailbox_timeout.into() {
            Some(timeout) => Ok(tx.send_timeout(signal, timeout).await?),
            None => Ok(tx.send(signal).await?),
        }
    }

    /// Sends a message to the actor after a delay.
    ///
    /// Returns a [`JoinHandle`] that can be used to cancel the scheduled send via
    /// [`JoinHandle::abort`]. The handle can safely be ignored for fire-and-forget usage.
    ///
    /// Awaiting the handle will block until the delay has elapsed and the message
    /// has been sent; this is rarely what you want.
    pub fn send_after(self, duration: Duration) -> JoinHandle<Result<(), SendError<M>>>
    where
        Tm: Into<Option<Duration>>,
    {
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: None,
            sent_within_actor: self.actor_ref.is_current(),
        };

        let tx = self.actor_ref.mailbox_sender().clone();
        if tx.capacity().is_some() {
            #[cfg(all(debug_assertions, feature = "tracing"))]
            warn_deadlock(
                self.actor_ref,
                "An actor is sending a `tell` request to itself using a bounded mailbox, which may lead to a deadlock. To avoid this, use `.try_send()`.",
                self.called_at,
            );
        }
        let mailbox_timeout = self.mailbox_timeout.into();

        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            match mailbox_timeout {
                Some(timeout) => Ok(tx.send_timeout(signal, timeout).await?),
                None => Ok(tx.send(signal).await?),
            }
        })
    }
}

impl<A, M> TellRequest<'_, A, M, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub fn try_send(self) -> Result<(), SendError<M>> {
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: None,
            sent_within_actor: self.actor_ref.is_current(),
        };

        Ok(self.actor_ref.mailbox_sender().try_send(signal)?)
    }

    /// Sends the message in a blocking context.
    pub fn blocking_send(self) -> Result<(), SendError<M>> {
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: None,
            sent_within_actor: self.actor_ref.is_current(),
        };

        let tx = self.actor_ref.mailbox_sender();
        if tx.capacity().is_some() {
            #[cfg(all(debug_assertions, feature = "tracing"))]
            warn_deadlock(
                self.actor_ref,
                "An actor is sending a blocking `tell` request to itself using a bounded mailbox, which may lead to a deadlock.",
                self.called_at,
            );
        }

        Ok(self.actor_ref.mailbox_sender().blocking_send(signal)?)
    }
}

impl<'a, A, M, Tm> IntoFuture for TellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<(), SendError<M>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.send().boxed()
    }
}

include!("tell/recipient.rs");
include!("tell/reply_recipient.rs");
include!("tell/remote.rs");
#[cfg(all(debug_assertions, feature = "tracing"))]
fn warn_deadlock<A: Actor>(
    actor_ref: &ActorRef<A>,
    msg: &'static str,
    called_at: &'static std::panic::Location<'static>,
) {
    use tracing::warn;

    if actor_ref.mailbox_sender().capacity().is_some() && actor_ref.is_current() {
        warn!("At {called_at}, {msg}");
    }
}

include!("tell/tests.rs");

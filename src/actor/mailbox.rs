mod bounded;
mod unbounded;

use dyn_clone::DynClone;
use futures::{future::BoxFuture, Future};
use tokio::sync::oneshot;

use crate::{
    error::{ActorStopReason, BoxSendError, SendError},
    message::{BoxReply, DynMessage},
    Actor,
};

use super::{ActorID, ActorRef};

/// A trait defining the behaviour and functionality of a mailbox.
pub trait Mailbox<A: Actor>: SignalMailbox + Clone + Send + Sync {
    type Receiver: MailboxReceiver<A>;
    type WeakMailbox: WeakMailbox<StrongMailbox = Self>;

    fn default_mailbox() -> (Self, Self::Receiver);
    fn send(
        &self,
        signal: Signal<A>,
    ) -> impl Future<Output = Result<(), SendError<Signal<A>>>> + Send + '_;
    fn closed(&self) -> impl Future<Output = ()> + '_;
    fn is_closed(&self) -> bool;
    fn downgrade(&self) -> Self::WeakMailbox;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

/// A mailbox receiver.
pub trait MailboxReceiver<A: Actor>: Send + 'static {
    fn recv(&mut self) -> impl Future<Output = Option<Signal<A>>> + Send + '_;
}

/// A weak mailbox which can be upraded.
pub trait WeakMailbox: SignalMailbox + Clone + Send + Sync {
    type StrongMailbox;

    fn upgrade(&self) -> Option<Self::StrongMailbox>;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

#[allow(missing_debug_implementations)]
#[doc(hidden)]
pub enum Signal<A: Actor> {
    StartupFinished,
    Message {
        message: Box<dyn DynMessage<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    },
    LinkDied {
        id: ActorID,
        reason: ActorStopReason,
    },
    Stop,
}

impl<A: Actor> Signal<A> {
    pub(crate) fn downcast_message<M>(self) -> Option<M>
    where
        M: 'static,
    {
        match self {
            Signal::Message { message, .. } => message.as_any().downcast().ok().map(|v| *v),
            _ => None,
        }
    }
}

#[doc(hidden)]
pub trait SignalMailbox: DynClone + Send {
    fn signal_startup_finished(&self) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>>;
}

dyn_clone::clone_trait_object!(SignalMailbox);

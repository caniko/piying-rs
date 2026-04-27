//! A multi-producer, single-consumer queue for sending messages and signals between actors.
//!
//! An actor mailbox is a channel which stores pending messages and signals for an actor to process sequentially.

use std::{
    fmt,
    task::{Context, Poll},
    time::Duration,
};

use dyn_clone::DynClone;
use futures::{FutureExt, future::BoxFuture};
use tokio::sync::mpsc::{self, error::TryRecvError};

use crate::{
    Actor,
    actor::{ActorId, ActorRef},
    error::{ActorStopReason, SendError},
    message::BoxMessage,
    reply::BoxReplySender,
};

/// Creates a bounded mailbox for communicating between actors with backpressure.
///
/// _See tokio's [`mpsc::channel`] docs for more info._
///
/// [`mpsc::channel`]: tokio::sync::mpsc::channel
pub fn bounded<A: Actor>(buffer: usize) -> (MailboxSender<A>, MailboxReceiver<A>) {
    let (tx, rx) = mpsc::channel(buffer);
    #[cfg(feature = "hotpath")]
    let (tx, rx) = hotpath::channel!((tx, rx), label = A::name());
    (
        MailboxSender {
            inner: MailboxSenderInner::Bounded(tx),
            #[cfg(feature = "metrics")]
            messages_sent: metrics::counter!("thespis_messages_sent", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            lifecycle_signals_sent: metrics::counter!("thespis_lifecycle_sent", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            link_died_signals_sent: metrics::counter!("thespis_link_died_sent", "actor_name" => A::name()),
        },
        MailboxReceiver {
            inner: MailboxReceiverInner::Bounded(rx),
            #[cfg(feature = "metrics")]
            messages_received: metrics::counter!("thespis_messages_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            lifecycle_signals_received: metrics::counter!("thespis_lifecycle_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            link_died_signals_received: metrics::counter!("thespis_link_died_received", "actor_name" => A::name()),
        },
    )
}

/// Creates an unbounded mailbox for communicating between actors without backpressure.
///
/// See tokio's [`mpsc::unbounded_channel`] docs for more info.
///
/// [`mpsc::unbounded_channel`]: tokio::sync::mpsc::unbounded_channel
pub fn unbounded<A: Actor>() -> (MailboxSender<A>, MailboxReceiver<A>) {
    let (tx, rx) = mpsc::unbounded_channel();
    #[cfg(feature = "hotpath")]
    let (tx, rx) = hotpath::channel!((tx, rx), label = A::name());
    (
        MailboxSender {
            inner: MailboxSenderInner::Unbounded(tx),
            #[cfg(feature = "metrics")]
            messages_sent: metrics::counter!("thespis_messages_sent", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            lifecycle_signals_sent: metrics::counter!("thespis_lifecycle_sent", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            link_died_signals_sent: metrics::counter!("thespis_link_died_sent", "actor_name" => A::name()),
        },
        MailboxReceiver {
            inner: MailboxReceiverInner::Unbounded(rx),
            #[cfg(feature = "metrics")]
            messages_received: metrics::counter!("thespis_messages_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            lifecycle_signals_received: metrics::counter!("thespis_lifecycle_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            link_died_signals_received: metrics::counter!("thespis_link_died_received", "actor_name" => A::name()),
        },
    )
}

/// Sends messages and signals to the associated `MailboxReceiver`.
///
/// Instances are created by the [`bounded`] and [`unbounded`] functions.
pub struct MailboxSender<A: Actor> {
    inner: MailboxSenderInner<A>,
    #[cfg(feature = "metrics")]
    messages_sent: metrics::Counter,
    #[cfg(feature = "metrics")]
    lifecycle_signals_sent: metrics::Counter,
    #[cfg(feature = "metrics")]
    link_died_signals_sent: metrics::Counter,
}

enum MailboxSenderInner<A: Actor> {
    /// Bounded mailbox sender.
    Bounded(mpsc::Sender<Signal<A>>),
    /// Unbounded mailbox sender.
    Unbounded(mpsc::UnboundedSender<Signal<A>>),
}

#[cfg(feature = "metrics")]
enum SignalKind {
    Message,
    Lifecycle,
    LinkDied,
}

#[cfg(feature = "metrics")]
impl SignalKind {
    #[inline]
    fn apply_metric<A: Actor>(self, tx: &MailboxSender<A>) {
        match self {
            SignalKind::Message => tx.messages_sent.increment(1),
            SignalKind::Lifecycle => tx.lifecycle_signals_sent.increment(1),
            SignalKind::LinkDied => tx.link_died_signals_sent.increment(1),
        }
    }
}

#[cfg(feature = "metrics")]
impl<A: Actor> From<&Signal<A>> for SignalKind {
    #[inline]
    fn from(signal: &Signal<A>) -> Self {
        match signal {
            Signal::Message { .. } => SignalKind::Message,
            Signal::StartupFinished | Signal::Stop => SignalKind::Lifecycle,
            Signal::LinkDied { .. } => SignalKind::LinkDied,
        }
    }
}

impl<A: Actor> MailboxSender<A> {
    /// Sends a value, waiting until there is capacity.
    ///
    /// See tokio's [`mpsc::Sender::send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::send`]: tokio::sync::mpsc::Sender::send
    /// [`mpsc::UnboundedSender::send`]: tokio::sync::mpsc::UnboundedSender::send
    pub async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        #[cfg(feature = "metrics")]
        let signal_kind = SignalKind::from(&signal);

        let res = match &self.inner {
            MailboxSenderInner::Bounded(tx) => tx.send(signal).await,
            MailboxSenderInner::Unbounded(tx) => tx.send(signal),
        };

        #[cfg(feature = "metrics")]
        if res.is_ok() {
            signal_kind.apply_metric(self);
        }

        res
    }

    /// Attempts to immediately send a message on this `Sender`.
    /// Unbounded mailboxes will always have capacity.
    ///
    /// See tokio's [`mpsc::Sender::try_send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::try_send`]: tokio::sync::mpsc::Sender::try_send
    /// [`mpsc::UnboundedSender::send`]: tokio::sync::mpsc::UnboundedSender::send
    #[allow(clippy::result_large_err)]
    pub fn try_send(&self, signal: Signal<A>) -> Result<(), mpsc::error::TrySendError<Signal<A>>> {
        #[cfg(feature = "metrics")]
        let signal_kind = SignalKind::from(&signal);

        let res = match &self.inner {
            MailboxSenderInner::Bounded(tx) => tx.try_send(signal),
            MailboxSenderInner::Unbounded(tx) => tx
                .send(signal)
                .map_err(|err| mpsc::error::TrySendError::Closed(err.0)),
        };

        #[cfg(feature = "metrics")]
        if res.is_ok() {
            signal_kind.apply_metric(self);
        }

        res
    }

    /// Sends a value, waiting until there is capacity, but only for a limited time.
    /// Unbounded mailboxes will never need to wait for capacity.
    ///
    /// See tokio's [`mpsc::Sender::try_send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::try_send`]: tokio::sync::mpsc::Sender::try_send
    /// [`mpsc::UnboundedSender::send`]: tokio::sync::mpsc::UnboundedSender::send
    pub async fn send_timeout(
        &self,
        signal: Signal<A>,
        timeout: Duration,
    ) -> Result<(), mpsc::error::SendTimeoutError<Signal<A>>> {
        #[cfg(feature = "metrics")]
        let signal_kind = SignalKind::from(&signal);

        let res = match &self.inner {
            MailboxSenderInner::Bounded(tx) => tx.send_timeout(signal, timeout).await,
            MailboxSenderInner::Unbounded(tx) => tx
                .send(signal)
                .map_err(|err| mpsc::error::SendTimeoutError::Closed(err.0)),
        };

        #[cfg(feature = "metrics")]
        if res.is_ok() {
            signal_kind.apply_metric(self);
        }

        res
    }

    /// Blocking send to call outside of asynchronous contexts.
    /// Unbounded mailboxes will never block due to unbounded capacity.
    ///
    /// See tokio's [`mpsc::Sender::blocking_send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::blocking_send`]: tokio::sync::mpsc::Sender::blocking_send
    /// [`mpsc::UnboundedSender::send`]: tokio::sync::mpsc::UnboundedSender::send
    #[allow(clippy::result_large_err)]
    pub fn blocking_send(
        &self,
        signal: Signal<A>,
    ) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        #[cfg(feature = "metrics")]
        let signal_kind = SignalKind::from(&signal);

        let res = match &self.inner {
            MailboxSenderInner::Bounded(tx) => tx.blocking_send(signal),
            MailboxSenderInner::Unbounded(tx) => tx.send(signal),
        };

        #[cfg(feature = "metrics")]
        if res.is_ok() {
            signal_kind.apply_metric(self);
        }

        res
    }

    /// Completes when the receiver has dropped.
    ///
    /// See tokio's [`mpsc::Sender::closed`] and [`mpsc::UnboundedSender::closed`] docs for more info.
    ///
    /// [`mpsc::Sender::closed`]: tokio::sync::mpsc::Sender::closed
    /// [`mpsc::UnboundedSender::closed`]: tokio::sync::mpsc::UnboundedSender::closed
    pub async fn closed(&self) {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => tx.closed().await,
            MailboxSenderInner::Unbounded(tx) => tx.closed().await,
        }
    }

    /// Checks if the channel has been closed. This happens when the
    /// [`MailboxReceiver`] is dropped, or when the [`MailboxReceiver::close`] method is
    /// called.
    ///
    /// See tokio's [`mpsc::Sender::is_closed`] and [`mpsc::UnboundedSender::is_closed`] docs for more info.
    ///
    /// [`mpsc::Sender::is_closed`]: tokio::sync::mpsc::Sender::is_closed
    /// [`mpsc::UnboundedSender::is_closed`]: tokio::sync::mpsc::UnboundedSender::is_closed
    pub fn is_closed(&self) -> bool {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => tx.is_closed(),
            MailboxSenderInner::Unbounded(tx) => tx.is_closed(),
        }
    }

    /// Returns `true` if senders belong to the same channel.
    ///
    /// See tokio's [`mpsc::Sender::same_channel`] and [`mpsc::UnboundedSender::same_channel`] docs for more info.
    ///
    /// [`mpsc::Sender::same_channel`]: tokio::sync::mpsc::Sender::same_channel
    /// [`mpsc::UnboundedSender::same_channel`]: tokio::sync::mpsc::UnboundedSender::same_channel
    pub fn same_channel(&self, other: &MailboxSender<A>) -> bool {
        match (&self.inner, &other.inner) {
            (MailboxSenderInner::Bounded(a), MailboxSenderInner::Bounded(b)) => a.same_channel(b),
            (MailboxSenderInner::Bounded(_), MailboxSenderInner::Unbounded(_)) => false,
            (MailboxSenderInner::Unbounded(_), MailboxSenderInner::Bounded(_)) => false,
            (MailboxSenderInner::Unbounded(a), MailboxSenderInner::Unbounded(b)) => {
                a.same_channel(b)
            }
        }
    }

    /// Returns the current capacity of the channel, if bounded.
    /// Unbounded channels return `None`.
    ///
    /// See tokio's [`mpsc::Sender::capacity`] docs for more info.
    ///
    /// [`mpsc::Sender::capacity`]: tokio::sync::mpsc::Sender::capacity
    pub fn capacity(&self) -> Option<usize> {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => Some(tx.capacity()),
            MailboxSenderInner::Unbounded(_) => None,
        }
    }

    /// Converts the `MailboxSender` to a [`WeakMailboxSender`] that does not count
    /// towards RAII semantics, i.e. if all `Sender` instances of the
    /// channel were dropped and only `WeakMailboxSender` instances remain,
    /// the channel is closed.
    ///
    /// See tokio's [`mpsc::Sender::downgrade`] and [`mpsc::UnboundedSender::downgrade`] docs for more info.
    ///
    /// [`mpsc::Sender::downgrade`]: tokio::sync::mpsc::Sender::downgrade
    /// [`mpsc::UnboundedSender::downgrade`]: tokio::sync::mpsc::UnboundedSender::downgrade
    pub fn downgrade(&self) -> WeakMailboxSender<A> {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => WeakMailboxSender {
                inner: WeakMailboxSenderInner::Bounded(tx.downgrade()),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
            MailboxSenderInner::Unbounded(tx) => WeakMailboxSender {
                inner: WeakMailboxSenderInner::Unbounded(tx.downgrade()),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
        }
    }

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Sender::strong_count`] and [`mpsc::UnboundedSender::strong_count`] docs for more info.
    ///
    /// [`mpsc::Sender::strong_count`]: tokio::sync::mpsc::Sender::strong_count
    /// [`mpsc::UnboundedSender::strong_count`]: tokio::sync::mpsc::UnboundedSender::strong_count
    pub fn strong_count(&self) -> usize {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => tx.strong_count(),
            MailboxSenderInner::Unbounded(tx) => tx.strong_count(),
        }
    }

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Sender::weak_count`] and [`mpsc::UnboundedSender::weak_count`] docs for more info.
    ///
    /// [`mpsc::Sender::weak_count`]: tokio::sync::mpsc::Sender::weak_count
    /// [`mpsc::UnboundedSender::weak_count`]: tokio::sync::mpsc::UnboundedSender::weak_count
    pub fn weak_count(&self) -> usize {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => tx.weak_count(),
            MailboxSenderInner::Unbounded(tx) => tx.weak_count(),
        }
    }
}

impl<A: Actor> Clone for MailboxSender<A> {
    fn clone(&self) -> Self {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => MailboxSender {
                inner: MailboxSenderInner::Bounded(tx.clone()),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
            MailboxSenderInner::Unbounded(tx) => MailboxSender {
                inner: MailboxSenderInner::Unbounded(tx.clone()),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
        }
    }
}

impl<A: Actor> fmt::Debug for MailboxSender<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            MailboxSenderInner::Bounded(tx) => f.debug_tuple("Bounded").field(tx).finish(),
            MailboxSenderInner::Unbounded(tx) => f.debug_tuple("Unbounded").field(tx).finish(),
        }
    }
}

include!("mailbox/weak_sender.rs");
include!("mailbox/receiver.rs");
include!("mailbox/signal.rs");

//! Constructs for handling replies and errors in Thespis's actor communication.
//!
//! This module provides the [`Reply`] trait and associated structures for managing message replies within the actor
//! system. It enables actors to communicate effectively, handling both successful outcomes and errors through a
//! unified interface.
//!
//! **Reply Trait Overview**
//!
//! The `Reply` trait plays a crucial role in Thespis by defining how actors respond to messages.
//! It is implemented for a variety of common types, facilitating easy adoption and use.
//! Special attention is given to the `Result` and [`DelegatedReply`] types:
//! - Implementations for `Result` allow errors returned by actor handlers to be communicated back as
//!   [`SendError::HandlerError`], integrating closely with Rust’s error handling patterns.
//! - The `DelegatedReply` type signifies that the actual reply will be managed by another part of the system,
//!   supporting asynchronous and decoupled communication workflows.
//! - Importantly, when messages are sent asynchronously with [`tell`](crate::actor::ActorRef::tell) and an error is returned by the actor
//!   without a direct means for the caller to handle it (due to the absence of a reply expectation), the error is treated
//!   as a panic within the actor. This behavior will trigger the actor's [`on_panic`](crate::actor::Actor::on_panic) hook, which may result in the actor
//!   being restarted or stopped based on the [Actor] implementation (which stops the actor by default).
//!
//! The `Reply` trait, by encompassing a broad range of types and defining specific behaviors for error handling,
//! ensures that actors can manage their communication responsibilities efficiently and effectively.

use std::{
    any,
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque},
    fmt,
    marker::PhantomData,
    num::{
        NonZeroI8, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI128, NonZeroIsize, NonZeroU8,
        NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU128, NonZeroUsize,
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Once, RwLock,
        atomic::{AtomicBool, AtomicIsize, AtomicPtr, AtomicUsize},
    },
    thread::Thread,
};

#[cfg(target_has_atomic = "8")]
use std::sync::atomic::{AtomicI8, AtomicU8};
#[cfg(target_has_atomic = "16")]
use std::sync::atomic::{AtomicI16, AtomicU16};
#[cfg(target_has_atomic = "32")]
use std::sync::atomic::{AtomicI32, AtomicU32};
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::{AtomicI64, AtomicU64};

use downcast_rs::{DowncastSend, impl_downcast};
use futures::Future;
use tokio::sync::oneshot;

use crate::{
    Actor,
    actor::{
        ActorId, ActorRef, PreparedActor, Recipient, ReplyRecipient, WeakActorRef, WeakRecipient,
        WeakReplyRecipient,
    },
    error::{ActorStopReason, BoxSendError, Infallible, PanicError, SendError},
    mailbox::{MailboxReceiver, MailboxSender},
    message::{BoxReply, Context},
};

/// A boxed reply sender which will be downcast to the correct type when receiving a reply.
///
/// This is reserved for advanced use cases, and misuse of this can result in panics.
pub type BoxReplySender = oneshot::Sender<Result<BoxReply, BoxSendError>>;

/// A reply value.
///
/// If an Err is returned by a handler, and is unhandled by the caller (ie, the message was sent asynchronously with `tell`),
/// then the error is treated as a panic in the actor.
///
/// This is implemented for all std lib types, and can be implemented on custom types manually or with the derive
/// macro.
///
/// # Example
///
/// ```
/// use thespis::Reply;
///
/// #[derive(Reply)]
/// pub struct Foo { }
/// ```
pub trait Reply: Send + 'static {
    /// The success type in the reply.
    type Ok: Send + 'static;
    /// The error type in the reply.
    type Error: ReplyError;
    /// The type sent back to the receiver.
    ///
    /// In almost all cases this will be `Self`. The only exception is the `DelegatedReply` type.
    type Value: Reply;

    /// Converts a reply to a `Result`.
    fn to_result(self) -> Result<Self::Ok, Self::Error>;

    /// Converts the reply into a `Box<any::Any + Send>` if it's an Err, otherwise `None`.
    fn into_any_err(self) -> Option<Box<dyn ReplyError>>;

    /// Converts the type to Self::Reply.
    ///
    /// In almost all cases, this will simply return itself.
    fn into_value(self) -> Self::Value;

    /// Downcasts a `Box<dyn Any>` into the `Self::Ok` type.
    fn downcast_ok(ok: Box<dyn any::Any>) -> Self::Ok {
        *ok.downcast().unwrap()
    }

    /// Downcasts a `Box<dyn Any>` into a `Self::Error` type.
    fn downcast_err<M: 'static>(err: BoxSendError) -> SendError<M, Self::Error> {
        err.downcast()
    }
}

/// A mechanism for sending replies back to the original requester in a message exchange.
///
/// `ReplySender` encapsulates the functionality to send a response back to wherever
/// a request was initiated. It is typically used in scenarios where the
/// processing of a request is delegated to another actor within the system.
/// Upon completion of the request handling, `ReplySender` is used to send the result back,
/// ensuring that the flow of communication is maintained and the requester receives the
/// necessary response.
///
/// This type is designed to be used once per message received; it consumes itself upon sending
/// a reply to enforce a single use and prevent multiple replies to a single message.
///
/// # Usage
///
/// A `ReplySender` is obtained as part of the delegation process when handling a message. It should
/// be used to send a reply once the requested data is available or the operation is complete.
///
/// The `ReplySender` provides a clear and straightforward interface for completing the message handling cycle,
/// facilitating efficient and organized communication within the system.
#[must_use = "the receiver expects a reply to be sent"]
pub struct ReplySender<R: ?Sized> {
    tx: BoxReplySender,
    phantom: PhantomData<R>,
}

impl<R> ReplySender<R> {
    pub(crate) fn new(tx: BoxReplySender) -> Self {
        ReplySender {
            tx,
            phantom: PhantomData,
        }
    }

    /// Converts the reply sender to a generic `BoxReplySender`.
    pub fn boxed(self) -> BoxReplySender {
        self.tx
    }

    /// Sends a reply using the current `ReplySender`.
    ///
    /// Consumes the `ReplySender`, sending the specified reply to the original
    /// requester. This method is the final step in the response process for
    /// delegated replies, ensuring that the message's intended recipient receives
    /// the necessary data or acknowledgment.
    ///
    /// The method takes ownership of the `ReplySender` to prevent multiple uses,
    /// aligning with the one-time use pattern typical in actor-based messaging for
    /// reply mechanisms. Once called, the `ReplySender` cannot be used again,
    /// enforcing a single-reply guarantee for each message received.
    ///
    /// # Note
    ///
    /// It is crucial to send a reply for every received message to avoid leaving the
    /// requester in a state of indefinite waiting. Failure to do so can lead to deadlocks
    /// or wasted resources in waiting for a response that will never arrive.
    pub fn send(self, reply: R)
    where
        R: Reply,
    {
        let _ = self.tx.send(
            reply
                .to_result()
                .map(|value| Box::new(value) as BoxReply)
                .map_err(|err| BoxSendError::HandlerError(Box::new(err))),
        );
    }

    pub(crate) fn cast<R2>(self) -> ReplySender<R2> {
        ReplySender {
            tx: self.tx,
            phantom: PhantomData,
        }
    }
}

impl<R: ?Sized> fmt::Debug for ReplySender<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplySender")
            .field("tx", &self.tx)
            .field("phantom", &self.phantom)
            .finish()
    }
}

/// An error type which can be used in replies.
///
/// This is implemented for all types which are `Debug + Send + 'static`.
pub trait ReplyError: DowncastSend + fmt::Debug + 'static {}
impl<T> ReplyError for T where T: fmt::Debug + Send + 'static {}
impl_downcast!(ReplyError);

/// A marker type indicating that the reply to a message will be handled elsewhere.
///
/// This structure is created by the [`reply_sender`] method on [`Context`].
///
/// [`reply_sender`]: method@crate::message::Context::reply_sender
/// [`Context`]: struct@crate::message::Context
#[must_use = "the deligated reply should be returned by the handler"]
#[derive(Clone, Copy, Debug)]
pub struct DelegatedReply<R> {
    phantom: PhantomData<fn() -> R>,
}

impl<R> DelegatedReply<R> {
    pub(crate) fn new() -> Self {
        DelegatedReply {
            phantom: PhantomData,
        }
    }
}

impl<R> Reply for DelegatedReply<R>
where
    R: Reply,
{
    type Ok = R::Ok;
    type Error = R::Error;
    type Value = R::Value;

    fn to_result(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("a DeligatedReply cannot be converted to a result and is only a marker type")
    }

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        None
    }

    fn into_value(self) -> Self::Value {
        unimplemented!("a DeligatedReply cannot be converted to a value and is only a marker type")
    }
}

include!("reply/forwarded.rs");
include!("reply/standard_impls.rs");
include!("reply/tests.rs");

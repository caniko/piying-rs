use std::{
    cell::Cell,
    cmp,
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    ops,
    sync::Arc,
    time::Duration,
};

use dyn_clone::DynClone;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, future::BoxFuture, stream::AbortHandle};
use tokio::{
    sync::{Mutex, SetOnce},
    task::JoinHandle,
    task_local,
};

#[cfg(feature = "remote")]
use std::borrow::Cow;
#[cfg(feature = "remote")]
use std::marker::PhantomData;

#[cfg(feature = "remote")]
use crate::remote;
#[cfg(feature = "remote")]
use crate::request;

use crate::{
    Actor, Reply,
    error::{self, HookError, Infallible, PanicError, SendError},
    mailbox::{MailboxSender, Signal, SignalMailbox, WeakMailboxSender},
    message::{Message, StreamMessage},
    reply::ReplyError,
    request::{
        AskRequest, RecipientTellRequest, ReplyRecipientAskRequest, ReplyRecipientTellRequest,
        TellRequest, WithoutRequestTimeout,
    },
};

use super::id::ActorId;

task_local! {
    pub(crate) static CURRENT_ACTOR_ID: ActorId;
}
thread_local! {
    pub(crate) static CURRENT_THREAD_ACTOR_ID: Cell<Option<ActorId>> = const { Cell::new(None) };
}

/// A reference to an actor, used for sending messages and managing its lifecycle.
///
/// An `ActorRef` allows interaction with an actor through message passing, both for asking (waiting for a reply)
/// and telling (without waiting for a reply). It also provides utilities for managing the actor's state,
/// such as checking if the actor is alive, registering the actor under a name, and stopping the actor gracefully.
pub struct ActorRef<A: Actor> {
    id: ActorId,
    mailbox_sender: MailboxSender<A>,
    abort_handle: AbortHandle,
    pub(crate) links: Links,
    pub(crate) startup_result: Arc<SetOnce<Result<(), PanicError>>>,
    pub(crate) shutdown_result: Arc<SetOnce<Result<(), PanicError>>>,
}

include!("actor_ref/lifecycle_methods.rs");
include!("actor_ref/messaging_methods.rs");
impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            id: self.id,
            mailbox_sender: self.mailbox_sender.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_result: self.startup_result.clone(),
            shutdown_result: self.shutdown_result.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("ActorRef");
        d.field("id", &self.id);
        match self.links.try_lock() {
            Ok(guard) => {
                d.field("links", &guard.keys());
            }
            Err(_) => {
                d.field("links", &format_args!("<locked>"));
            }
        }
        d.finish()
    }
}

impl<A: Actor> PartialEq for ActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<A: Actor> Eq for ActorRef<A> {}

impl<A: Actor> PartialOrd for ActorRef<A> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<A: Actor> Ord for ActorRef<A> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<A: Actor> Hash for ActorRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

include!("actor_ref/recipients.rs");
include!("actor_ref/remote_ref.rs");
include!("actor_ref/weak.rs");
include!("actor_ref/links_and_handlers.rs");

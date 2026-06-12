//! Core functionality for defining and managing actors in Thespis.
//!
//! Actors are independent units of computation that run asynchronously, sending and receiving messages.
//! Each actor operates within its own task, and its lifecycle is managed by hooks such as `on_start`, `on_panic`, and `on_stop`.
//!
//! The actor trait is designed to support fault tolerance, recovery from panics, and clean termination.
//! Lifecycle hooks allow customization of actor behavior when starting, encountering errors, or shutting down.
//!
//! This module contains the primary `Actor` trait, which all actors must implement,
//! as well as types for managing message queues (mailboxes) and actor references ([`ActorRef`]).
//!
//! # Features
//! - **Asynchronous Message Handling**: Each actor processes messages asynchronously within its own task.
//! - **Lifecycle Hooks**: Customizable hooks ([`on_start`], [`on_stop`], [`on_panic`]) for managing the actor's lifecycle.
//! - **Backpressure**: Mailboxes can be bounded or unbounded, controlling the flow of messages.
//! - **Supervision**: Actors can be linked, enabling robust supervision and error recovery systems.
//!
//! This module allows building resilient, fault-tolerant, distributed systems with flexible control over the actor lifecycle.
//!
//! [`on_start`]: Actor::on_start
//! [`on_stop`]: Actor::on_stop
//! [`on_panic`]: Actor::on_panic

mod actor_ref;
mod id;
mod kind;
mod spawn;

use std::{any, ops::ControlFlow};

use futures::Future;

use crate::{
    error::{ActorStopReason, PanicError},
    mailbox::{self, MailboxReceiver, MailboxSender, Signal},
    message::BoxMessage,
    reply::{BoxReplySender, ReplyError},
};

pub use actor_ref::*;
pub use id::*;
pub use spawn::*;

const DEFAULT_MAILBOX_CAPACITY: usize = 64;

include!("actor/trait_def.rs");
include!("actor/spawn_trait.rs");

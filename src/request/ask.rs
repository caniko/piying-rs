use futures::{FutureExt, future::BoxFuture};
use std::{
    future::{Future, IntoFuture},
    pin, task,
    time::Duration,
};
use tokio::sync::oneshot;

#[cfg(feature = "remote")]
use crate::{actor, remote};
#[cfg(feature = "remote")]
use remote::codec::Decode as _;

use crate::{
    Actor, Reply,
    actor::{ActorRef, ReplyRecipient},
    error::{self, SendError},
    mailbox::Signal,
    message::Message,
    reply::{ReplyError, ReplySender},
};

use super::{WithRequestTimeout, WithoutRequestTimeout};

include!("ask/local.rs");
include!("ask/pending.rs");
include!("ask/remote_pending.rs");
include!("ask/reply_recipient.rs");
include!("ask/remote.rs");
#[cfg(all(debug_assertions, feature = "tracing"))]
fn warn_deadlock<A: Actor>(
    actor_ref: &ActorRef<A>,
    msg: &'static str,
    called_at: &'static std::panic::Location<'static>,
) {
    use tracing::warn;

    if actor_ref.is_current() {
        warn!("At {called_at}, {msg}");
    }
}

include!("ask/tests.rs");

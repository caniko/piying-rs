//! Defines error handling constructs for thespis.
//!
//! This module centralizes error types used throughout thespis, encapsulating common failure scenarios encountered
//! in actor lifecycle management, message passing, and actor interaction. It simplifies error handling by providing
//! a consistent set of errors that can occur in the operation of actors and their communications.

use std::{
    any::{self},
    cmp, error, fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex,
        atomic::{AtomicPtr, Ordering},
    },
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize, ser::SerializeStruct};
use tokio::{
    sync::{mpsc, oneshot},
    time::error::Elapsed,
};

use crate::{Actor, actor::ActorId, mailbox::Signal, reply::ReplyError};

type ErrorHookFn = fn(&PanicError);

static PANIC_HOOK: AtomicPtr<()> = AtomicPtr::new(default_panic_hook as *mut ());

#[allow(unused_variables)]
fn default_panic_hook(err: &PanicError) {
    #[cfg(feature = "tracing")]
    tracing::error!("actor panicked: {err:?}");
}

/// Sets a custom error hook function that's called when an actor's lifecycle hooks return an error.
///
/// This function allows you to define custom error handling behavior when an actor's
/// `on_start` or `on_stop` method returns an error. The hook will be called immediately
/// when such errors occur, regardless of whether the error is explicitly handled elsewhere.
///
/// By default, the actor system uses a hook that simply logs the error. Setting a custom
/// hook allows for more sophisticated error handling, such as metrics collection,
/// alerting, or custom logging formats.
///
/// # Parameters
///
/// * `hook`: A function that takes a reference to the error information and performs
///   custom error handling.
///
/// # Example
///
/// ```
/// use thespis::error::{set_actor_error_hook, PanicError};
///
/// // Define a custom error hook
/// fn my_custom_hook(err: &PanicError) {
///     // log the error or something...
/// }
///
/// // Install the custom hook
/// set_actor_error_hook(my_custom_hook);
/// ```
///
/// # Notes
///
/// * This hook is global and will affect all actors in the system.
/// * Setting a new hook replaces any previously set hook.
/// * The hook is called even if the error is also being explicitly handled via
///   `wait_for_startup_result` or `wait_for_shutdown_result`.
pub fn set_actor_error_hook(hook: ErrorHookFn) {
    let fn_ptr = hook as *mut ();
    PANIC_HOOK.store(fn_ptr, Ordering::SeqCst);
}

pub(crate) fn invoke_actor_error_hook(err: &PanicError) {
    // Load the function pointer atomically
    let fn_ptr = PANIC_HOOK.load(Ordering::SeqCst);

    // Cast back to function type and call it
    let hook = unsafe { std::mem::transmute::<*mut (), ErrorHookFn>(fn_ptr) };
    hook(err);
}

include!("error/send.rs");
include!("error/panic.rs");
include!("error/infallible.rs");
include!("error/registry.rs");
include!("error/remote.rs");

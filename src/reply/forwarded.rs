/// A delegated reply that has been forwarded to another actor or contains a direct response.
///
/// This type is returned by [`Context::forward`] and its variants, but can also be created
/// directly to respond to the original sender when forwarding is not possible or desired.
/// This allows actors to gracefully handle scenarios where forwarding would otherwise
/// require a panic or cause unexpected termination.
///
/// # Examples
///
/// ## Basic forwarding
///
/// ```
/// use thespis::{prelude::*, reply::ForwardedReply};
/// use std::collections::HashMap;
///
/// #[derive(Actor)]
/// struct RouterActor {
///     routes: HashMap<u32, ActorRef<TargetActor>>,
/// }
///
/// #[derive(Actor, Default)]
/// struct TargetActor;
///
/// struct RouteMessage {
///     target_id: u32,
///     data: String,
/// }
///
/// struct ProcessData(String);
///
/// impl Message<ProcessData> for TargetActor {
///     type Reply = String;
///
///     async fn handle(&mut self, msg: ProcessData, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
///         format!("Processed: {}", msg.0)
///     }
/// }
///
/// impl Message<RouteMessage> for RouterActor {
///     type Reply = ForwardedReply<ProcessData, String>;
///
///     async fn handle(&mut self, msg: RouteMessage, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
///         if let Some(target_ref) = self.routes.get(&msg.target_id) {
///             // Forward to the target actor
///             ctx.forward(target_ref, ProcessData(msg.data)).await
///         } else {
///             // Target not found - respond directly instead of panicking
///             ForwardedReply::from_ok("Default response".to_string())
///         }
///     }
/// }
/// ```
///
/// ## Error handling without forwarding
///
/// ```
/// use thespis::{prelude::*, reply::ForwardedReply};
///
/// #[derive(Debug)]
/// enum RouterError {
///     TargetNotFound,
///     InvalidData,
/// }
///
/// impl std::fmt::Display for RouterError {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         match self {
///             RouterError::TargetNotFound => write!(f, "Target not found"),
///             RouterError::InvalidData => write!(f, "Invalid data"),
///         }
///     }
/// }
///
/// impl std::error::Error for RouterError {}
///
/// #[derive(Actor)]
/// struct SafeRouter;
///
/// struct SafeRouteMessage {
///     target_id: u32,
///     data: String,
/// }
///
/// impl Message<SafeRouteMessage> for SafeRouter {
///     type Reply = ForwardedReply<SafeRouteMessage, Result<String, RouterError>>;
///
///     async fn handle(&mut self, msg: SafeRouteMessage, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
///         if msg.data.is_empty() {
///             // Return error directly without forwarding
///             ForwardedReply::from_err(RouterError::InvalidData)
///         } else if msg.target_id == 999 {
///             // Another error case
///             ForwardedReply::from_err(RouterError::TargetNotFound)
///         } else {
///             // Success case
///             ForwardedReply::from_ok(format!("Routed: {}", msg.data))
///         }
///     }
/// }
/// ```
///
/// [`Context::forward`]: crate::message::Context::forward
pub struct ForwardedReply<M, R>
where
    R: Reply,
{
    inner: ForwardedReplyInner<M, R>,
}

enum ForwardedReplyInner<M, R>
where
    R: Reply,
{
    /// The message was successfully forwarded or failed to be forwarded
    Forwarded(Result<(), SendError<M, R::Error>>),
    /// A direct response without forwarding
    Direct(Result<R::Ok, R::Error>),
}

// Manual Debug implementation to avoid requiring Debug on R::Value
impl<M, R> fmt::Debug for ForwardedReply<M, R>
where
    M: fmt::Debug,
    R: Reply,
    R::Ok: fmt::Debug,
    R::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForwardedReply")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<M, R> fmt::Debug for ForwardedReplyInner<M, R>
where
    M: fmt::Debug,
    R: Reply,
    R::Ok: fmt::Debug,
    R::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forwarded(res) => f.debug_tuple("Forwarded").field(res).finish(),
            Self::Direct(res) => f.debug_tuple("Direct").field(res).finish(),
        }
    }
}

impl<M, R> ForwardedReply<M, R>
where
    R: Reply,
{
    pub(crate) fn new(res: Result<(), SendError<M, R::Error>>) -> Self {
        ForwardedReply {
            inner: ForwardedReplyInner::Forwarded(res),
        }
    }

    /// Creates a ForwardedReply with a direct successful response.
    ///
    /// This allows an actor to respond directly to the original sender when forwarding
    /// is not possible or desired, avoiding the need to panic or cause actor termination.
    ///
    /// # Example
    ///
    /// ```rust
    /// use thespis::reply::ForwardedReply;
    ///
    /// // Create a direct successful response
    /// let reply: ForwardedReply<(), String> = ForwardedReply::from_ok("Success!".to_string());
    /// ```
    pub fn from_ok(value: R::Ok) -> Self {
        ForwardedReply {
            inner: ForwardedReplyInner::Direct(Ok(value)),
        }
    }

    /// Creates a ForwardedReply with a direct error response.
    ///
    /// This allows an actor to respond directly with an error when forwarding
    /// is not possible or desired, maintaining the error semantics of the system.
    ///
    /// # Example
    ///
    /// ```rust
    /// use thespis::reply::ForwardedReply;
    /// use std::{error, fmt};
    ///
    /// #[derive(Debug)]
    /// enum MyError {
    ///     InvalidParameters,
    /// }
    ///
    /// impl fmt::Display for MyError {
    ///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    ///         write!(f, "Invalid parameters")
    ///     }
    /// }
    ///
    /// impl error::Error for MyError {}
    ///
    /// // Create a direct error response
    /// let reply: ForwardedReply<(), Result<String, MyError>> =
    ///     ForwardedReply::from_err(MyError::InvalidParameters);
    /// ```
    pub fn from_err(error: R::Error) -> Self {
        ForwardedReply {
            inner: ForwardedReplyInner::Direct(Err(error)),
        }
    }

    /// Creates a ForwardedReply from a Result.
    ///
    /// This is a convenience method that accepts either an Ok or Err value
    /// and creates the appropriate direct response.
    ///
    /// # Example
    ///
    /// ```rust
    /// use thespis::reply::ForwardedReply;
    /// use std::{error, fmt};
    ///
    /// #[derive(Debug)]
    /// enum ComputeError {
    ///     InvalidInput,
    /// }
    ///
    /// impl fmt::Display for ComputeError {
    ///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    ///         write!(f, "Invalid input")
    ///     }
    /// }
    ///
    /// impl error::Error for ComputeError {}
    ///
    /// // Create from a successful result
    /// let success_result = Ok(42);
    /// let reply_ok: ForwardedReply<(), Result<i32, ComputeError>> =
    ///     ForwardedReply::from_result(success_result);
    ///
    /// // Create from an error result
    /// let error_result = Err(ComputeError::InvalidInput);
    /// let reply_err: ForwardedReply<(), Result<i32, ComputeError>> =
    ///     ForwardedReply::from_result(error_result);
    /// ```
    pub fn from_result(result: Result<R::Ok, R::Error>) -> Self {
        ForwardedReply {
            inner: ForwardedReplyInner::Direct(result),
        }
    }
}

impl<M, R> Reply for ForwardedReply<M, R>
where
    R: Reply,
    M: Send + 'static,
{
    type Ok = R::Ok;
    type Error = SendError<M, R::Error>;
    type Value = Result<Self::Ok, Self::Error>;

    fn to_result(self) -> Result<Self::Ok, Self::Error> {
        match self.inner {
            ForwardedReplyInner::Forwarded(res) => res.map(|_| {
                unreachable!("forwarded reply is only converted to a result if its an error")
            }),
            ForwardedReplyInner::Direct(result) => {
                result.map_err(SendError::<M, R::Error>::HandlerError)
            }
        }
    }

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        match self.inner {
            ForwardedReplyInner::Forwarded(res) => {
                res.err().map(|err| Box::new(err) as Box<dyn ReplyError>)
            }
            ForwardedReplyInner::Direct(result) => result.err().map(|err| {
                Box::new(SendError::<M, R::Error>::HandlerError(err)) as Box<dyn ReplyError>
            }),
        }
    }

    fn into_value(self) -> Self::Value {
        match self.inner {
            ForwardedReplyInner::Forwarded(res) => res.map(|_| {
                unreachable!("forwarded reply is only an error if it failed to forward the message")
            }),
            ForwardedReplyInner::Direct(result) => {
                result.map_err(SendError::<M, R::Error>::HandlerError)
            }
        }
    }

    /// If the forwarded reply succeeded, the we can safely assume
    /// the `Box<dyn Any>` we have here is the ok value of the inner `R`.
    fn downcast_ok(ok: Box<dyn any::Any>) -> Self::Ok {
        *ok.downcast().unwrap()
    }

    /// The error is either from the inner `R`, or our outer `SendError`.
    /// We'll try both.
    fn downcast_err<N: 'static>(err: BoxSendError) -> SendError<N, Self::Error> {
        err.try_downcast::<N, R::Error>()
            .map(|err| err.map_err(SendError::HandlerError))
            .unwrap_or_else(|err| {
                err.downcast::<M, SendError<M, R::Error>>().map_msg(|_| {
                    unreachable!(
                        "forwarded reply is only an error if it failed to forward the message"
                    )
                })
            })
    }
}


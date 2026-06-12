/// A dyn boxed send error.
pub type BoxSendError = SendError<Box<dyn any::Any + Send>, Box<dyn any::Any + Send>>;

/// Error that can occur when sending a message to an actor.
#[derive(Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SendError<M = (), E = Infallible> {
    /// The actor isn't running.
    ActorNotRunning(M),
    /// The actor panicked or was stopped before a reply could be received.
    ActorStopped,
    /// The actors mailbox is full.
    MailboxFull(M),
    /// An error returned by the actor's message handler.
    HandlerError(E),
    /// Timed out waiting for a reply.
    Timeout(Option<M>),
}

impl<M, E> SendError<M, E> {
    /// Maps the inner message to another type if the variant is [`ActorNotRunning`](SendError::ActorNotRunning).
    pub fn map_msg<N, F>(self, mut f: F) -> SendError<N, E>
    where
        F: FnMut(M) -> N,
    {
        match self {
            SendError::ActorNotRunning(msg) => SendError::ActorNotRunning(f(msg)),
            SendError::ActorStopped => SendError::ActorStopped,
            SendError::MailboxFull(msg) => SendError::MailboxFull(f(msg)),
            SendError::HandlerError(err) => SendError::HandlerError(err),
            SendError::Timeout(msg) => SendError::Timeout(msg.map(f)),
        }
    }

    /// Maps the inner error to another type if the variant is [`HandlerError`](SendError::HandlerError).
    pub fn map_err<F, O>(self, mut op: O) -> SendError<M, F>
    where
        O: FnMut(E) -> F,
    {
        match self {
            SendError::ActorNotRunning(msg) => SendError::ActorNotRunning(msg),
            SendError::ActorStopped => SendError::ActorStopped,
            SendError::MailboxFull(msg) => SendError::MailboxFull(msg),
            SendError::HandlerError(err) => SendError::HandlerError(op(err)),
            SendError::Timeout(msg) => SendError::Timeout(msg),
        }
    }

    /// Converts the inner error types to `Box<dyn Any + Send>`.
    pub fn boxed(self) -> BoxSendError
    where
        M: Send + 'static,
        E: Send + 'static,
    {
        match self {
            SendError::ActorNotRunning(err) => SendError::ActorNotRunning(Box::new(err)),
            SendError::ActorStopped => SendError::ActorStopped,
            SendError::MailboxFull(msg) => SendError::MailboxFull(Box::new(msg)),
            SendError::HandlerError(err) => SendError::HandlerError(Box::new(err)),
            SendError::Timeout(msg) => {
                SendError::Timeout(msg.map(|msg| Box::new(msg) as Box<dyn any::Any + Send>))
            }
        }
    }

    /// Returns the inner message if available.
    pub fn msg(self) -> Option<M> {
        match self {
            SendError::ActorNotRunning(msg) => Some(msg),
            SendError::MailboxFull(msg) => Some(msg),
            SendError::Timeout(msg) => msg,
            _ => None,
        }
    }

    /// Returns the inner error if available.
    pub fn err(self) -> Option<E> {
        match self {
            SendError::HandlerError(err) => Some(err),
            _ => None,
        }
    }

    /// Unwraps the inner message, consuming the `self` value.
    ///
    /// # Panics
    ///
    /// Panics if the error does not contain the inner message.
    pub fn unwrap_msg(self) -> M {
        match self.msg() {
            Some(msg) => msg,
            None => panic!("called `SendError::unwrap_msg()` on a non message error"),
        }
    }

    /// Unwraps the inner handler error, consuming the `self` value.
    ///
    /// # Panics
    ///
    /// Panics if the error does not contain a handler error.
    pub fn unwrap_err(self) -> E {
        match self.err() {
            Some(err) => err,
            None => panic!("called `SendError::unwrap_err()` on a non error"),
        }
    }

    pub(crate) fn reset_err_infallible<F>(self) -> SendError<M, F> {
        self.map_err(|_| panic!("reset err infallible called on a `SendError::HandlerError`"))
    }
}

impl<M, E> SendError<M, SendError<M, E>> {
    /// Flattens a nested SendError.
    pub fn flatten(self) -> SendError<M, E> {
        match self {
            SendError::ActorNotRunning(msg)
            | SendError::HandlerError(SendError::ActorNotRunning(msg)) => {
                SendError::ActorNotRunning(msg)
            }
            SendError::ActorStopped | SendError::HandlerError(SendError::ActorStopped) => {
                SendError::ActorStopped
            }
            SendError::MailboxFull(msg) | SendError::HandlerError(SendError::MailboxFull(msg)) => {
                SendError::MailboxFull(msg)
            }
            SendError::HandlerError(SendError::HandlerError(err)) => SendError::HandlerError(err),
            SendError::Timeout(msg) | SendError::HandlerError(SendError::Timeout(msg)) => {
                SendError::Timeout(msg)
            }
        }
    }
}

impl BoxSendError {
    /// Downcasts the inner error types to a concrete type.
    #[must_use]
    pub fn downcast<M, E>(self) -> SendError<M, E>
    where
        M: 'static,
        E: 'static,
    {
        self.try_downcast().unwrap()
    }

    /// Downcasts the inner error types to a concrete type, returning an error if its the wrong type.
    pub fn try_downcast<M, E>(self) -> Result<SendError<M, E>, Self>
    where
        M: 'static,
        E: 'static,
    {
        match self {
            SendError::ActorNotRunning(err) => Ok(SendError::ActorNotRunning(
                *err.downcast::<M>().map_err(SendError::ActorNotRunning)?,
            )),
            SendError::ActorStopped => Ok(SendError::ActorStopped),
            SendError::MailboxFull(err) => Ok(SendError::MailboxFull(
                *err.downcast().map_err(SendError::MailboxFull)?,
            )),
            SendError::HandlerError(err) => Ok(SendError::HandlerError(
                *err.downcast().map_err(SendError::HandlerError)?,
            )),
            SendError::Timeout(err) => Ok(SendError::Timeout(
                err.map(|err| {
                    err.downcast()
                        .map(|v| *v)
                        .map_err(|err| SendError::Timeout(Some(err)))
                })
                .transpose()?,
            )),
        }
    }
}

impl<M, E> fmt::Debug for SendError<M, E>
where
    E: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::ActorNotRunning(_) => write!(f, "ActorNotRunning"),
            SendError::ActorStopped => write!(f, "ActorStopped"),
            SendError::MailboxFull(_) => write!(f, "MailboxFull"),
            SendError::HandlerError(err) => err.fmt(f),
            SendError::Timeout(_) => write!(f, "Timeout"),
        }
    }
}

impl<M, E> fmt::Display for SendError<M, E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::ActorNotRunning(_) => write!(f, "actor not running"),
            SendError::ActorStopped => write!(f, "actor stopped"),
            SendError::MailboxFull(_) => write!(f, "mailbox full"),
            SendError::HandlerError(err) => err.fmt(f),
            SendError::Timeout(_) => write!(f, "timeout"),
        }
    }
}

impl<A, M, E> From<mpsc::error::SendError<Signal<A>>> for SendError<M, E>
where
    A: Actor,
    M: 'static,
{
    fn from(err: mpsc::error::SendError<Signal<A>>) -> Self {
        SendError::ActorNotRunning(err.0.downcast_message::<M>().unwrap())
    }
}

impl<A, M, E> From<mpsc::error::TrySendError<Signal<A>>> for SendError<M, E>
where
    A: Actor,
    M: 'static,
{
    fn from(err: mpsc::error::TrySendError<Signal<A>>) -> Self {
        match err {
            mpsc::error::TrySendError::Full(signal) => {
                SendError::MailboxFull(signal.downcast_message::<M>().unwrap())
            }
            mpsc::error::TrySendError::Closed(signal) => {
                SendError::ActorNotRunning(signal.downcast_message::<M>().unwrap())
            }
        }
    }
}

impl<M, E> From<oneshot::error::RecvError> for SendError<M, E> {
    fn from(_err: oneshot::error::RecvError) -> Self {
        SendError::ActorStopped
    }
}

impl<A, M, E> From<mpsc::error::SendTimeoutError<Signal<A>>> for SendError<M, E>
where
    A: Actor,
    M: 'static,
{
    fn from(err: mpsc::error::SendTimeoutError<Signal<A>>) -> Self {
        match err {
            mpsc::error::SendTimeoutError::Timeout(msg) => {
                SendError::Timeout(Some(msg.downcast_message::<M>().unwrap()))
            }
            mpsc::error::SendTimeoutError::Closed(msg) => {
                SendError::ActorNotRunning(msg.downcast_message::<M>().unwrap())
            }
        }
    }
}

impl<M, E> From<Elapsed> for SendError<M, E> {
    fn from(_: Elapsed) -> Self {
        SendError::Timeout(None)
    }
}

impl<M, E> error::Error for SendError<M, E> where E: fmt::Debug + fmt::Display {}

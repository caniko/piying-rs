/// Reason for an actor being stopped.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ActorStopReason {
    /// Actor stopped normally.
    Normal,
    /// Actor was killed.
    Killed,
    /// Actor panicked.
    Panicked(PanicError),
    /// Link died.
    LinkDied {
        /// Actor ID.
        id: ActorId,
        /// Actor died reason.
        reason: Box<ActorStopReason>,
    },
    /// The peer was disconnected.
    #[cfg(feature = "remote")]
    PeerDisconnected,
}

impl fmt::Debug for ActorStopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorStopReason::Normal => write!(f, "Normal"),
            ActorStopReason::Killed => write!(f, "Killed"),
            ActorStopReason::Panicked(err) => {
                let mut dbg_struct = f.debug_struct("Panicked");
                err.with_debug_inner(|err| {
                    dbg_struct.field("err", err);
                });
                dbg_struct.finish()
            }
            ActorStopReason::LinkDied { id, reason } => f
                .debug_struct("LinkDied")
                .field("id", id)
                .field("reason", &reason)
                .finish(),
            #[cfg(feature = "remote")]
            ActorStopReason::PeerDisconnected => write!(f, "PeerDisconnected"),
        }
    }
}

impl fmt::Display for ActorStopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorStopReason::Normal => write!(f, "actor stopped normally"),
            ActorStopReason::Killed => write!(f, "actor was killed"),
            ActorStopReason::Panicked(err) => err.fmt(f),
            ActorStopReason::LinkDied { id, reason: _ } => {
                write!(f, "link {id} died")
            }
            #[cfg(feature = "remote")]
            ActorStopReason::PeerDisconnected => write!(f, "peer disconnected"),
        }
    }
}

/// An error type returned from actor startup/shutdown results.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum HookError<E> {
    /// The hook panic error.
    Panicked(PanicError),
    /// The returned hook error.
    Error(E),
}

impl<E> fmt::Display for HookError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HookError::Panicked(err) => err.fmt(f),
            HookError::Error(err) => err.fmt(f),
        }
    }
}

impl<E> error::Error for HookError<E> where E: error::Error {}

/// A shared error that occurs when an actor panics or returns an error from a hook in the [Actor] trait.
#[derive(Clone)]
pub struct PanicError {
    kind: PanicErrorKind,
    reason: PanicReason,
}

#[derive(Clone)]
enum PanicErrorKind {
    /// Local actor error — preserves concrete type for downcasting.
    Dynamic(Arc<Mutex<Box<dyn ReplyError>>>),
    /// Deserialized from network — just the Display string, no Box.
    Message(String),
}

impl PanicError {
    /// Creates a new PanicError from a generic boxed reply error.
    pub fn new(err: Box<dyn ReplyError>, reason: PanicReason) -> Self {
        PanicError {
            kind: PanicErrorKind::Dynamic(Arc::new(Mutex::new(err))),
            reason,
        }
    }

    /// Creates a [`PanicError`] from a pre-formatted error string.
    ///
    /// Useful when deserializing a remote error where the original concrete
    /// type is unavailable — only the `Display` output was transmitted.
    pub fn from_wire(err: String, reason: PanicReason) -> Self {
        PanicError {
            kind: PanicErrorKind::Message(err),
            reason,
        }
    }

    pub(crate) fn new_from_panic_any(err: Box<dyn any::Any + Send>, reason: PanicReason) -> Self {
        err.downcast::<&'static str>()
            .map(|s| PanicError::new(Box::new(*s), reason))
            .or_else(|err| {
                err.downcast::<String>()
                    .map(|s| PanicError::new(Box::new(*s), reason))
            })
            .unwrap_or_else(|err| PanicError::new(Box::new(err), reason))
    }

    /// Returns the reason for the panic.
    pub fn reason(&self) -> PanicReason {
        self.reason
    }

    /// Returns the error message as a string, if available.
    pub fn with_str<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&str) -> R,
    {
        match &self.kind {
            PanicErrorKind::Message(s) => Some(f(s)),
            PanicErrorKind::Dynamic(err) => {
                let lock = match err.lock() {
                    Ok(lock) => lock,
                    Err(poisoned) => poisoned.into_inner(),
                };
                lock.downcast_ref::<&str>()
                    .copied()
                    .or_else(|| lock.downcast_ref::<String>().map(String::as_str))
                    .map(f)
            }
        }
    }

    /// Downcasts and clones the inner error, returning `Some` if the panic error matches the type `T`.
    pub fn downcast<T>(&self) -> Option<T>
    where
        T: ReplyError + Clone,
    {
        self.with_downcast_ref(|err: &T| err.clone())
    }

    /// Calls the passed closure `f` with the inner type downcast into `T`, otherwise returns `None`.
    pub fn with_downcast_ref<T, F, R>(&self, f: F) -> Option<R>
    where
        T: ReplyError,
        F: FnOnce(&T) -> R,
    {
        match &self.kind {
            PanicErrorKind::Message(_) => None,
            PanicErrorKind::Dynamic(err) => match err.lock() {
                Ok(lock) => lock.downcast_ref().map(f),
                Err(err) => err.get_ref().downcast_ref().map(f),
            },
        }
    }

    /// Returns a reference to the error as a `&Box<dyn ReplyError>`.
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Box<dyn ReplyError>) -> R,
    {
        match &self.kind {
            PanicErrorKind::Dynamic(err) => match err.lock() {
                Ok(lock) => f(&lock),
                Err(err) => f(err.get_ref()),
            },
            PanicErrorKind::Message(s) => {
                let boxed: Box<dyn ReplyError> = Box::new(s.clone());
                f(&boxed)
            }
        }
    }

    fn with_debug_inner<F>(&self, mut f: F)
    where
        F: FnMut(&dyn fmt::Debug),
    {
        match &self.kind {
            PanicErrorKind::Message(s) => f(&s),
            PanicErrorKind::Dynamic(_) => {
                self.with_str(|s| f(&s))
                    .or_else(|| self.with_downcast_ref::<Box<dyn ReplyError>, _, _>(|err| f(err)))
                    .unwrap_or_else(|| self.with(|any| f(any)));
            }
        }
    }
}

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.with_str(|s| write!(f, "{}: {s}", self.reason))
            .unwrap_or_else(|| write!(f, "{}", self.reason))
    }
}

impl fmt::Debug for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg_struct = f.debug_struct("PanicError");

        self.with_debug_inner(|err| {
            dbg_struct.field("err", err);
            dbg_struct.field("reason", &self.reason);
        });

        dbg_struct.finish()
    }
}

impl error::Error for PanicError {}

#[cfg(feature = "serde")]
impl Serialize for PanicError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut ser = serializer.serialize_struct("PanicError", 2)?;
        ser.serialize_field("err", &self.to_string())?;
        ser.serialize_field("reason", &self.reason)?;
        ser.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for PanicError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        enum Field {
            Err,
            Reason,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> serde::de::Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        formatter.write_str("`err` or `reason`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "err" => Ok(Field::Err),
                            "reason" => Ok(Field::Reason),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct PanicErrorVisitor;

        impl<'de> serde::de::Visitor<'de> for PanicErrorVisitor {
            type Value = PanicError;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct PanicError")
            }

            fn visit_map<V>(self, mut map: V) -> Result<PanicError, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut err: Option<String> = None;
                let mut reason = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Err => {
                            if err.is_some() {
                                return Err(serde::de::Error::duplicate_field("err"));
                            }
                            err = Some(map.next_value()?);
                        }
                        Field::Reason => {
                            if reason.is_some() {
                                return Err(serde::de::Error::duplicate_field("reason"));
                            }
                            reason = Some(map.next_value()?);
                        }
                    }
                }

                let err = err.ok_or_else(|| serde::de::Error::missing_field("err"))?;
                let reason = reason.ok_or_else(|| serde::de::Error::missing_field("reason"))?;

                Ok(PanicError::from_wire(err, reason))
            }
        }

        const FIELDS: &[&str] = &["err", "reason"];
        deserializer.deserialize_struct("PanicError", FIELDS, PanicErrorVisitor)
    }
}

/// Describes the cause of an actor panic or fatal error.
///
/// In thespis, several error conditions are treated as panics, triggering the
/// [`on_panic`](crate::actor::Actor::on_panic) lifecycle hook and potentially
/// stopping the actor.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub enum PanicReason {
    /// A message handler panicked during execution.
    ///
    /// This occurs when an actor's [`Message::handle`](crate::message::Message::handle)
    /// implementation panics or unwinds.
    HandlerPanic,
    /// The [`on_message`] hook returned an error.
    ///
    /// In the default implementation, this occurs when a message handler returns
    /// an error during a [`tell`](crate::actor::ActorRef::tell) operation, where
    /// there's no mechanism to return the error to the caller. However, if
    /// [`on_message`] is overridden with
    /// custom logic, this variant indicates that the custom implementation
    /// returned an error.
    ///
    /// [`on_message`]: Actor::on_message
    OnMessage,
    /// The [`on_start`](Actor::on_start) lifecycle hook returned an error.
    OnStart,
    /// The [`on_panic`](Actor::on_panic) lifecycle hook returned an error.
    OnPanic,
    /// The [`on_link_died`](Actor::on_link_died) lifecycle hook returned an error.
    OnLinkDied,
    /// The [`on_stop`](Actor::on_stop) lifecycle hook returned an error.
    OnStop,
    /// The [`next`](Actor::next) lifecycle hook returned an error.
    Next,
}

impl PanicReason {
    /// Returns `true` if the panic occurred in a lifecycle hook.
    ///
    /// Lifecycle hooks include `on_start`, `on_panic`, `on_link_died`, and `on_stop`.
    /// This can be useful for distinguishing between initialization/cleanup errors
    /// and runtime message handling errors.
    ///
    /// # Example
    ///
    /// ```rust
    /// use thespis::error::PanicReason;
    ///
    /// assert!(PanicReason::OnStart.is_lifecycle_hook());
    /// assert!(!PanicReason::HandlerPanic.is_lifecycle_hook());
    /// ```
    pub fn is_lifecycle_hook(&self) -> bool {
        matches!(
            self,
            PanicReason::OnStart
                | PanicReason::OnPanic
                | PanicReason::OnLinkDied
                | PanicReason::OnStop
        )
    }

    /// Returns `true` if the panic occurred while processing a message.
    ///
    /// This includes both panics during message handler execution and errors
    /// returned by `on_message`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use thespis::error::PanicReason;
    ///
    /// assert!(PanicReason::HandlerPanic.is_message_processing());
    /// assert!(PanicReason::OnMessage.is_message_processing());
    /// assert!(!PanicReason::OnStart.is_message_processing());
    /// ```
    pub fn is_message_processing(&self) -> bool {
        matches!(self, PanicReason::HandlerPanic | PanicReason::OnMessage)
    }
}

impl fmt::Display for PanicReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PanicReason::HandlerPanic => write!(f, "message handler panicked"),
            PanicReason::OnMessage => write!(f, "on_message returned error"),
            PanicReason::OnStart => write!(f, "on_start returned error"),
            PanicReason::OnPanic => write!(f, "on_panic returned error"),
            PanicReason::OnLinkDied => write!(f, "on_link_died returned error"),
            PanicReason::OnStop => write!(f, "on_stop returned error"),
            PanicReason::Next => write!(f, "next returned error"),
        }
    }
}


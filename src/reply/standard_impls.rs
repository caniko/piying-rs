impl<T, E> Reply for Result<T, E>
where
    T: Send + 'static,
    E: ReplyError,
{
    type Ok = T;
    type Error = E;
    type Value = Self;

    fn to_result(self) -> Result<T, E> {
        self
    }

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        self.map_err(|err| Box::new(err) as Box<dyn ReplyError>)
            .err()
    }

    #[inline]
    fn into_value(self) -> Self::Value {
        self
    }
}

macro_rules! impl_infallible_reply {
    ([
        $(
            $( {
                $( $generics:tt )*
             } )?
            $ty:ty
        ),* $(,)?
    ]) => {
        $(
            impl_infallible_reply!(
                $( {
                    $( $generics )*
                 } )?
                $ty
            );
        )*
    };
    (
        $( {
            $( $generics:tt )*
         } )?
        $ty:ty
    ) => {
        impl $( < $($generics)* > )? Reply for $ty {
            type Ok = Self;
            type Error = $crate::error::Infallible;
            type Value = Self;

            fn to_result(self) -> Result<Self, $crate::error::Infallible> {
                Ok(self)
            }

            fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
                None
            }

            #[inline]
            fn into_value(self) -> Self::Value {
                self
            }
        }
    };
}

impl_infallible_reply!([
    // Thespis types
    ActorId,
    {A: Actor} ActorRef<A>,
    {A: Actor} PreparedActor<A>,
    {M: Send} Recipient<M>,
    {M: Send, Ok: Send, Err: ReplyError} ReplyRecipient<M, Ok, Err>,
    {A: Actor} WeakActorRef<A>,
    {M: Send} WeakRecipient<M>,
    {M: Send, Ok: Send, Err: ReplyError} WeakReplyRecipient<M, Ok, Err>,
    ActorStopReason,
    PanicError,
    SendError,
    {A: Actor} MailboxReceiver<A>,
    {A: Actor} MailboxSender<A>,
    {A: Actor, R: Reply} Context<A, R>,
    {R: Reply} ReplySender<R>,
    Infallible,
    // Std lib types
    (),
    usize,
    u8,
    u16,
    u32,
    u64,
    u128,
    isize,
    i8,
    i16,
    i32,
    i64,
    i128,
    f32,
    f64,
    char,
    bool,
    &'static str,
    String,
    &'static Path,
    PathBuf,
    {T: 'static + Send} Option<T>,
    {T: Clone + Send + Sync} Cow<'static, T>,
    {T: 'static + Send + Sync} Arc<T>,
    {T: 'static + Send} Mutex<T>,
    {T: 'static + Send} RwLock<T>,
    {const N: usize, T: 'static + Send + Sync} &'static [T; N],
    {const N: usize, T: 'static + Send} [T; N],
    {T: 'static + Send + Sync} &'static [T],
    {T: 'static + Send} &'static mut T,
    {T: 'static + Send} Box<T>,
    {T: 'static + Send} Vec<T>,
    {T: 'static + Send} VecDeque<T>,
    {T: 'static + Send} LinkedList<T>,
    {K: 'static + Send, V: 'static + Send} HashMap<K, V>,
    {K: 'static + Send, V: 'static + Send} BTreeMap<K, V>,
    {T: 'static + Send} HashSet<T>,
    {T: 'static + Send} BTreeSet<T>,
    {T: 'static + Send} BinaryHeap<T>,
    NonZeroI8,
    NonZeroI16,
    NonZeroI32,
    NonZeroI64,
    NonZeroI128,
    NonZeroIsize,
    NonZeroU8,
    NonZeroU16,
    NonZeroU32,
    NonZeroU64,
    NonZeroU128,
    NonZeroUsize,
    AtomicBool,
    AtomicIsize,
    {T: 'static + Send} AtomicPtr<T>,
    AtomicUsize,
    Once,
    Thread,
    {T: 'static + Send} std::cell::OnceCell<T>,
    {T: 'static + Send} std::sync::mpsc::Sender<T>,
    {T: 'static + Send} std::sync::mpsc::Receiver<T>,
    // Tokio types
    {T: 'static + Send + Future<Output = O>, O: Send} futures::stream::FuturesOrdered<T>,
    {T: 'static + Send} futures::stream::FuturesUnordered<T>,
    {T: 'static + Send} tokio::sync::OnceCell<T>,
    tokio::sync::Semaphore,
    tokio::sync::Notify,
    {T: 'static + Send} tokio::sync::mpsc::Sender<T>,
    {T: 'static + Send} tokio::sync::mpsc::Receiver<T>,
    {T: 'static + Send} tokio::sync::mpsc::UnboundedSender<T>,
    {T: 'static + Send} tokio::sync::mpsc::UnboundedReceiver<T>,
    {T: 'static + Send + Sync} tokio::sync::watch::Sender<T>,
    {T: 'static + Send + Sync} tokio::sync::watch::Receiver<T>,
    {T: 'static + Send} tokio::sync::broadcast::Sender<T>,
    {T: 'static + Send} tokio::sync::broadcast::Receiver<T>,
    {T: 'static + Send} tokio::sync::oneshot::Sender<T>,
    {T: 'static + Send} tokio::sync::oneshot::Receiver<T>,
    {T: 'static + Send} tokio::sync::Mutex<T>,
    {T: 'static + Send} tokio::sync::RwLock<T>,
    tokio::task::AbortHandle,
    // Tuple types
    {A: 'static + Send} (A,),
    {A: 'static + Send, B: 'static + Send} (A, B),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send} (A, B, C),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send} (A, B, C, D),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send} (A, B, C, D, E),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send} (A, B, C, D, E, F),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send} (A, B, C, D, E, F, G),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send} (A, B, C, D, E, F, G, H),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send} (A, B, C, D, E, F, G, H, I),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send} (A, B, C, D, E, F, G, H, I, J),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send, W: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send, W: 'static + Send, X: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send, W: 'static + Send, X: 'static + Send, Y: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send, W: 'static + Send, X: 'static + Send, Y: 'static + Send, Z: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z),
]);

#[cfg(feature = "remote")]
impl_infallible_reply!([
    {A: Actor + crate::remote::RemoteActor} crate::actor::RemoteActorRef<A>,
    {E: 'static + Send} crate::error::RemoteSendError<E>,
    crate::remote::ActorSwarm,
]);

#[cfg(target_has_atomic = "8")]
impl_infallible_reply!([AtomicI8, AtomicU8]);
#[cfg(target_has_atomic = "16")]
impl_infallible_reply!([AtomicI16, AtomicU16]);
#[cfg(target_has_atomic = "32")]
impl_infallible_reply!([AtomicI32, AtomicU32]);
#[cfg(target_has_atomic = "64")]
impl_infallible_reply!([AtomicI64, AtomicU64]);


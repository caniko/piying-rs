/// A stream of remote actor references discovered during distributed lookup.
///
/// This stream yields [`RemoteActorRef<A>`] instances as they are discovered across
/// the network. The stream completes when all known actors matching the lookup
/// name have been found.
///
/// # Errors
///
/// Individual stream items may be errors if specific actors cannot be reached
/// or validated during lookup.
///
/// # Example
///
/// ```rust,no_run
/// # use thespis::{Actor, RemoteActor, actor::RemoteActorRef};
/// # use futures::TryStreamExt;
/// #
/// # #[derive(Actor, RemoteActor)]
/// # struct MyActor;
/// #
/// # tokio_test::block_on(async {
/// let mut stream = RemoteActorRef::<MyActor>::lookup_all("my-service");
/// while let Some(actor_ref) = stream.try_next().await? {
///     // Handle each discovered actor
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # });
/// ```
///
/// [`RemoteActorRef<A>`]: crate::actor::RemoteActorRef
#[derive(Debug)]
pub struct LookupStream<A> {
    inner: LookupStreamInner,
    _phantom: PhantomData<fn() -> A>,
}

impl<A> LookupStream<A> {
    fn new(swarm_tx: SwarmSender, reply_rx: mpsc::UnboundedReceiver<LookupResult>) -> Self {
        Self {
            inner: LookupStreamInner::Stream { swarm_tx, reply_rx },
            _phantom: PhantomData,
        }
    }

    pub(crate) fn new_err() -> Self {
        Self {
            inner: LookupStreamInner::SwarmNotBootstrapped { done: false },
            _phantom: PhantomData,
        }
    }
}

#[derive(Debug)]
enum LookupStreamInner {
    SwarmNotBootstrapped {
        done: bool,
    },
    Stream {
        swarm_tx: SwarmSender,
        reply_rx: mpsc::UnboundedReceiver<Result<ActorRegistration<'static>, RegistryError>>,
    },
}

impl<A: Actor + RemoteActor> Stream for LookupStream<A> {
    type Item = Result<RemoteActorRef<A>, RegistryError>;

    fn poll_next(
        self: pin::Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match &mut this.inner {
            LookupStreamInner::SwarmNotBootstrapped { done } => {
                if *done {
                    Poll::Ready(None)
                } else {
                    *done = true;
                    Poll::Ready(Some(Err(RegistryError::SwarmNotBootstrapped)))
                }
            }
            LookupStreamInner::Stream { swarm_tx, reply_rx } => {
                match ready!(reply_rx.poll_recv(cx)) {
                    Some(Ok(registration)) => {
                        if A::REMOTE_ID != registration.remote_id {
                            Poll::Ready(Some(Err(RegistryError::BadActorType)))
                        } else {
                            Poll::Ready(Some(Ok(RemoteActorRef::new(
                                registration.actor_id,
                                swarm_tx.clone(),
                            ))))
                        }
                    }
                    Some(Err(err)) => Poll::Ready(Some(Err(err))),
                    None => Poll::Ready(None),
                }
            }
        }
    }
}


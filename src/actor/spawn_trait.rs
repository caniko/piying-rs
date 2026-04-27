/// Provides methods for spawning actors with various configurations.
///
/// This trait is automatically implemented for all types that implement [`Actor`], providing
/// convenient methods to spawn actors in different execution contexts and with different
/// mailbox configurations.
///
/// The `Spawn` trait separates actor instantiation from actor behavior, keeping the [`Actor`]
/// trait focused on lifecycle hooks and message handling while providing ergonomic spawn
/// methods through this extension trait.
///
/// # Choosing a Spawn Method
///
/// - **[`spawn`]** or **[`spawn_default`]**: Standard async actor with bounded mailbox (most common)
/// - **[`spawn_with_mailbox`]**: Custom mailbox configuration (unbounded, custom capacity)
/// - **[`spawn_in_thread`]**: Blocking operations requiring dedicated thread
/// - **[`spawn_link`]**: Actor needs supervision link established before spawning
///
/// # Examples
///
/// ## Basic Spawning
///
/// ```
/// use thespis::Actor;
/// use thespis::actor::Spawn;
///
/// #[derive(Actor)]
/// struct Counter {
///     count: i32,
/// }
///
/// # tokio_test::block_on(async {
/// // Spawn with explicit initialization
/// let actor_ref = Counter::spawn(Counter { count: 0 });
/// # })
/// ```
///
/// ## Default Spawning
///
/// ```
/// use thespis::Actor;
/// use thespis::actor::Spawn;
///
/// #[derive(Actor, Default)]
/// struct Counter {
///     count: i32,
/// }
///
/// # tokio_test::block_on(async {
/// // Spawn with default initialization
/// let actor_ref = Counter::spawn_default();
/// # })
/// ```
///
/// ## Custom Mailbox
///
/// ```
/// use thespis::Actor;
/// use thespis::actor::Spawn;
/// use thespis::mailbox;
///
/// #[derive(Actor)]
/// struct HighThroughput;
///
/// # tokio_test::block_on(async {
/// // Spawn with unbounded mailbox for high message rates
/// let actor_ref = HighThroughput::spawn_with_mailbox(
///     HighThroughput,
///     mailbox::unbounded()
/// );
/// # })
/// ```
///
/// ## Blocking Operations
///
/// ```no_run
/// use std::fs::File;
/// use thespis::Actor;
/// use thespis::actor::Spawn;
///
/// #[derive(Actor)]
/// struct FileWriter {
///     file: File,
/// }
///
/// // Spawn in dedicated thread for blocking I/O
/// let actor_ref = FileWriter::spawn_in_thread(
///     FileWriter { file: File::create("log.txt").unwrap() }
/// );
/// ```
///
/// ## Supervision
///
/// ```
/// use thespis::Actor;
/// use thespis::actor::Spawn;
///
/// #[derive(Actor)]
/// struct Supervisor;
///
/// #[derive(Actor)]
/// struct Worker;
///
/// # tokio_test::block_on(async {
/// let supervisor = Supervisor::spawn(Supervisor);
/// // Link worker to supervisor before spawning
/// let worker = Worker::spawn_link(&supervisor, Worker).await;
/// # })
/// ```
///
/// # Note
///
/// This trait is sealed and cannot be implemented manually. It is automatically available
/// for all [`Actor`] types through a blanket implementation.
///
/// [`spawn`]: Spawn::spawn
/// [`spawn_default`]: Spawn::spawn_default
/// [`spawn_with_mailbox`]: Spawn::spawn_with_mailbox
/// [`spawn_in_thread`]: Spawn::spawn_in_thread
/// [`spawn_link`]: Spawn::spawn_link
pub trait Spawn: Actor + private::Sealed {
    /// Spawns the actor in a Tokio task, running asynchronously with a default bounded mailbox.
    ///
    /// This function spawns the actor in a non-blocking Tokio task, making it suitable for actors that need to
    /// perform asynchronous operations. The actor runs in the background and can be interacted with through
    /// the returned [`ActorRef`].
    ///
    /// By default, a bounded mailbox with capacity 64 is used to provide backpressure.
    /// For custom mailbox configuration, use [`Spawn::spawn_with_mailbox`].
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    ///
    /// #[derive(Actor)]
    /// struct MyActor;
    ///
    /// # tokio_test::block_on(async {
    /// // Spawns with a default bounded mailbox (capacity 64)
    /// let actor_ref = MyActor::spawn(MyActor);
    /// # })
    /// ```
    ///
    /// The actor will continue running in the background, and messages can be sent to it via `actor_ref`.
    fn spawn(args: Self::Args) -> ActorRef<Self> {
        Spawn::spawn_with_mailbox(args, mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
    }

    /// Spawns the actor with default initialization in a Tokio task.
    ///
    /// This is a convenience method for actors that implement [`Default`], equivalent to calling
    /// `Self::spawn(Self::default())`. The actor runs asynchronously in a non-blocking Tokio task
    /// and can be interacted with through the returned [`ActorRef`].
    ///
    /// By default, a bounded mailbox with capacity 64 is used to provide backpressure.
    /// For custom initialization or mailbox configuration, use [`Spawn::spawn`] or
    /// [`Spawn::spawn_with_mailbox`] instead.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    ///
    /// #[derive(Actor, Default)]
    /// struct MyActor {
    ///     count: i32,
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// // Spawns with default state and bounded mailbox (capacity 64)
    /// let actor_ref = MyActor::spawn_default();
    /// # })
    /// ```
    ///
    /// # Requirements
    ///
    /// This method requires that `Self::Args` implements [`Default`]. For actors where
    /// `Args = Self`, this means the actor struct itself must implement `Default`.
    #[must_use]
    fn spawn_default() -> ActorRef<Self>
    where
        Self::Args: Default,
    {
        Spawn::spawn(Self::Args::default())
    }

    /// Spawns the actor in a Tokio task with a specific mailbox configuration.
    ///
    /// This function allows you to explicitly specify a mailbox when spawning an actor.
    /// Use this when you need custom mailbox behavior or capacity.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    /// use thespis::mailbox;
    ///
    /// #[derive(Actor)]
    /// struct MyActor;
    ///
    /// # tokio_test::block_on(async {
    /// // Using a bounded mailbox with custom capacity
    /// let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(1000));
    ///
    /// // Using an unbounded mailbox
    /// let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());
    /// # })
    /// ```
    fn spawn_with_mailbox(
        args: Self::Args,
        (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>),
    ) -> ActorRef<Self> {
        let prepared_actor = PreparedActor::new((mailbox_tx, mailbox_rx));
        let actor_ref = prepared_actor.actor_ref().clone();
        prepared_actor.spawn(args);
        actor_ref
    }

    /// Spawns and links the actor in a Tokio task with a default bounded mailbox.
    ///
    /// This function is used to ensure an actor is linked with another actor before it's truly spawned,
    /// which avoids possible edge cases where the actor could die before having the chance to be linked.
    ///
    /// By default, a bounded mailbox with capacity 64 is used to provide backpressure.
    /// For custom mailbox configuration, use [`Spawn::spawn_link_with_mailbox`].
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    ///
    /// #[derive(Actor)]
    /// struct FooActor;
    ///
    /// #[derive(Actor)]
    /// struct BarActor;
    ///
    /// # tokio_test::block_on(async {
    /// let link_ref = FooActor::spawn(FooActor);
    /// // Spawns with default bounded mailbox (capacity 64)
    /// let actor_ref = BarActor::spawn_link(&link_ref, BarActor).await;
    /// # })
    /// ```
    fn spawn_link<L>(
        link_ref: &ActorRef<L>,
        args: Self::Args,
    ) -> impl Future<Output = ActorRef<Self>> + Send
    where
        L: Actor,
    {
        <Self as Spawn>::spawn_link_with_mailbox::<L>(
            link_ref,
            args,
            mailbox::bounded(DEFAULT_MAILBOX_CAPACITY),
        )
    }

    /// Spawns and links the actor in a Tokio task with a specific mailbox configuration.
    ///
    /// This function is used to ensure an actor is linked with another actor before it's truly spawned,
    /// which avoids possible edge cases where the actor could die before having the chance to be linked.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    /// use thespis::mailbox;
    ///
    /// #[derive(Actor)]
    /// struct FooActor;
    ///
    /// #[derive(Actor)]
    /// struct BarActor;
    ///
    /// # tokio_test::block_on(async {
    /// let link_ref = FooActor::spawn(FooActor);
    /// // Using a custom mailbox
    /// let actor_ref = BarActor::spawn_link_with_mailbox(&link_ref, BarActor, mailbox::unbounded()).await;
    /// # })
    /// ```
    fn spawn_link_with_mailbox<L>(
        link_ref: &ActorRef<L>,
        args: Self::Args,
        (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>),
    ) -> impl Future<Output = ActorRef<Self>> + Send
    where
        L: Actor,
    {
        async move {
            let prepared_actor = PreparedActor::new((mailbox_tx, mailbox_rx));
            let actor_ref = prepared_actor.actor_ref().clone();
            actor_ref.link(link_ref).await;
            prepared_actor.spawn(args);
            actor_ref
        }
    }

    /// Spawns the actor in its own dedicated thread with a default bounded mailbox.
    ///
    /// This function spawns the actor in a separate thread, making it suitable for actors that perform blocking
    /// operations, such as file I/O or other tasks that cannot be efficiently executed in an asynchronous context.
    /// Despite running in a blocking thread, the actor can still communicate asynchronously with other actors.
    ///
    /// By default, a bounded mailbox with capacity 64 is used to provide backpressure.
    /// For custom mailbox configuration, use [`Spawn::spawn_in_thread_with_mailbox`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::io::{self, Write};
    /// use std::fs::File;
    ///
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    /// use thespis::message::{Context, Message};
    ///
    /// #[derive(Actor)]
    /// struct MyActor {
    ///     file: File,
    /// }
    ///
    /// struct Flush;
    /// impl Message<Flush> for MyActor {
    ///     type Reply = io::Result<()>;
    ///
    ///     async fn handle(&mut self, _: Flush, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
    ///         self.file.flush() // This blocking operation is handled in its own thread
    ///     }
    /// }
    ///
    /// let actor_ref = MyActor::spawn_in_thread(
    ///     MyActor { file: File::create("output.txt").unwrap() }
    /// );
    /// actor_ref.tell(Flush).blocking_send()?;
    /// # Ok::<(), thespis::error::SendError<Flush>>(())
    /// ```
    ///
    /// This function is useful for actors that require or benefit from running blocking operations while still
    /// enabling asynchronous functionality.
    fn spawn_in_thread(args: Self::Args) -> ActorRef<Self> {
        Spawn::spawn_in_thread_with_mailbox(args, mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
    }

    /// Spawns the actor in its own dedicated thread with a specific mailbox configuration.
    ///
    /// This function allows you to explicitly specify a mailbox when spawning an actor in a dedicated thread.
    /// Use this when you need custom mailbox behavior or capacity for actors that perform blocking operations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::io::{self, Write};
    /// use std::fs::File;
    ///
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    /// use thespis::mailbox;
    /// use thespis::message::{Context, Message};
    ///
    /// #[derive(Actor)]
    /// struct MyActor {
    ///     file: File,
    /// }
    ///
    /// struct Flush;
    /// impl Message<Flush> for MyActor {
    ///     type Reply = io::Result<()>;
    ///
    ///     async fn handle(&mut self, _: Flush, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
    ///         self.file.flush() // This blocking operation is handled in its own thread
    ///     }
    /// }
    ///
    /// let actor_ref = MyActor::spawn_in_thread_with_mailbox(
    ///     MyActor { file: File::create("output.txt").unwrap() },
    ///     mailbox::bounded(100)
    /// );
    /// actor_ref.tell(Flush).blocking_send()?;
    /// # Ok::<(), thespis::error::SendError<Flush>>(())
    /// ```
    fn spawn_in_thread_with_mailbox(
        args: Self::Args,
        (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>),
    ) -> ActorRef<Self> {
        let prepared_actor = PreparedActor::new((mailbox_tx, mailbox_rx));
        let actor_ref = prepared_actor.actor_ref().clone();
        prepared_actor.spawn_in_thread(args);
        actor_ref
    }

    /// Creates a new prepared actor, allowing access to its [`ActorRef`] before spawning.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    ///
    /// #[derive(Actor)]
    /// struct MyActor;
    ///
    /// # tokio_test::block_on(async {
    /// let other_actor = MyActor::spawn(MyActor);
    /// let prepared_actor = MyActor::prepare();
    /// prepared_actor.actor_ref().link(&other_actor).await;
    /// let actor_ref = prepared_actor.spawn(MyActor);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    fn prepare() -> PreparedActor<Self> {
        Spawn::prepare_with_mailbox(mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
    }

    /// Creates a new prepared actor with a specific mailbox configuration, allowing access to its [`ActorRef`] before spawning.
    ///
    /// This function allows you to explicitly specify a mailbox when preparing an actor.
    /// Use this when you need custom mailbox behavior or capacity.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::Actor;
    /// use thespis::actor::Spawn;
    /// use thespis::mailbox;
    ///
    ///  #[derive(Actor)]
    ///  struct MyActor;
    ///
    /// # tokio_test::block_on(async {
    /// let other_actor = MyActor::spawn(MyActor);
    /// let prepared_actor = MyActor::prepare_with_mailbox(mailbox::unbounded());
    /// prepared_actor.actor_ref().link(&other_actor).await;
    /// let actor_ref = prepared_actor.spawn(MyActor);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    fn prepare_with_mailbox(
        (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>),
    ) -> PreparedActor<Self> {
        PreparedActor::new((mailbox_tx, mailbox_rx))
    }
}

impl<A: Actor> Spawn for A {}

mod private {
    use super::Actor;

    pub trait Sealed {}
    impl<A: Actor> Sealed for A {}
}

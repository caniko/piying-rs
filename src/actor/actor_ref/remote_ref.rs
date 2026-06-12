/// A reference to an actor running remotely.
///
/// `RemoteActorRef` allows sending messages to actors on different nodes in a distributed system.
/// It supports the same messaging patterns as `ActorRef` for local actors, including `ask` and `tell` messaging.
#[cfg(feature = "remote")]
pub struct RemoteActorRef<A: Actor> {
    id: ActorId,
    swarm_tx: remote::SwarmSender,
    phantom: PhantomData<fn(&mut A)>,
}

#[cfg(feature = "remote")]
impl<A> RemoteActorRef<A>
where
    A: Actor + remote::RemoteActor,
{
    /// Creates a new `RemoteActorRef` with the given actor ID and swarm sender.
    pub fn new(id: ActorId, swarm_tx: remote::SwarmSender) -> Self {
        RemoteActorRef {
            id,
            swarm_tx,
            phantom: PhantomData,
        }
    }

    /// Create a `RemoteActorRef` for a known remote peer without DHT lookup.
    ///
    /// This bypasses Kademlia discovery and constructs the reference directly.
    /// The message routing uses the `actor_remote_id` string on the remote side,
    /// so the `sequence_id` in the `ActorId` is set to 0 (unused for routing).
    ///
    /// Returns `None` if the `ActorSwarm` is not initialized.
    pub fn for_peer(peer_id: libp2p::PeerId) -> Option<Self> {
        let swarm = remote::ActorSwarm::get()?;
        let actor_id = ActorId::new_with_peer_id(0, peer_id);
        Some(RemoteActorRef {
            id: actor_id,
            swarm_tx: swarm.sender().clone(),
            phantom: PhantomData,
        })
    }

    /// Creates a `RemoteActorRef` for the well-known actor on the given peer,
    /// using an explicit `SwarmSender` instead of reading the global `ActorSwarm`.
    ///
    /// This is infallible — it never fails because it doesn't depend on global state.
    /// Uses the well-known `ActorId(0, peer_id)` convention.
    pub fn for_peer_with_sender(peer_id: libp2p::PeerId, swarm_tx: remote::SwarmSender) -> Self {
        let actor_id = ActorId::new_with_peer_id(0, peer_id);
        Self {
            id: actor_id,
            swarm_tx,
            phantom: PhantomData,
        }
    }

    /// Returns the unique identifier of the remote actor.
    pub fn id(&self) -> ActorId {
        self.id
    }

    /// Looks up a single actor registered by name across the distributed network.
    ///
    /// If multiple actors are registered under the same name, returns one of them.
    /// The specific actor returned is not deterministic and may vary between calls.
    ///
    /// Returns `None` if no actor with the given name is found.
    ///
    /// Use [`lookup_all`] when multiple actors might exist and you need deterministic behavior.
    ///
    /// [`lookup_all`]: Self::lookup_all
    pub async fn lookup(name: impl Into<Arc<str>>) -> Result<Option<Self>, error::RegistryError>
    where
        A: remote::RemoteActor + 'static,
    {
        remote::ActorSwarm::get()
            .ok_or(error::RegistryError::SwarmNotBootstrapped)?
            .lookup(name.into())
            .await
    }

    /// Looks up all actors registered by name across the distributed network.
    ///
    /// Returns a stream of all remote actor refs found under the given name.
    /// The stream completes when all known actors have been discovered.
    ///
    /// Use this when multiple actors may be registered under the same name
    /// and you need to handle all of them or make deterministic choices.
    pub fn lookup_all(name: impl Into<Arc<str>>) -> remote::LookupStream<A>
    where
        A: remote::RemoteActor + 'static,
    {
        match remote::ActorSwarm::get() {
            Some(swarm) => swarm.lookup_all(name.into()),
            None => remote::LookupStream::new_err(),
        }
    }

    /// Sends a message to the remote actor and waits for a reply.
    ///
    /// The `ask` pattern is used when a response is expected from the remote actor. This method
    /// returns an `AskRequest`, which can be awaited asynchronously, or sent in a blocking manner using one of the [`request`](crate::request) traits.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use thespis::actor::RemoteActorRef;
    ///
    /// # #[derive(thespis::Actor, thespis::RemoteActor)]
    /// # struct MyActor;
    /// #
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Msg;
    /// #
    /// # #[thespis::remote_message("id")]
    /// # impl thespis::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut thespis::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let remote_actor_ref = RemoteActorRef::<MyActor>::lookup("my_actor").await?.unwrap();
    /// # let msg = Msg;
    /// let reply = remote_actor_ref.ask(&msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    #[doc(alias = "send")]
    pub fn ask<'a, M>(
        &'a self,
        msg: &'a M,
    ) -> request::RemoteAskRequest<'a, A, M, WithoutRequestTimeout, WithoutRequestTimeout>
    where
        A: remote::RemoteActor + Message<M> + remote::RemoteMessage<M>,
        M: Send + 'static,
    {
        request::RemoteAskRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }

    /// Sends a message to the remote actor without waiting for a reply.
    ///
    /// The `tell` pattern is used when no response is expected from the remote actor. This method
    /// returns a `TellRequest`, which can be awaited asynchronously, or configured using one of the [`request`](crate::request) traits.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use thespis::actor::RemoteActorRef;
    ///
    /// # #[derive(thespis::Actor, thespis::RemoteActor)]
    /// # struct MyActor;
    /// #
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Msg;
    /// #
    /// # #[thespis::remote_message("id")]
    /// # impl thespis::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut thespis::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let remote_actor_ref = RemoteActorRef::<MyActor>::lookup("my_actor").await?.unwrap();
    /// # let msg = Msg;
    /// remote_actor_ref.tell(&msg).send()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    #[doc(alias = "send_async")]
    pub fn tell<'a, M>(
        &'a self,
        msg: &'a M,
    ) -> request::RemoteTellRequest<'a, A, M, WithoutRequestTimeout>
    where
        A: Message<M> + remote::RemoteMessage<M>,
        M: Send + 'static,
    {
        request::RemoteTellRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }

    /// Sends a fire-and-forget message to the remote actor synchronously.
    ///
    /// Unlike [`tell`](Self::tell), this does not return a future and performs no async work.
    /// It encodes the message and enqueues it for the swarm in a single synchronous step.
    /// This is useful when sending from non-async contexts (e.g., Bevy ECS observers).
    ///
    /// No delivery guarantee: the message is enqueued but the caller does not wait
    /// for a network-level acknowledgement.
    #[inline]
    pub fn tell_sync<M>(&self, msg: &M) -> Result<(), error::RemoteSendError>
    where
        A: Message<M> + remote::RemoteMessage<M>,
        M: remote::codec::Encode + Send + 'static,
    {
        let payload = msg
            .encode()
            .map_err(error::RemoteSendError::SerializeMessage)?;
        self.send_to_swarm(remote::SwarmCommand::Tell {
            actor_id: self.id,
            actor_remote_id: Cow::Borrowed(<A as remote::RemoteActor>::REMOTE_ID),
            message_remote_id: Cow::Borrowed(<A as remote::RemoteMessage<M>>::REMOTE_ID),
            payload,
            mailbox_timeout: None,
            immediate: true,
            reply: None,
        });
        Ok(())
    }

    /// Links two remote actors, ensuring they notify each other if either one dies.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use thespis::actor::RemoteActorRef;
    /// #
    /// # #[derive(thespis::Actor, thespis::RemoteActor)]
    /// # struct ActorA;
    /// #
    /// # #[derive(thespis::Actor, thespis::RemoteActor)]
    /// # struct ActorB;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_a = RemoteActorRef::<ActorA>::lookup("actor_a").await?.unwrap();
    /// let actor_b = RemoteActorRef::<ActorB>::lookup("actor_b").await?.unwrap();
    ///
    /// actor_a.unlink_remote(&actor_b).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn link_remote<B>(
        &self,
        sibling_ref: &RemoteActorRef<B>,
    ) -> Result<(), error::RemoteSendError<error::Infallible>>
    where
        A: remote::RemoteActor,
        B: Actor + remote::RemoteActor,
    {
        if self.id == sibling_ref.id {
            return Ok(());
        }

        let swarm =
            remote::ActorSwarm::get().ok_or(error::RemoteSendError::SwarmNotBootstrapped)?;
        let fut_a = swarm.link::<A, B>(self.id, sibling_ref.id);
        let fut_b = swarm.link::<B, A>(sibling_ref.id, self.id);

        tokio::try_join!(fut_a, fut_b)?;

        Ok(())
    }

    /// Unlinks two previously linked remote actors.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use thespis::actor::RemoteActorRef;
    /// #
    /// # #[derive(thespis::Actor, thespis::RemoteActor)]
    /// # struct ActorA;
    /// #
    /// # #[derive(thespis::Actor, thespis::RemoteActor)]
    /// # struct ActorB;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_a = RemoteActorRef::<ActorA>::lookup("actor_a").await?.unwrap();
    /// let actor_b = RemoteActorRef::<ActorB>::lookup("actor_b").await?.unwrap();
    ///
    /// actor_a.unlink_remote(&actor_b).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn unlink_remote<B>(
        &self,
        sibling_ref: &RemoteActorRef<B>,
    ) -> Result<(), error::RemoteSendError<error::Infallible>>
    where
        A: remote::RemoteActor,
        B: Actor + remote::RemoteActor,
    {
        if self.id == sibling_ref.id {
            return Ok(());
        }

        let swarm =
            remote::ActorSwarm::get().ok_or(error::RemoteSendError::SwarmNotBootstrapped)?;
        let fut_a = swarm.unlink::<B>(self.id, sibling_ref.id);
        let fut_b = swarm.unlink::<A>(sibling_ref.id, self.id);

        tokio::try_join!(fut_a, fut_b)?;

        Ok(())
    }

    pub(crate) fn send_to_swarm(&self, msg: remote::SwarmCommand) {
        self.swarm_tx.send(msg)
    }
}

#[cfg(feature = "remote")]
impl<A: Actor> Clone for RemoteActorRef<A> {
    fn clone(&self) -> Self {
        RemoteActorRef {
            id: self.id,
            swarm_tx: self.swarm_tx.clone(),
            phantom: PhantomData,
        }
    }
}

#[cfg(feature = "remote")]
impl<A: Actor> fmt::Debug for RemoteActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("RemoteActorRef");
        d.field("id", &self.id);
        d.finish()
    }
}

#[cfg(feature = "remote")]
impl<A: Actor> PartialEq for RemoteActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[cfg(feature = "remote")]
impl<A: Actor> Eq for RemoteActorRef<A> {}

#[cfg(feature = "remote")]
impl<A: Actor> PartialOrd for RemoteActorRef<A> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "remote")]
impl<A: Actor> Ord for RemoteActorRef<A> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

#[cfg(feature = "remote")]
impl<A: Actor> Hash for RemoteActorRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[cfg(all(feature = "remote", feature = "serde"))]
impl<A: Actor> serde::Serialize for RemoteActorRef<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut ser = serializer.serialize_struct("RemoteActorRef", 1)?;
        ser.serialize_field("id", &self.id)?;
        ser.end()
    }
}

#[cfg(all(feature = "remote", feature = "serde"))]
impl<'de, A: Actor> serde::Deserialize<'de> for RemoteActorRef<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdVisitor<A>(std::marker::PhantomData<A>);

        impl<'de, A: Actor> serde::de::Visitor<'de> for IdVisitor<A> {
            type Value = RemoteActorRef<A>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct RemoteActorRef")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut id = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        "id" => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(map.next_value()?);
                        }
                        _ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                let id = id.ok_or_else(|| serde::de::Error::missing_field("id"))?;
                let swarm = remote::ActorSwarm::get()
                    .ok_or_else(|| serde::de::Error::custom("actor swarm not bootstrapped"))?;

                Ok(RemoteActorRef {
                    id,
                    swarm_tx: swarm.sender().clone(),
                    phantom: PhantomData,
                })
            }

            fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
            where
                S: serde::de::SeqAccess<'de>,
            {
                let id: Option<ActorId> = seq.next_element()?;
                let id = id.ok_or_else(|| serde::de::Error::missing_field("id"))?;

                let swarm = remote::ActorSwarm::get()
                    .ok_or_else(|| serde::de::Error::custom("actor swarm not bootstrapped"))?;

                Ok(RemoteActorRef {
                    id,
                    swarm_tx: swarm.sender().clone(),
                    phantom: PhantomData,
                })
            }
        }

        let visitor = IdVisitor(std::marker::PhantomData);
        deserializer.deserialize_struct("RemoteActorRef", &["id"], visitor)
    }
}


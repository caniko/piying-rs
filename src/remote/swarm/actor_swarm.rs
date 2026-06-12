impl ActorSwarm {
    /// Retrieves a shared reference to the current `ActorSwarm` if it has been bootstrapped.
    ///
    /// Checks the thread-local override first, then falls back to the global.
    /// Prefer [`with`](Self::with) when you only need a transient borrow to avoid even
    /// the Arc refcount bump.
    ///
    /// ## Returns
    /// An optional `Arc<ActorSwarm>`, or `None` if it has not been bootstrapped.
    pub fn get() -> Option<Arc<Self>> {
        THREAD_LOCAL_SWARM
            .with(|tls| tls.borrow().clone())
            .or_else(|| ACTOR_SWARM.load_full())
    }

    /// Borrows the current `ActorSwarm` for the duration of the closure.
    ///
    /// Checks the thread-local override first, then falls back to the global.
    /// Returns `None` if neither is set.
    pub fn with<R>(f: impl FnOnce(&Self) -> R) -> Option<R> {
        let tls = THREAD_LOCAL_SWARM.with(|tls| tls.borrow().clone());
        if let Some(ref swarm) = tls {
            return Some(f(swarm));
        }
        let guard = ACTOR_SWARM.load();
        (*guard).as_ref().map(|arc| f(arc))
    }

    /// Replaces the global ActorSwarm. Always succeeds (overwrites previous value).
    pub(crate) fn set(
        swarm_tx: mpsc::UnboundedSender<SwarmCommand>,
        local_peer_id: PeerId,
    ) -> Result<(), Self> {
        let new = Self {
            swarm_tx: SwarmSender(swarm_tx),
            local_peer_id,
        };
        ACTOR_SWARM.store(Some(Arc::new(new)));
        Ok(())
    }

    /// Set a thread-local `ActorSwarm` that shadows the global for the current
    /// thread. Returns a guard that clears it on drop.
    ///
    /// Useful for test isolation when multiple swarms coexist in the same
    /// process (e.g., batch test harness).
    #[allow(dead_code)]
    pub fn set_thread_local(
        swarm_tx: mpsc::UnboundedSender<SwarmCommand>,
        local_peer_id: PeerId,
    ) -> SwarmGuard {
        let swarm = Arc::new(Self {
            swarm_tx: SwarmSender(swarm_tx),
            local_peer_id,
        });
        THREAD_LOCAL_SWARM.with(|tls| *tls.borrow_mut() = Some(swarm));
        SwarmGuard {
            _not_send: PhantomData,
        }
    }

    /// Returns the local peer ID, which uniquely identifies this node in the libp2p network.
    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    /// Looks up an actor running locally.
    pub(crate) fn lookup_local<A: Actor + RemoteActor + 'static>(
        &self,
        name: Arc<str>,
    ) -> impl Future<Output = Result<Option<ActorRef<A>>, RegistryError>> {
        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::LookupLocal { name, reply });

        async move {
            let Some(ActorRegistration {
                actor_id,
                remote_id,
            }) = reply_rx.await?
            else {
                return Ok(None);
            };
            if A::REMOTE_ID != remote_id {
                return Err(RegistryError::BadActorType);
            }

            let registry = REMOTE_REGISTRY.lock().await;
            let Some(actor_ref_any) = registry.get(&actor_id) else {
                return Ok(None);
            };
            match actor_ref_any.downcast() {
                Ok(actor_ref) => Ok(Some(actor_ref)),
                Err(DowncastRegsiteredActorRefError::BadActorType) => {
                    Err(RegistryError::BadActorType)
                }
                Err(DowncastRegsiteredActorRefError::ActorNotRunning) => Ok(None),
            }
        }
    }

    /// Looks up an actor in the swarm.
    pub(crate) async fn lookup<A: Actor + RemoteActor>(
        &self,
        name: Arc<str>,
    ) -> Result<Option<RemoteActorRef<A>>, RegistryError> {
        #[cfg(all(debug_assertions, feature = "tracing"))]
        let name_clone = name.clone();
        let mut stream = self.lookup_all(name);

        let first = stream.next().await.transpose()?;

        #[cfg(all(debug_assertions, feature = "tracing"))]
        if first.is_some() {
            tokio::spawn(async move {
                // Check if there's a second actor
                if let Ok(Some(_)) = stream.next().await.transpose() {
                    tracing::warn!(
                        "Multiple actors found for '{name_clone}'. Consider using lookup_all() for deterministic behavior when multiple actors may exist."
                    );
                }
            });
        }

        Ok(first)
    }

    /// Looks up all actors with a given name in the swarm.
    pub(crate) fn lookup_all<A: Actor + RemoteActor>(&self, name: Arc<str>) -> LookupStream<A> {
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        let cmd = SwarmCommand::Lookup {
            name,
            reply: reply_tx,
        };
        self.swarm_tx.send(cmd);

        let swarm_tx = self.swarm_tx.clone();
        LookupStream::new(swarm_tx, reply_rx)
    }

    /// Registers an actor within the swarm.
    pub(crate) fn register<A: Actor + RemoteActor + 'static>(
        &self,
        actor_ref: ActorRef<A>,
        name: Arc<str>,
    ) -> impl Future<Output = Result<(), RegistryError>> {
        let registration = ActorRegistration::new(actor_ref.id(), Cow::Borrowed(A::REMOTE_ID));

        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::Register {
                name: name.clone(),
                registration,
                reply,
            });

        async move {
            let res = reply_rx.await;
            match res {
                Ok(()) | Err(RegistryError::QuorumFailed { .. }) => {
                    REMOTE_REGISTRY.lock().await.insert(
                        actor_ref.id(),
                        RemoteRegistryActorRef::new(actor_ref, Some(name)),
                    );

                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
    }

    /// Unregisters an actor within the swarm.
    ///
    /// The future returned by unregister does not have to be awaited.
    /// Awaiting it is only necessary to handle the result.
    pub fn unregister(&self, name: Arc<str>) -> impl Future<Output = ()> {
        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::Unregister { name, reply });

        async move {
            reply_rx.await;
        }
    }

    pub(crate) fn link<A: Actor + RemoteActor, B: Actor + RemoteActor>(
        &self,
        actor_id: ActorId,
        sibling_id: ActorId,
    ) -> impl Future<Output = Result<(), RemoteSendError<Infallible>>> {
        let reply_rx = self.swarm_tx.send_with_reply(|reply| SwarmCommand::Link {
            actor_id,
            actor_remote_id: Cow::Borrowed(A::REMOTE_ID),
            sibling_id,
            sibling_remote_id: Cow::Borrowed(B::REMOTE_ID),
            reply,
        });

        async move {
            match reply_rx.await {
                SwarmResponse::Link(result) => result.map_err(|e| e.into_infallible()),
                SwarmResponse::OutboundFailure(err) => Err(err.into_infallible()),
                _ => panic!("got an unexpected swarm response"),
            }
        }
    }

    pub(crate) fn unlink<B: Actor + RemoteActor>(
        &self,
        actor_id: ActorId,
        sibling_id: ActorId,
    ) -> impl Future<Output = Result<(), RemoteSendError<Infallible>>> {
        let reply_rx = self.swarm_tx.send_with_reply(|reply| SwarmCommand::Unlink {
            actor_id,
            sibling_id,
            sibling_remote_id: Cow::Borrowed(B::REMOTE_ID),
            reply,
        });

        async move {
            match reply_rx.await {
                SwarmResponse::Unlink(result) => result.map_err(|e| e.into_infallible()),
                SwarmResponse::OutboundFailure(err) => Err(err.into_infallible()),
                _ => panic!("got an unexpected swarm response"),
            }
        }
    }

    pub(crate) fn signal_link_died(
        &self,
        dead_actor_id: ActorId,
        notified_actor_id: ActorId,
        notified_actor_remote_id: Cow<'static, str>,
        stop_reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), RemoteSendError<Infallible>>> {
        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
                reply,
            });

        async move {
            match reply_rx.await {
                SwarmResponse::SignalLinkDied(result) => result.map_err(|e| e.into_infallible()),
                SwarmResponse::OutboundFailure(err) => Err(err.into_infallible()),
                _ => panic!("got an unexpected swarm response"),
            }
        }
    }

    /// Returns a reference to the swarm command sender.
    pub fn sender(&self) -> &SwarmSender {
        &self.swarm_tx
    }
}


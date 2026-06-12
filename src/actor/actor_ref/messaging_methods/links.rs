impl<A> ActorRef<A>
where
    A: Actor,
{
    /// Links two actors as siblings, ensuring they notify each other if either one dies.
    ///
    /// # Example
    ///
    /// ```
    /// # use thespis::Actor;
    /// # use thespis::actor::Spawn;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibling_ref = MyActor::spawn(MyActor);
    ///
    /// actor_ref.link(&sibling_ref).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub async fn link<B: Actor>(&self, sibling_ref: &ActorRef<B>) {
        if self.id == sibling_ref.id {
            return;
        }

        if self.id < sibling_ref.id {
            let mut this_links = self.links.lock().await;
            let mut sibling_links = sibling_ref.links.lock().await;

            this_links.insert(
                sibling_ref.id,
                Link::Local(sibling_ref.weak_signal_mailbox()),
            );
            sibling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
        } else {
            let mut sibling_links = sibling_ref.links.lock().await;
            let mut this_links = self.links.lock().await;

            this_links.insert(
                sibling_ref.id,
                Link::Local(sibling_ref.weak_signal_mailbox()),
            );
            sibling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
        }
    }

    /// Blockingly links two actors as siblings, ensuring they notify each other if either one dies.
    ///
    /// This method is intended for use cases where you need to link actors in synchronous code.
    /// For async contexts, [`link`] is preferred.
    ///
    /// # Example
    ///
    /// ```
    /// use std::thread;
    ///
    /// # use thespis::Actor;
    /// # use thespis::actor::Spawn;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibling_ref = MyActor::spawn(MyActor);
    ///
    /// thread::spawn(move || {
    ///     actor_ref.blocking_link(&sibling_ref);
    /// });
    /// # });
    /// ```
    ///
    /// [`link`]: ActorRef::link
    #[inline]
    pub fn blocking_link<B: Actor>(&self, sibling_ref: &ActorRef<B>) {
        if self.id == sibling_ref.id {
            return;
        }

        if self.id < sibling_ref.id {
            let mut this_links = self.links.blocking_lock();
            let mut sibling_links = sibling_ref.links.blocking_lock();

            this_links.insert(
                sibling_ref.id,
                Link::Local(sibling_ref.weak_signal_mailbox()),
            );
            sibling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
        } else {
            let mut sibling_links = sibling_ref.links.blocking_lock();
            let mut this_links = self.links.blocking_lock();

            this_links.insert(
                sibling_ref.id,
                Link::Local(sibling_ref.weak_signal_mailbox()),
            );
            sibling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
        }
    }

    /// Links the local actor with a remote actor, ensuring they notify each other if either one dies.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use thespis::Actor;
    /// # use thespis::actor::{RemoteActorRef, Spawn};
    /// #
    /// # #[derive(Actor, thespis::RemoteActor)]
    /// # struct MyActor;
    /// #
    /// # #[derive(Actor, thespis::RemoteActor)]
    /// # struct OtherActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibling_ref = RemoteActorRef::<OtherActor>::lookup("other_actor").await?.unwrap();
    ///
    /// actor_ref.link_remote(&sibling_ref).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[cfg(feature = "remote")]
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

        remote::REMOTE_REGISTRY
            .lock()
            .await
            .entry(self.id)
            .or_insert_with(|| remote::RemoteRegistryActorRef::new(self.clone(), None));

        self.links.lock().await.insert(
            sibling_ref.id,
            Link::Remote(std::borrow::Cow::Borrowed(B::REMOTE_ID)),
        );
        remote::ActorSwarm::get()
            .ok_or(error::RemoteSendError::SwarmNotBootstrapped)?
            .link::<A, B>(self.id, sibling_ref.id)
            .await
    }

    /// Unlinks two previously linked sibling actors.
    ///
    /// # Example
    ///
    /// ```
    /// # use thespis::Actor;
    /// # use thespis::actor::Spawn;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibling_ref = MyActor::spawn(MyActor);
    ///
    /// actor_ref.link(&sibling_ref).await;
    /// actor_ref.unlink(&sibling_ref).await;
    /// # });
    /// ```
    #[inline]
    pub async fn unlink<B: Actor>(&self, sibling_ref: &ActorRef<B>) {
        if self.id == sibling_ref.id {
            return;
        }

        if self.id < sibling_ref.id {
            let mut this_links = self.links.lock().await;
            let mut sibling_links = sibling_ref.links.lock().await;

            this_links.remove(&sibling_ref.id);
            sibling_links.remove(&self.id);
        } else {
            let mut sibling_links = sibling_ref.links.lock().await;
            let mut this_links = self.links.lock().await;

            this_links.remove(&sibling_ref.id);
            sibling_links.remove(&self.id);
        }
    }

    /// Blockingly unlinks two previously linked sibling actors.
    ///
    /// This method is intended for use cases where you need to link actors in synchronous code.
    /// For async contexts, [`unlink`] is preferred.
    ///
    ///
    /// # Example
    ///
    /// ```
    /// # use std::thread;
    /// #
    /// # use thespis::Actor;
    /// # use thespis::actor::Spawn;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibling_ref = MyActor::spawn(MyActor);
    ///
    /// thread::spawn(move || {
    ///     actor_ref.blocking_link(&sibling_ref);
    ///     actor_ref.blocking_unlink(&sibling_ref);
    /// });
    /// # });
    /// ```
    ///
    /// [`unlink`]: ActorRef::unlink
    #[inline]
    pub fn blocking_unlink<B: Actor>(&self, sibling_ref: &ActorRef<B>) {
        if self.id == sibling_ref.id {
            return;
        }

        if self.id < sibling_ref.id {
            let mut this_links = self.links.blocking_lock();
            let mut sibling_links = sibling_ref.links.blocking_lock();

            this_links.remove(&sibling_ref.id);
            sibling_links.remove(&self.id);
        } else {
            let mut sibling_links = sibling_ref.links.blocking_lock();
            let mut this_links = self.links.blocking_lock();

            this_links.remove(&sibling_ref.id);
            sibling_links.remove(&self.id);
        }
    }

    /// Unlinks the local actor with a previously linked remote actor.
    ///
    /// # Example
    ///
    /// ```
    /// # use thespis::Actor;
    /// # use thespis::actor::{RemoteActorRef, Spawn};
    /// #
    /// # #[derive(Actor, thespis::RemoteActor)]
    /// # struct MyActor;
    /// #
    /// # #[derive(Actor, thespis::RemoteActor)]
    /// # struct OtherActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibling_ref = RemoteActorRef::<OtherActor>::lookup("other_actor").await?.unwrap();
    ///
    /// actor_ref.unlink_remote(&sibling_ref).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[cfg(feature = "remote")]
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

        self.links.lock().await.remove(&sibling_ref.id);
        remote::ActorSwarm::get()
            .ok_or(error::RemoteSendError::SwarmNotBootstrapped)?
            .unlink::<B>(self.id, sibling_ref.id)
            .await
    }


}

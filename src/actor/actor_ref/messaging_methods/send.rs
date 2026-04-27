impl<A> ActorRef<A>
where
    A: Actor,
{

    /// Sends a message to the actor and waits for a reply.
    ///
    /// The `ask` pattern is used when you expect a response from the actor. This method returns
    /// an `AskRequest`, which can be awaited asynchronously, or sent in a blocking manner using one of the [`request`](crate::request) traits.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::actor::{Actor, ActorRef, Spawn};
    ///
    /// # #[derive(thespis::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl thespis::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut thespis::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// let reply = actor_ref.ask(msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    #[doc(alias = "send")]
    pub fn ask<M>(
        &self,
        msg: M,
    ) -> AskRequest<'_, A, M, WithoutRequestTimeout, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        AskRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }

    /// Sends a message to the actor without waiting for a reply.
    ///
    /// The `tell` pattern is used for one-way communication, where no response is expected from the actor. This method
    /// returns a `TellRequest`, which can be awaited asynchronously, or configured using one of the [`request`](crate::request) traits.
    ///
    /// # Example
    ///
    /// ```
    /// use thespis::actor::{Actor, ActorRef, Spawn};
    ///
    /// # #[derive(thespis::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl thespis::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut thespis::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// actor_ref.tell(msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    #[doc(alias = "send_async")]
    pub fn tell<M>(&self, msg: M) -> TellRequest<'_, A, M, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }


}

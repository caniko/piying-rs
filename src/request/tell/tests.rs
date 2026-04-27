#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use crate::{
        Actor,
        actor::{ActorRef, Spawn},
        error::{Infallible, SendError},
        mailbox,
        message::{Context, Message},
    };

    #[tokio::test]
    async fn bounded_tell_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(100));

        actor_ref.tell(Msg).await?; // Should be a regular MessageSend request
        actor_ref.tell(Msg).send().await?;
        actor_ref.tell(Msg).try_send()?;
        tokio::task::spawn_blocking({
            let actor_ref = actor_ref.clone();
            move || actor_ref.tell(Msg).blocking_send()
        })
        .await??;

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_tell_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());

        actor_ref.tell(Msg).await?; // Should be a regular MessageSend request
        actor_ref.tell(Msg).send().await?;
        actor_ref.tell(Msg).try_send()?;
        actor_ref.tell(Msg).blocking_send()?;

        Ok(())
    }

    #[tokio::test]
    async fn bounded_tell_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(100));
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_shutdown().await;

        assert_eq!(
            actor_ref.tell(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_send(),
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.tell(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_tell_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_shutdown().await;

        assert_eq!(
            actor_ref.tell(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_send(),
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.tell(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn bounded_tell_requests_mailbox_full() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(1));
        actor_ref.wait_for_startup().await;
        // We need enough messages to both (a) occupy the actor (sleeping 10s
        // in the handler) and (b) fill the bounded channel.  Without hotpath
        // the channel capacity is 1, so 2 messages suffice: the first is
        // dequeued by the actor after the 2ms yield, the second stays queued.
        #[cfg(not(feature = "hotpath"))]
        let fill_count = 2;
        #[cfg(feature = "hotpath")]
        let fill_count = 4;
        for _ in 0..fill_count {
            assert_eq!(actor_ref.tell(Msg).try_send(), Ok(()));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(
            actor_ref.tell(Msg).try_send(),
            Err(SendError::MailboxFull(Msg))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn bounded_tell_requests_mailbox_timeout() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Sleep(Duration);

        impl Message<Sleep> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(1));
        // Mailbox empty, will succeed
        assert_eq!(
            actor_ref
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(10))
                .send()
                .await,
            Ok(())
        );
        // Mailbox is empty, this will make there be one item in the mailbox
        #[cfg(not(feature = "hotpath"))]
        let fill_count = 1;
        #[cfg(feature = "hotpath")]
        let fill_count = 3;
        for _ in 0..fill_count {
            assert_eq!(
                actor_ref
                    .tell(Sleep(Duration::from_millis(100)))
                    .mailbox_timeout(Duration::from_millis(10))
                    .send()
                    .await,
                Ok(())
            );
        }
        // Finally, this one will fail because there's one item in the mailbox already.
        assert_eq!(
            actor_ref
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(50))
                .send()
                .await,
            Err(SendError::Timeout(Some(Sleep(Duration::from_millis(100)))))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn tell_request_send_after_delays_message() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor {
            handled: Arc<AtomicUsize>,
        }

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                self.handled.fetch_add(1, Ordering::SeqCst);
            }
        }

        let handled = Arc::new(AtomicUsize::new(0));
        let actor_ref = MyActor::spawn(MyActor {
            handled: handled.clone(),
        });

        let send = actor_ref.tell(Msg).send_after(Duration::from_millis(25));
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(handled.load(Ordering::SeqCst), 0);

        send.await??;
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(handled.load(Ordering::SeqCst), 1);

        actor_ref.kill();

        Ok(())
    }
}

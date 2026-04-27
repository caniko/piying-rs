#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        Actor,
        actor::{ActorRef, Spawn},
        error::{Infallible, SendError},
        mailbox,
        message::{Context, Message},
    };

    #[tokio::test]
    async fn bounded_ask_requests() -> Result<(), Box<dyn std::error::Error>> {
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
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(100));

        assert!(actor_ref.ask(Msg).await?); // Should be a regular MessageSend request
        assert!(actor_ref.ask(Msg).send().await?);
        assert!(actor_ref.ask(Msg).try_send().await?);
        assert!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await??
        );

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_ask_requests() -> Result<(), Box<dyn std::error::Error>> {
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
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());

        assert!(actor_ref.ask(Msg).await?); // Should be a regular MessageSend request
        assert!(actor_ref.ask(Msg).send().await?);
        assert!(actor_ref.ask(Msg).try_send().await?);
        assert!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await??
        );

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
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
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(100));
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_shutdown().await;

        assert_eq!(
            actor_ref.ask(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.ask(Msg).try_send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_ask_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
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
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_shutdown().await;

        assert_eq!(
            actor_ref.ask(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.ask(Msg).try_send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_mailbox_full() -> Result<(), Box<dyn std::error::Error>> {
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
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(Duration::from_secs(10)).await;
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(1));
        assert_eq!(actor_ref.tell(Msg).try_send(), Ok(()));
        assert_eq!(
            actor_ref.ask(Msg).try_send().await,
            Err(SendError::MailboxFull(Msg))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_mailbox_timeout() -> Result<(), Box<dyn std::error::Error>> {
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
            type Reply = bool;

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(1));
        // Mailbox empty, this will succeed
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
        let fill_count = 3; // Sadly, hotpath adds some proxy layers, causing the fill count to be 3 instead of 1
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
        // Mailbox has one item, this will fail
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(50))
                .send()
                .await,
            Err(SendError::Timeout(Some(Sleep(Duration::from_millis(100)))))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_reply_timeout() -> Result<(), Box<dyn std::error::Error>> {
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
            type Reply = bool;

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(100));
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(120))
                .send()
                .await,
            Ok(true)
        );
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(90))
                .send()
                .await,
            Err(SendError::Timeout(None))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_ask_requests_reply_timeout() -> Result<(), Box<dyn std::error::Error>> {
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
            type Reply = bool;

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(120))
                .send()
                .await,
            Ok(true)
        );
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(90))
                .send()
                .await,
            Err(SendError::Timeout(None))
        );
        actor_ref.kill();

        Ok(())
    }
}

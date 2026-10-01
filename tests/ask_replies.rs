use std::time::Duration;

use piying::{
    actor::{Actor, ActorRef, Spawn},
    error::{Infallible, SendError},
    message::{Context, Message},
};
use tokio::sync::oneshot;

struct ReplyActor;

impl Actor for ReplyActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
}

impl Message<Result<u32, u32>> for ReplyActor {
    type Reply = Result<u32, u32>;

    async fn handle(
        &mut self,
        msg: Result<u32, u32>,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        msg
    }
}

impl Message<oneshot::Receiver<()>> for ReplyActor {
    type Reply = ();

    async fn handle(&mut self, gate: oneshot::Receiver<()>, _: &mut Context<Self, Self::Reply>) {
        let _ = gate.await;
    }
}

#[tokio::test]
async fn all_local_ask_variants_preserve_success_and_handler_errors() {
    let actor = ReplyActor::spawn(ReplyActor);
    for msg in [Ok(7), Err(9)] {
        let expected = msg.map_err(SendError::HandlerError);
        assert_eq!(actor.ask(msg).send().await, expected);
        assert_eq!(actor.ask(msg).try_send().await, expected);
        assert_eq!(actor.ask(msg).enqueue().await.unwrap().await, expected);
        assert_eq!(actor.ask(msg).try_enqueue().unwrap().await, expected);
        for enqueue in [false, true] {
            let actor = actor.clone();
            let result = tokio::task::spawn_blocking(move || {
                if enqueue {
                    actor.ask(msg).blocking_enqueue().unwrap().recv()
                } else {
                    actor.ask(msg).blocking_send()
                }
            })
            .await
            .unwrap();
            assert_eq!(result, expected);
        }
    }
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;
}

#[tokio::test]
async fn pending_replies_preserve_reply_timeout_errors() {
    let actor = ReplyActor::spawn(ReplyActor);
    for try_enqueue in [false, true] {
        let (release, gate) = oneshot::channel();
        let request = actor.ask(gate).reply_timeout(Duration::from_millis(1));
        let pending = if try_enqueue {
            request.try_enqueue().unwrap()
        } else {
            request.enqueue().await.unwrap()
        };
        assert!(matches!(pending.await, Err(SendError::Timeout(None))));
        drop(release);
    }
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;
}

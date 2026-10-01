use std::time::Duration;

use futures::FutureExt;
use piying::{
    actor::{Actor, ActorRef, PreparedActor},
    error::{Infallible, SendError},
    mailbox,
    message::{Context, Message},
};

struct Recorder(Vec<u32>);

impl Actor for Recorder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Record(u32);

impl Message<Record> for Recorder {
    type Reply = ();

    async fn handle(&mut self, Record(value): Record, _: &mut Context<Self, Self::Reply>) {
        self.0.push(value);
    }
}

async fn fill_bounded_mailbox(actor: &ActorRef<Recorder>) -> Vec<u32> {
    // Hotpath 0.11 adds a one-slot proxy and an in-flight forwarding slot.
    // Fill both as well as the one-slot mailbox before testing backpressure.
    let backlog: Vec<u32> = (0..if cfg!(feature = "hotpath") { 3 } else { 1 }).collect();
    for &value in &backlog {
        actor.tell(Record(value)).await.unwrap();
    }
    backlog
}

#[tokio::test]
async fn graceful_stop_rejects_new_messages_and_processes_the_backlog() {
    for mailbox in [mailbox::bounded::<Recorder>(8), mailbox::unbounded()] {
        let prepared = PreparedActor::new(mailbox);
        let actor = prepared.actor_ref().clone();
        let recipient = actor.clone().recipient();
        let weak = actor.downgrade();

        actor.tell(Record(0)).await.unwrap();
        actor.tell(Record(1)).await.unwrap();
        recipient.stop_gracefully().await.unwrap();

        assert!(!actor.is_alive());
        assert!(!recipient.is_alive());
        assert_eq!(
            actor.tell(Record(2)).await,
            Err(SendError::ActorNotRunning(Record(2)))
        );
        assert_eq!(
            actor.tell(Record(3)).try_send(),
            Err(SendError::ActorNotRunning(Record(3)))
        );
        assert_eq!(
            actor.tell(Record(4)).blocking_send(),
            Err(SendError::ActorNotRunning(Record(4)))
        );
        assert_eq!(
            actor
                .tell(Record(5))
                .mailbox_timeout(Duration::from_secs(1))
                .await,
            Err(SendError::ActorNotRunning(Record(5)))
        );
        assert_eq!(
            actor.ask(Record(6)).send().now_or_never(),
            Some(Err(SendError::ActorNotRunning(Record(6))))
        );
        assert_eq!(
            recipient.tell(Record(7)).await,
            Err(SendError::ActorNotRunning(Record(7)))
        );
        assert_eq!(
            weak.upgrade().unwrap().tell(Record(8)).await,
            Err(SendError::ActorNotRunning(Record(8)))
        );

        let (state, _) = prepared.spawn(Recorder(Vec::new())).await.unwrap().unwrap();
        assert_eq!(state.0, [0, 1]);
        actor.wait_for_shutdown().await;
    }
}

#[tokio::test]
async fn enqueue_after_stop_returns_an_error_without_panicking() {
    let prepared = PreparedActor::new(mailbox::unbounded::<Recorder>());
    let actor = prepared.actor_ref();
    actor.stop_gracefully().await.unwrap();
    assert!(matches!(
        actor.ask(Record(1)).enqueue().now_or_never(),
        Some(Err(SendError::ActorNotRunning(())))
    ));
    assert!(matches!(
        actor.ask(Record(2)).try_enqueue(),
        Err(SendError::ActorNotRunning(()))
    ));
    assert!(matches!(
        actor.ask(Record(3)).blocking_enqueue(),
        Err(SendError::ActorNotRunning(()))
    ));
}

#[tokio::test]
async fn cancelling_stop_on_a_full_mailbox_keeps_the_actor_usable() {
    let prepared = PreparedActor::new(mailbox::bounded::<Recorder>(1));
    let actor = prepared.actor_ref().clone();
    let mut expected = fill_bounded_mailbox(&actor).await;
    let next = expected.len() as u32;

    assert!(actor.stop_gracefully().now_or_never().is_none());
    assert!(
        actor.is_alive(),
        "a cancelled stop must not close acceptance"
    );
    assert_eq!(
        actor.tell(Record(next)).try_send(),
        Err(SendError::MailboxFull(Record(next)))
    );

    let task = prepared.spawn(Recorder(Vec::new()));
    tokio::time::timeout(Duration::from_secs(2), async {
        actor.tell(Record(next)).await.unwrap();
        actor.stop_gracefully().await.unwrap();
        actor.wait_for_shutdown().await;
        expected.push(next);
        assert_eq!(task.await.unwrap().unwrap().0.0, expected);
    })
    .await
    .expect("the actor must accept messages and a later stop after cancellation");
}

#[tokio::test]
async fn cancelling_one_stop_preserves_another_stop_and_earlier_pending_sends() {
    let prepared = PreparedActor::new(mailbox::bounded::<Recorder>(1));
    let actor = prepared.actor_ref().clone();
    let recipient = actor.clone().recipient();
    let weak = actor.downgrade();
    let mut expected = fill_bounded_mailbox(&actor).await;
    let next = expected.len() as u32;

    let mut pending_send = Box::pin(actor.tell(Record(next)).send());
    assert!(pending_send.as_mut().now_or_never().is_none());
    let mut cancelled_stop = Box::pin(actor.stop_gracefully());
    assert!(cancelled_stop.as_mut().now_or_never().is_none());
    let mut stop = Box::pin(recipient.stop_gracefully());
    assert!(stop.as_mut().now_or_never().is_none());
    drop(cancelled_stop);
    assert!(actor.is_alive());

    let task = prepared.spawn(Recorder(Vec::new()));
    tokio::time::timeout(Duration::from_secs(2), async {
        let (sent, stopped) = tokio::join!(pending_send, stop);
        sent.unwrap();
        stopped.unwrap();
        weak.wait_for_shutdown_result().await.unwrap();
        actor.wait_for_shutdown_result().await.unwrap();
        expected.push(next);
        assert_eq!(task.await.unwrap().unwrap().0.0, expected);
        assert_eq!(
            actor.tell(Record(next + 1)).await,
            Err(SendError::ActorNotRunning(Record(next + 1)))
        );
    })
    .await
    .expect("cancelling one caller must not cancel another caller's stop");
}

#[tokio::test]
async fn concurrent_stop_callers_complete_when_the_mailbox_closes() {
    let prepared = PreparedActor::new(mailbox::bounded::<Recorder>(1));
    let actor = prepared.actor_ref().clone();
    let expected = fill_bounded_mailbox(&actor).await;
    let mut first = Box::pin(actor.stop_gracefully());
    let mut second = Box::pin(actor.stop_gracefully());
    assert!(first.as_mut().now_or_never().is_none());
    assert!(second.as_mut().now_or_never().is_none());

    let task = prepared.spawn(Recorder(Vec::new()));
    tokio::time::timeout(Duration::from_secs(2), async {
        let (first, second) = tokio::join!(first, second);
        first.unwrap();
        assert!(matches!(
            second,
            Ok(()) | Err(SendError::ActorNotRunning(()))
        ));
        actor.wait_for_shutdown_result().await.unwrap();
        assert_eq!(task.await.unwrap().unwrap().0.0, expected);
    })
    .await
    .expect("concurrent stop callers must not strand either caller");
}

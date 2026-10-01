use piying::{
    actor::{Actor, ActorRef, Spawn},
    error::{Infallible, SendError},
    message::{Context, Message},
};
use piying_actors::pool::{ActorPool, Dispatch};

#[derive(Debug, PartialEq, Eq)]
struct Job(u32);

struct Worker;

impl Actor for Worker {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<Job> for Worker {
    type Reply = Result<u32, &'static str>;

    async fn handle(&mut self, Job(value): Job, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if value == 0 {
            Err("invalid job")
        } else {
            Ok(value)
        }
    }
}

#[tokio::test]
async fn dispatch_preserves_worker_results_and_errors() {
    let worker = Worker::spawn(Worker);
    let pool = ActorPool::spawn(ActorPool::new(1, {
        let worker = worker.clone();
        move || worker.clone()
    }));

    assert_eq!(pool.ask(Dispatch(Job(42))).await, Ok(42));
    assert!(matches!(
        pool.ask(Dispatch(Job(0))).await,
        Err(SendError::HandlerError(SendError::HandlerError(
            "invalid job"
        )))
    ));

    worker.kill();
    worker.wait_for_shutdown().await;
    assert!(matches!(
        pool.ask(Dispatch(Job(42))).await,
        Err(SendError::HandlerError(SendError::ActorNotRunning(Job(42))))
    ));
    pool.kill();
    pool.wait_for_shutdown().await;
}

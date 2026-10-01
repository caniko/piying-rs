use std::time::Duration;

use piying::{
    actor::{Actor, ActorRef, Spawn, WeakActorRef},
    error::{ActorStopReason, HookError, Infallible, PanicReason},
};
use tokio::sync::oneshot;

struct ShutdownActor {
    gate: Option<oneshot::Receiver<()>>,
    panic: bool,
}

impl Actor for ShutdownActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        if let Some(gate) = self.gate.take() {
            gate.await.unwrap();
        }
        assert!(!self.panic, "shutdown panic");
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_panic_is_reported_to_strong_and_weak_handles() {
    let actor = ShutdownActor::spawn(ShutdownActor {
        gate: None,
        panic: true,
    });
    actor.wait_for_startup().await;
    let weak = actor.downgrade();
    actor.stop_gracefully().await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        for result in [
            actor.wait_for_shutdown_result().await,
            weak.wait_for_shutdown_result().await,
        ] {
            let Err(HookError::Panicked(err)) = result else {
                panic!("expected a captured shutdown panic");
            };
            assert_eq!(err.reason(), PanicReason::OnStop);
            assert_eq!(
                err.with_str(str::to_owned).as_deref(),
                Some("shutdown panic")
            );
        }
        actor
            .wait_for_shutdown_with_result(|result| {
                let Err(HookError::Panicked(err)) = result else {
                    panic!("expected a captured shutdown panic");
                };
                assert!(err.with_str(|s| s == "shutdown panic").unwrap());
            })
            .await;
        weak.wait_for_shutdown().await;
    })
    .await
    .expect("shutdown results must be published even when on_stop panics");
}

#[tokio::test]
async fn wait_for_shutdown_waits_for_on_stop() {
    let (tx, rx) = oneshot::channel();
    let actor = ShutdownActor::spawn(ShutdownActor {
        gate: Some(rx),
        panic: false,
    });
    actor.wait_for_startup().await;
    actor.stop_gracefully().await.unwrap();
    actor.mailbox_sender().closed().await;

    assert!(
        tokio::time::timeout(Duration::from_millis(20), actor.wait_for_shutdown())
            .await
            .is_err(),
        "mailbox closure does not mean on_stop has completed"
    );
    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), actor.wait_for_shutdown())
        .await
        .unwrap();
}

struct ConstructionPanic(bool);

impl Actor for ConstructionPanic {
    type Args = bool;
    type Error = Infallible;

    fn on_start(
        panic: bool,
        _: ActorRef<Self>,
    ) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send {
        assert!(!panic, "startup construction panic");
        std::future::ready(Ok(Self(true)))
    }

    fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        assert!(!self.0, "shutdown construction panic");
        std::future::ready(Ok(()))
    }
}

#[tokio::test]
async fn hook_construction_panics_publish_lifecycle_results() {
    tokio::time::timeout(Duration::from_secs(2), async {
        for panic_on_start in [true, false] {
            let actor = ConstructionPanic::spawn(panic_on_start);
            let weak = actor.downgrade();
            let (reason, text) = if panic_on_start {
                for result in [
                    actor.wait_for_startup_result().await,
                    weak.wait_for_startup_result().await,
                ] {
                    let Err(HookError::Panicked(err)) = result else {
                        panic!("expected a captured startup construction panic");
                    };
                    assert_eq!(err.reason(), PanicReason::OnStart);
                }
                (PanicReason::OnStart, "startup construction panic")
            } else {
                actor.wait_for_startup_result().await.unwrap();
                actor.stop_gracefully().await.unwrap();
                (PanicReason::OnStop, "shutdown construction panic")
            };
            for result in [
                actor.wait_for_shutdown_result().await,
                weak.wait_for_shutdown_result().await,
            ] {
                let Err(HookError::Panicked(err)) = result else {
                    panic!("expected a captured hook construction panic");
                };
                assert_eq!(err.reason(), reason);
                assert_eq!(err.with_str(str::to_owned).as_deref(), Some(text));
            }
        }
    })
    .await
    .expect("hook construction panics must not strand lifecycle waiters");
}

struct StartupPollingPanic;

impl Actor for StartupPollingPanic {
    type Args = ();
    type Error = Infallible;

    async fn on_start(_: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        tokio::task::yield_now().await;
        panic!("startup polling panic");
    }
}

#[tokio::test]
async fn startup_polling_panics_publish_startup_and_shutdown_results() {
    let actor = StartupPollingPanic::spawn(());
    let weak = actor.downgrade();
    tokio::time::timeout(Duration::from_secs(2), async {
        for result in [
            actor.wait_for_startup_result().await,
            weak.wait_for_startup_result().await,
            actor.wait_for_shutdown_result().await,
            weak.wait_for_shutdown_result().await,
        ] {
            let Err(HookError::Panicked(err)) = result else {
                panic!("expected a captured startup polling panic");
            };
            assert_eq!(err.reason(), PanicReason::OnStart);
            assert_eq!(
                err.with_str(str::to_owned).as_deref(),
                Some("startup polling panic")
            );
        }
        actor.wait_for_shutdown().await;
        weak.wait_for_shutdown().await;
        assert!(!actor.is_alive());
    })
    .await
    .expect("startup polling panics must not strand startup or shutdown waiters");
}

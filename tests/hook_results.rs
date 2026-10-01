use std::{fmt::Debug, marker::PhantomData, time::Duration};

use piying::{
    actor::{Actor, ActorRef, Spawn, WeakActorRef},
    error::{ActorStopReason, HookError},
};

struct ErrorActor<E>(PhantomData<fn() -> E>);

impl<E: Debug + Default + Send + 'static> Actor for ErrorActor<E> {
    type Args = bool;
    type Error = E;

    async fn on_start(fail: bool, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        if fail {
            Err(E::default())
        } else {
            Ok(Self(PhantomData))
        }
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Err(E::default())
    }
}

#[derive(Debug, Default)]
struct NonCloneError;

#[tokio::test]
async fn borrowed_hook_results_accept_non_clone_errors_and_once_callbacks() {
    tokio::time::timeout(Duration::from_secs(2), async {
        for fail_start in [false, true] {
            let actor = ErrorActor::<NonCloneError>::spawn(fail_start);
            let weak = actor.downgrade();
            let token = Box::new(());
            let token = actor
                .wait_for_startup_with_result(move |result| {
                    assert_eq!(matches!(result, Err(HookError::Error(_))), fail_start);
                    token
                })
                .await;
            let token = weak
                .wait_for_startup_with_result(move |result| {
                    assert_eq!(matches!(result, Err(HookError::Error(_))), fail_start);
                    token
                })
                .await;

            if !fail_start {
                actor.stop_gracefully().await.unwrap();
                let token = actor
                    .wait_for_shutdown_with_result(move |result| {
                        assert!(matches!(result, Err(HookError::Error(_))));
                        token
                    })
                    .await;
                weak.wait_for_shutdown_with_result(move |result| {
                    assert!(matches!(result, Err(HookError::Error(_))));
                    token
                })
                .await;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cloned_hook_results_preserve_typed_errors_for_strong_and_weak_handles() {
    tokio::time::timeout(Duration::from_secs(2), async {
        for fail_start in [false, true] {
            let actor = ErrorActor::<String>::spawn(fail_start);
            let weak = actor.downgrade();
            for result in [
                actor.wait_for_startup_result().await,
                weak.wait_for_startup_result().await,
            ] {
                if fail_start {
                    assert!(matches!(result, Err(HookError::Error(err)) if err.is_empty()));
                } else {
                    assert!(result.is_ok());
                }
            }
            if !fail_start {
                actor.stop_gracefully().await.unwrap();
                for result in [
                    actor.wait_for_shutdown_result().await,
                    weak.wait_for_shutdown_result().await,
                ] {
                    assert!(matches!(result, Err(HookError::Error(err)) if err.is_empty()));
                }
            }
        }
    })
    .await
    .unwrap();
}

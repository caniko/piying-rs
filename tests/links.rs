use std::time::Duration;

use piying::{
    actor::{Actor, ActorRef, Spawn},
    error::Infallible,
    message::{Context, Message},
};

struct Probe;

impl Actor for Probe {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
}

impl Message<()> for Probe {
    type Reply = bool;

    async fn handle(&mut self, _: (), _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        true
    }
}

#[tokio::test]
async fn unlink_variants_remove_both_links_in_either_id_order() {
    tokio::time::timeout(Duration::from_secs(2), async {
        for reverse in [false, true] {
            for blocking in [false, true] {
                for weak in [false, true] {
                    let first = Probe::spawn(Probe);
                    let second = Probe::spawn(Probe);
                    let (actor, sibling) = if reverse {
                        (second, first)
                    } else {
                        (first, second)
                    };
                    actor.wait_for_startup().await;
                    sibling.wait_for_startup().await;

                    if blocking {
                        let actor = actor.clone();
                        let sibling = sibling.clone();
                        tokio::task::spawn_blocking(move || {
                            actor.blocking_link(&actor);
                            actor.blocking_link(&sibling);
                            if weak {
                                actor.downgrade().blocking_unlink(&actor);
                                actor.downgrade().blocking_unlink(&sibling);
                            } else {
                                actor.blocking_unlink(&actor);
                                actor.blocking_unlink(&sibling);
                            }
                        })
                        .await
                        .unwrap();
                    } else {
                        actor.link(&actor).await;
                        tokio::join!(actor.link(&sibling), sibling.link(&actor));
                        if weak {
                            actor.downgrade().unlink(&actor).await;
                            actor.downgrade().unlink(&sibling).await;
                        } else {
                            actor.unlink(&actor).await;
                            actor.unlink(&sibling).await;
                        }
                    }

                    // Killing either side must leave the other actor able to handle messages.
                    let (stopped, survivor) = if weak {
                        (actor, sibling)
                    } else {
                        (sibling, actor)
                    };
                    stopped.kill();
                    stopped.wait_for_shutdown().await;
                    assert!(survivor.ask(()).await.unwrap());
                    survivor.stop_gracefully().await.unwrap();
                    survivor.wait_for_shutdown().await;
                }
            }
        }
    })
    .await
    .expect("link operations must not deadlock");
}

#[tokio::test]
async fn killed_actor_notifies_and_stops_its_linked_sibling() {
    let actor = Probe::spawn(Probe);
    let sibling = Probe::spawn(Probe);
    actor.wait_for_startup().await;
    sibling.wait_for_startup().await;
    actor.link(&sibling).await;
    actor.kill();
    tokio::time::timeout(Duration::from_secs(2), sibling.wait_for_shutdown())
        .await
        .unwrap();
    actor.wait_for_shutdown().await;
}

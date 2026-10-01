use super::*;
use crate::{actor::PreparedActor, error::Infallible, message::Message};
use metrics::{Counter, atomics::AtomicU64};

struct TestActor;

impl Actor for TestActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
}

impl Message<()> for TestActor {
    type Reply = ();

    async fn handle(&mut self, _: (), _: &mut crate::message::Context<Self, Self::Reply>) {}
}

#[derive(Clone, Copy)]
enum Receive {
    Async,
    AsyncBatch,
    Try,
    Blocking,
    BlockingBatch,
    Poll,
    PollBatch,
}

fn receive_one(
    mode: Receive,
    rx: &mut MailboxReceiver<TestActor>,
    buffer: &mut Vec<Signal<TestActor>>,
) {
    match mode {
        Receive::Async => buffer.push(futures::executor::block_on(rx.recv()).unwrap()),
        Receive::AsyncBatch => {
            assert_eq!(futures::executor::block_on(rx.recv_many(buffer, 1)), 1);
        }
        Receive::Try => {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                match rx.try_recv() {
                    Ok(signal) => {
                        buffer.push(signal);
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        // Hotpath can forward through a proxy; wait for that delivery.
                        assert!(std::time::Instant::now() < deadline);
                        std::thread::yield_now();
                    }
                    Err(err) => panic!("unexpected receive error: {err}"),
                }
            }
        }
        Receive::Blocking => buffer.push(rx.blocking_recv().unwrap()),
        Receive::BlockingBatch => assert_eq!(rx.blocking_recv_many(buffer, 1), 1),
        Receive::Poll => buffer.push(
            futures::executor::block_on(futures::future::poll_fn(|cx| rx.poll_recv(cx))).unwrap(),
        ),
        Receive::PollBatch => assert_eq!(
            futures::executor::block_on(futures::future::poll_fn(|cx| {
                rx.poll_recv_many(cx, buffer, 1)
            })),
            1
        ),
    }
}

#[test]
fn receive_metrics_count_only_new_channel_signals_across_all_receive_variants() {
    for (tx, mut rx) in [bounded::<TestActor>(8), unbounded()] {
        let counts = std::array::from_fn::<_, 3, _>(|_| Arc::new(AtomicU64::new(0)));
        rx.messages_received = Counter::from_arc(counts[0].clone());
        rx.lifecycle_signals_received = Counter::from_arc(counts[1].clone());
        rx.link_died_signals_received = Counter::from_arc(counts[2].clone());
        let prepared = PreparedActor::<TestActor>::new(unbounded());

        for (index, mode) in [
            Receive::Async,
            Receive::AsyncBatch,
            Receive::Try,
            Receive::Blocking,
            Receive::BlockingBatch,
            Receive::Poll,
            Receive::PollBatch,
        ]
        .into_iter()
        .enumerate()
        {
            for signal in [
                Signal::message(prepared.actor_ref(), (), None),
                Signal::Stop,
                Signal::LinkDied {
                    id: prepared.actor_ref().id(),
                    reason: ActorStopReason::Normal,
                },
            ] {
                tx.try_send(signal).unwrap();
            }
            // A pre-existing buffer entry must never be included in batch metrics.
            let mut buffer = vec![Signal::StartupFinished];
            for _ in 0..3 {
                receive_one(mode, &mut rx, &mut buffer);
            }
            assert_eq!(
                counts.each_ref().map(|c| c.load(Ordering::Relaxed)),
                [index as u64 + 1; 3]
            );

            // Replayed front-buffer entries have already been counted on receipt.
            rx.push_front(buffer.drain(1..).collect());
            for _ in 0..3 {
                receive_one(mode, &mut rx, &mut buffer);
            }
            assert_eq!(
                counts.each_ref().map(|c| c.load(Ordering::Relaxed)),
                [index as u64 + 1; 3]
            );
        }
        let before = counts.each_ref().map(|c| c.load(Ordering::Relaxed));
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(rx.poll_recv(&mut cx).is_pending());
        assert!(rx.poll_recv_many(&mut cx, &mut Vec::new(), 1).is_pending());
        assert_eq!(rx.blocking_recv_many(&mut Vec::new(), 0), 0);
        drop(tx);
        assert!(rx.blocking_recv().is_none());
        assert_eq!(counts.each_ref().map(|c| c.load(Ordering::Relaxed)), before);
    }
}

#[tokio::test]
async fn cloned_strong_and_weak_handles_share_stop_acceptance_and_metrics() {
    for (mut tx, mut rx) in [bounded::<TestActor>(1), unbounded()] {
        let count = Arc::new(AtomicU64::new(0));
        tx.state.lifecycle_signals_sent = Counter::from_arc(count.clone());
        let strong = tx.clone();
        let weak = tx.downgrade().clone();
        strong.send(Signal::StartupFinished).await.unwrap();
        rx.recv().await.unwrap();
        if let MailboxSenderInner::Bounded(inner) = &strong.inner {
            // Hold the capacity directly: Hotpath's proxy can otherwise free it
            // between sending a signal and polling the stop future.
            let permit = inner.reserve().await.unwrap();
            assert!(weak.signal_stop().now_or_never().is_none());
            assert!(strong.is_accepting());
            assert_eq!(count.load(Ordering::Relaxed), 1);
            drop(permit);
        }
        weak.signal_stop().await.unwrap();
        assert!(!tx.is_accepting());
        assert!(!strong.is_accepting());
        assert!(!weak.upgrade().unwrap().is_accepting());
        assert_eq!(count.load(Ordering::Relaxed), 2);
        assert!(matches!(rx.recv().await, Some(Signal::Stop)));
        drop(rx);
        // With Hotpath, the proxy observes receiver closure asynchronously.
        tokio::time::timeout(Duration::from_secs(2), tx.closed())
            .await
            .expect("sender must observe receiver closure");
        assert!(tx.signal_stop().await.is_err());
        assert_eq!(count.load(Ordering::Relaxed), 2);
    }
}

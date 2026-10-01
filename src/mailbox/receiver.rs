/// Receives values from the associated `MailboxSender`.
///
/// Instances are created by the [`bounded`] and [`unbounded`] functions.
pub struct MailboxReceiver<A: Actor> {
    inner: MailboxReceiverInner<A>,
    front: VecDeque<Signal<A>>,
    #[cfg(feature = "metrics")]
    messages_received: metrics::Counter,
    #[cfg(feature = "metrics")]
    lifecycle_signals_received: metrics::Counter,
    #[cfg(feature = "metrics")]
    link_died_signals_received: metrics::Counter,
}

enum MailboxReceiverInner<A: Actor> {
    /// Bounded mailbox receiver.
    Bounded(mpsc::Receiver<Signal<A>>),
    /// Unbounded mailbox receiver.
    Unbounded(mpsc::UnboundedReceiver<Signal<A>>),
}

impl<A: Actor> MailboxReceiver<A> {
    fn new(inner: MailboxReceiverInner<A>) -> Self {
        Self {
            inner,
            front: VecDeque::new(),
            #[cfg(feature = "metrics")]
            messages_received: metrics::counter!("piying_messages_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            lifecycle_signals_received: metrics::counter!("piying_lifecycle_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            link_died_signals_received: metrics::counter!("piying_link_died_received", "actor_name" => A::name()),
        }
    }

    #[cfg(feature = "metrics")]
    fn record_received(&self, signal: &Signal<A>) {
        match signal {
            Signal::Message { .. } => self.messages_received.increment(1),
            Signal::StartupFinished | Signal::Stop => self.lifecycle_signals_received.increment(1),
            Signal::LinkDied { .. } => self.link_died_signals_received.increment(1),
        }
    }

    #[cfg(feature = "metrics")]
    fn record_received_batch(&self, buffer: &[Signal<A>], count: usize) {
        for signal in &buffer[buffer.len() - count..] {
            self.record_received(signal);
        }
    }

    /// Re-inserts previously received signals ahead of the channel in their original
    /// order. These signals have already been counted by the receive metrics.
    #[cfg(test)]
    pub(crate) fn push_front(&mut self, mut signals: VecDeque<Signal<A>>) {
        signals.append(&mut self.front);
        self.front = signals;
    }

    /// Moves up to `limit` already-buffered front signals into `buffer`, returning the count.
    fn drain_front_into(&mut self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize {
        let count = self.front.len().min(limit);
        buffer.extend(self.front.drain(..count));
        count
    }

    /// Receives the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::recv`] and [`mpsc::UnboundedReceiver::recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::recv`]: tokio::sync::mpsc::Receiver::recv
    /// [`mpsc::UnboundedReceiver::recv`]: tokio::sync::mpsc::UnboundedReceiver::recv
    pub async fn recv(&mut self) -> Option<Signal<A>> {
        if let Some(signal) = self.front.pop_front() {
            return Some(signal);
        }

        let signal = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.recv().await,
            MailboxReceiverInner::Unbounded(rx) => rx.recv().await,
        };

        #[cfg(feature = "metrics")]
        if let Some(signal) = &signal {
            self.record_received(signal);
        }

        signal
    }

    /// Receives the next values for this receiver and extends `buffer`.
    ///
    /// See tokio's [`mpsc::Receiver::recv_many`] and [`mpsc::UnboundedReceiver::recv_many`] docs for more info.
    ///
    /// [`mpsc::Receiver::recv_many`]: tokio::sync::mpsc::Receiver::recv_many
    /// [`mpsc::UnboundedReceiver::recv_many`]: tokio::sync::mpsc::UnboundedReceiver::recv_many
    pub async fn recv_many(&mut self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize {
        if !self.front.is_empty() {
            return self.drain_front_into(buffer, limit);
        }
        let count = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.recv_many(buffer, limit).await,
            MailboxReceiverInner::Unbounded(rx) => rx.recv_many(buffer, limit).await,
        };

        #[cfg(feature = "metrics")]
        self.record_received_batch(buffer, count);

        count
    }

    /// Tries to receive the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::try_recv`] and [`mpsc::UnboundedReceiver::try_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::try_recv`]: tokio::sync::mpsc::Receiver::try_recv
    /// [`mpsc::UnboundedReceiver::try_recv`]: tokio::sync::mpsc::UnboundedReceiver::try_recv
    pub fn try_recv(&mut self) -> Result<Signal<A>, TryRecvError> {
        if let Some(signal) = self.front.pop_front() {
            return Ok(signal);
        }

        let res = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.try_recv(),
            MailboxReceiverInner::Unbounded(rx) => rx.try_recv(),
        };

        #[cfg(feature = "metrics")]
        if let Ok(signal) = &res {
            self.record_received(signal);
        }

        res
    }

    /// Blocking receive to call outside of asynchronous contexts.
    ///
    /// See tokio's [`mpsc::Receiver::blocking_recv`] and [`mpsc::UnboundedReceiver::blocking_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::blocking_recv`]: tokio::sync::mpsc::Receiver::blocking_recv
    /// [`mpsc::UnboundedReceiver::blocking_recv`]: tokio::sync::mpsc::UnboundedReceiver::blocking_recv
    pub fn blocking_recv(&mut self) -> Option<Signal<A>> {
        if let Some(signal) = self.front.pop_front() {
            return Some(signal);
        }

        let signal = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.blocking_recv(),
            MailboxReceiverInner::Unbounded(rx) => rx.blocking_recv(),
        };

        #[cfg(feature = "metrics")]
        if let Some(signal) = &signal {
            self.record_received(signal);
        }

        signal
    }

    /// Variant of [`Self::recv_many`] for blocking contexts.
    ///
    /// See tokio's [`mpsc::Receiver::blocking_recv_many`] and [`mpsc::UnboundedReceiver::blocking_recv_many`] docs for more info.
    ///
    /// [`mpsc::Receiver::blocking_recv_many`]: tokio::sync::mpsc::Receiver::blocking_recv_many
    /// [`mpsc::UnboundedReceiver::blocking_recv_many`]: tokio::sync::mpsc::UnboundedReceiver::blocking_recv_many
    pub fn blocking_recv_many(&mut self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize {
        if !self.front.is_empty() {
            return self.drain_front_into(buffer, limit);
        }

        let count = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.blocking_recv_many(buffer, limit),
            MailboxReceiverInner::Unbounded(rx) => rx.blocking_recv_many(buffer, limit),
        };

        #[cfg(feature = "metrics")]
        self.record_received_batch(buffer, count);

        count
    }

    /// Closes the receiving half of a channel, without dropping it.
    ///
    /// See tokio's [`mpsc::Receiver::close`] and [`mpsc::UnboundedReceiver::close`] docs for more info.
    ///
    /// [`mpsc::Receiver::close`]: tokio::sync::mpsc::Receiver::close
    /// [`mpsc::UnboundedReceiver::close`]: tokio::sync::mpsc::UnboundedReceiver::close
    pub fn close(&mut self) {
        match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.close(),
            MailboxReceiverInner::Unbounded(rx) => rx.close(),
        }
    }

    /// Checks if a channel is closed.
    ///
    /// See tokio's [`mpsc::Receiver::is_closed`] and [`mpsc::UnboundedReceiver::is_closed`] docs for more info.
    ///
    /// [`mpsc::Receiver::is_closed`]: tokio::sync::mpsc::Receiver::is_closed
    /// [`mpsc::UnboundedReceiver::is_closed`]: tokio::sync::mpsc::UnboundedReceiver::is_closed
    pub fn is_closed(&self) -> bool {
        match &self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.is_closed(),
            MailboxReceiverInner::Unbounded(rx) => rx.is_closed(),
        }
    }

    /// Checks if a channel is empty.
    ///
    /// See tokio's [`mpsc::Receiver::is_empty`] and [`mpsc::UnboundedReceiver::is_empty`] docs for more info.
    ///
    /// [`mpsc::Receiver::is_empty`]: tokio::sync::mpsc::Receiver::is_empty
    /// [`mpsc::UnboundedReceiver::is_empty`]: tokio::sync::mpsc::UnboundedReceiver::is_empty
    pub fn is_empty(&self) -> bool {
        if !self.front.is_empty() {
            return false;
        }

        match &self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.is_empty(),
            MailboxReceiverInner::Unbounded(rx) => rx.is_empty(),
        }
    }

    /// Returns the number of messages in the channel.
    ///
    /// See tokio's [`mpsc::Receiver::len`] and [`mpsc::UnboundedReceiver::len`] docs for more info.
    ///
    /// [`mpsc::Receiver::len`]: tokio::sync::mpsc::Receiver::len
    /// [`mpsc::UnboundedReceiver::len`]: tokio::sync::mpsc::UnboundedReceiver::len
    pub fn len(&self) -> usize {
        let inner = match &self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.len(),
            MailboxReceiverInner::Unbounded(rx) => rx.len(),
        };
        self.front.len() + inner
    }

    /// Polls to receive the next message on this channel.
    ///
    /// See tokio's [`mpsc::Receiver::poll_recv`] and [`mpsc::UnboundedReceiver::poll_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::poll_recv`]: tokio::sync::mpsc::Receiver::poll_recv
    /// [`mpsc::UnboundedReceiver::poll_recv`]: tokio::sync::mpsc::UnboundedReceiver::poll_recv
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Signal<A>>> {
        if let Some(signal) = self.front.pop_front() {
            return Poll::Ready(Some(signal));
        }

        let poll = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.poll_recv(cx),
            MailboxReceiverInner::Unbounded(rx) => rx.poll_recv(cx),
        };

        #[cfg(feature = "metrics")]
        if let Poll::Ready(Some(signal)) = &poll {
            self.record_received(signal);
        }

        poll
    }

    /// Polls to receive multiple messages on this channel, extending the provided buffer.
    ///
    /// See tokio's [`mpsc::Receiver::poll_recv_many`] and [`mpsc::UnboundedReceiver::poll_recv_many`] docs for more info.
    ///
    /// [`mpsc::Receiver::poll_recv_many`]: tokio::sync::mpsc::Receiver::poll_recv_many
    /// [`mpsc::UnboundedReceiver::poll_recv_many`]: tokio::sync::mpsc::UnboundedReceiver::poll_recv_many
    pub fn poll_recv_many(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut Vec<Signal<A>>,
        limit: usize,
    ) -> Poll<usize> {
        if !self.front.is_empty() {
            return Poll::Ready(self.drain_front_into(buffer, limit));
        }

        let poll = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.poll_recv_many(cx, buffer, limit),
            MailboxReceiverInner::Unbounded(rx) => rx.poll_recv_many(cx, buffer, limit),
        };

        #[cfg(feature = "metrics")]
        if let Poll::Ready(count) = poll {
            self.record_received_batch(buffer, count);
        }

        poll
    }

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Receiver::sender_strong_count`] and [`mpsc::UnboundedReceiver::sender_strong_count`] docs for more info.
    ///
    /// [`mpsc::Receiver::sender_strong_count`]: tokio::sync::mpsc::Receiver::sender_strong_count
    /// [`mpsc::UnboundedReceiver::sender_strong_count`]: tokio::sync::mpsc::UnboundedReceiver::sender_strong_count
    pub fn sender_strong_count(&self) -> usize {
        match &self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.sender_strong_count(),
            MailboxReceiverInner::Unbounded(rx) => rx.sender_strong_count(),
        }
    }

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Receiver::sender_weak_count`] and [`mpsc::UnboundedReceiver::sender_weak_count`] docs for more info.
    ///
    /// [`mpsc::Receiver::sender_weak_count`]: tokio::sync::mpsc::Receiver::sender_weak_count
    /// [`mpsc::UnboundedReceiver::sender_weak_count`]: tokio::sync::mpsc::UnboundedReceiver::sender_weak_count
    pub fn sender_weak_count(&self) -> usize {
        match &self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.sender_weak_count(),
            MailboxReceiverInner::Unbounded(rx) => rx.sender_weak_count(),
        }
    }
}

impl<A: Actor> fmt::Debug for MailboxReceiver<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            MailboxReceiverInner::Bounded(tx) => f.debug_tuple("Bounded").field(tx).finish(),
            MailboxReceiverInner::Unbounded(tx) => f.debug_tuple("Unbounded").field(tx).finish(),
        }
    }
}

#[cfg(test)]
mod receiver_tests {
    use super::*;
    use crate::error::Infallible;

    struct TestActor;

    impl Actor for TestActor {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(args)
        }
    }

    #[tokio::test]
    async fn batch_receive_preserves_front_order_and_reports_length() {
        for (tx, mut rx) in [bounded::<TestActor>(8), unbounded()] {
            tx.send(Signal::Stop).await.unwrap();
            // Hotpath may proxy sends asynchronously. Buffer a received signal before
            // checking the front queue so this assertion does not depend on scheduling.
            let stop = rx.recv().await.unwrap();
            rx.push_front(VecDeque::from([stop]));
            rx.push_front(VecDeque::from([Signal::StartupFinished]));
            assert_eq!(rx.len(), 2);
            let mut buffer = Vec::new();
            assert_eq!(rx.recv_many(&mut buffer, 0).await, 0);
            assert_eq!(rx.recv_many(&mut buffer, 1).await, 1);
            assert!(matches!(buffer.pop(), Some(Signal::StartupFinished)));
            assert_eq!(rx.recv_many(&mut buffer, 1).await, 1);
            assert!(matches!(buffer.pop(), Some(Signal::Stop)));
            assert!(rx.is_empty());
        }
    }

    #[test]
    fn batch_receive_handles_empty_and_prefilled_buffers() {
        for (tx, mut rx) in [bounded::<TestActor>(8), unbounded()] {
            assert_eq!(rx.blocking_recv_many(&mut Vec::new(), 0), 0);
            let mut buffer = vec![Signal::StartupFinished];
            assert_eq!(rx.blocking_recv_many(&mut buffer, 0), 0);
            tx.try_send(Signal::Stop).unwrap();
            assert_eq!(rx.blocking_recv_many(&mut buffer, 1), 1);
            assert!(matches!(buffer[0], Signal::StartupFinished));
            assert!(matches!(buffer[1], Signal::Stop));

            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert_eq!(rx.poll_recv_many(&mut cx, &mut buffer, 0), Poll::Ready(0));
            tx.try_send(Signal::StartupFinished).unwrap();
            assert_eq!(
                futures::executor::block_on(futures::future::poll_fn(|cx| {
                    rx.poll_recv_many(cx, &mut buffer, 1)
                })),
                1
            );
            assert!(matches!(buffer[2], Signal::StartupFinished));
        }
    }
}

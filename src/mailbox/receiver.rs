/// Receives values from the associated `MailboxSender`.
///
/// Instances are created by the [`bounded`] and [`unbounded`] functions.
pub struct MailboxReceiver<A: Actor> {
    inner: MailboxReceiverInner<A>,
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
    /// Receives the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::recv`] and [`mpsc::UnboundedReceiver::recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::recv`]: tokio::sync::mpsc::Receiver::recv
    /// [`mpsc::UnboundedReceiver::recv`]: tokio::sync::mpsc::UnboundedReceiver::recv
    pub async fn recv(&mut self) -> Option<Signal<A>> {
        let signal = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.recv().await,
            MailboxReceiverInner::Unbounded(rx) => rx.recv().await,
        };

        #[cfg(feature = "metrics")]
        match &signal {
            Some(Signal::Message { .. }) => self.messages_received.increment(1),
            Some(Signal::StartupFinished | Signal::Stop) => {
                self.lifecycle_signals_received.increment(1)
            }
            Some(Signal::LinkDied { .. }) => self.link_died_signals_received.increment(1),
            None => {}
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
        let count = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.recv_many(buffer, limit).await,
            MailboxReceiverInner::Unbounded(rx) => rx.recv_many(buffer, limit).await,
        };

        #[cfg(feature = "metrics")]
        {
            let len = buffer.len();
            for signal in &buffer[len - 1 - count..len - 1] {
                match signal {
                    Signal::Message { .. } => self.messages_received.increment(1),
                    Signal::StartupFinished | Signal::Stop => {
                        self.lifecycle_signals_received.increment(1)
                    }
                    Signal::LinkDied { .. } => self.link_died_signals_received.increment(1),
                }
            }
        }

        count
    }

    /// Tries to receive the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::try_recv`] and [`mpsc::UnboundedReceiver::try_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::try_recv`]: tokio::sync::mpsc::Receiver::try_recv
    /// [`mpsc::UnboundedReceiver::try_recv`]: tokio::sync::mpsc::UnboundedReceiver::try_recv
    pub fn try_recv(&mut self) -> Result<Signal<A>, TryRecvError> {
        let res = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.try_recv(),
            MailboxReceiverInner::Unbounded(rx) => rx.try_recv(),
        };

        #[cfg(feature = "metrics")]
        match &res {
            Ok(Signal::Message { .. }) => self.messages_received.increment(1),
            Ok(Signal::StartupFinished | Signal::Stop) => {
                self.lifecycle_signals_received.increment(1)
            }
            Ok(Signal::LinkDied { .. }) => self.link_died_signals_received.increment(1),
            Err(_) => {}
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
        let signal = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.blocking_recv(),
            MailboxReceiverInner::Unbounded(rx) => rx.blocking_recv(),
        };

        #[cfg(feature = "metrics")]
        match &signal {
            Some(Signal::Message { .. }) => self.messages_received.increment(1),
            Some(Signal::StartupFinished | Signal::Stop) => {
                self.lifecycle_signals_received.increment(1)
            }
            Some(Signal::LinkDied { .. }) => self.link_died_signals_received.increment(1),
            None => {}
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
        let count = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.blocking_recv_many(buffer, limit),
            MailboxReceiverInner::Unbounded(rx) => rx.blocking_recv_many(buffer, limit),
        };

        #[cfg(feature = "metrics")]
        {
            let len = buffer.len();
            for signal in &buffer[len - 1 - count..len - 1] {
                match signal {
                    Signal::Message { .. } => self.messages_received.increment(1),
                    Signal::StartupFinished | Signal::Stop => {
                        self.lifecycle_signals_received.increment(1)
                    }
                    Signal::LinkDied { .. } => self.link_died_signals_received.increment(1),
                }
            }
        }

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
        match &self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.len(),
            MailboxReceiverInner::Unbounded(rx) => rx.len(),
        }
    }

    /// Polls to receive the next message on this channel.
    ///
    /// See tokio's [`mpsc::Receiver::poll_recv`] and [`mpsc::UnboundedReceiver::poll_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::poll_recv`]: tokio::sync::mpsc::Receiver::poll_recv
    /// [`mpsc::UnboundedReceiver::poll_recv`]: tokio::sync::mpsc::UnboundedReceiver::poll_recv
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Signal<A>>> {
        let poll = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.poll_recv(cx),
            MailboxReceiverInner::Unbounded(rx) => rx.poll_recv(cx),
        };

        #[cfg(feature = "metrics")]
        match &poll {
            Poll::Ready(Some(Signal::Message { .. })) => self.messages_received.increment(1),
            Poll::Ready(Some(Signal::StartupFinished | Signal::Stop)) => {
                self.lifecycle_signals_received.increment(1)
            }
            Poll::Ready(Some(Signal::LinkDied { .. })) => {
                self.link_died_signals_received.increment(1)
            }
            Poll::Ready(None) | Poll::Pending => {}
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
        let poll = match &mut self.inner {
            MailboxReceiverInner::Bounded(rx) => rx.poll_recv_many(cx, buffer, limit),
            MailboxReceiverInner::Unbounded(rx) => rx.poll_recv_many(cx, buffer, limit),
        };

        #[cfg(feature = "metrics")]
        {
            if let Poll::Ready(count) = poll {
                let len = buffer.len();
                for signal in &buffer[len - 1 - count..len - 1] {
                    match signal {
                        Signal::Message { .. } => self.messages_received.increment(1),
                        Signal::StartupFinished | Signal::Stop => {
                            self.lifecycle_signals_received.increment(1)
                        }
                        Signal::LinkDied { .. } => self.link_died_signals_received.increment(1),
                    }
                }
            }
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


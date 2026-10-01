/// A mailbox sender that does not prevent the channel from being closed.
///
/// See tokio's [`mpsc::WeakSender`] and [`mpsc::WeakUnboundedSender`] docs for more info.
///
/// [`mpsc::WeakSender`]: tokio::sync::mpsc::WeakSender
/// [`mpsc::WeakUnboundedSender`]: tokio::sync::mpsc::WeakUnboundedSender
pub struct WeakMailboxSender<A: Actor> {
    inner: WeakMailboxSenderInner<A>,
    state: MailboxSenderState,
}

enum WeakMailboxSenderInner<A: Actor> {
    /// Bounded weak mailbox sender.
    Bounded(mpsc::WeakSender<Signal<A>>),
    /// Unbounded weak mailbox sender.
    Unbounded(mpsc::WeakUnboundedSender<Signal<A>>),
}

impl<A: Actor> WeakMailboxSender<A> {
    /// Tries to convert a `WeakMailboxSender` into a [`MailboxSender`]. This will return `Some`
    /// if there are other `MailboxSender` instances alive and the channel wasn't
    /// previously dropped, otherwise `None` is returned.
    ///
    /// See tokio's [`mpsc::WeakSender::upgrade`] and [`mpsc::WeakUnboundedSender::upgrade`] docs for more info.
    ///
    /// [`mpsc::WeakSender::upgrade`]: tokio::sync::mpsc::WeakSender::upgrade
    /// [`mpsc::WeakUnboundedSender::upgrade`]: tokio::sync::mpsc::WeakUnboundedSender::upgrade
    pub fn upgrade(&self) -> Option<MailboxSender<A>> {
        let inner = match &self.inner {
            WeakMailboxSenderInner::Bounded(tx) => MailboxSenderInner::Bounded(tx.upgrade()?),
            WeakMailboxSenderInner::Unbounded(tx) => MailboxSenderInner::Unbounded(tx.upgrade()?),
        };
        Some(MailboxSender {
            inner,
            state: self.state.clone(),
        })
    }

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::WeakSender::strong_count`] and [`mpsc::WeakUnboundedSender::strong_count`] docs for more info.
    ///
    /// [`mpsc::WeakSender::strong_count`]: tokio::sync::mpsc::WeakSender::strong_count
    /// [`mpsc::WeakUnboundedSender::strong_count`]: tokio::sync::mpsc::WeakUnboundedSender::strong_count
    pub fn strong_count(&self) -> usize {
        match &self.inner {
            WeakMailboxSenderInner::Bounded(tx) => tx.strong_count(),
            WeakMailboxSenderInner::Unbounded(tx) => tx.strong_count(),
        }
    }

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::WeakSender::weak_count`] and [`mpsc::WeakUnboundedSender::weak_count`] docs for more info.
    ///
    /// [`mpsc::WeakSender::weak_count`]: tokio::sync::mpsc::WeakSender::weak_count
    /// [`mpsc::WeakUnboundedSender::weak_count`]: tokio::sync::mpsc::WeakUnboundedSender::weak_count
    pub fn weak_count(&self) -> usize {
        match &self.inner {
            WeakMailboxSenderInner::Bounded(tx) => tx.weak_count(),
            WeakMailboxSenderInner::Unbounded(tx) => tx.weak_count(),
        }
    }
}

impl<A: Actor> Clone for WeakMailboxSender<A> {
    fn clone(&self) -> Self {
        let inner = match &self.inner {
            WeakMailboxSenderInner::Bounded(tx) => WeakMailboxSenderInner::Bounded(tx.clone()),
            WeakMailboxSenderInner::Unbounded(tx) => WeakMailboxSenderInner::Unbounded(tx.clone()),
        };
        Self {
            inner,
            state: self.state.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for WeakMailboxSender<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            WeakMailboxSenderInner::Bounded(tx) => f.debug_tuple("Bounded").field(tx).finish(),
            WeakMailboxSenderInner::Unbounded(tx) => f.debug_tuple("Unbounded").field(tx).finish(),
        }
    }
}

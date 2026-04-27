/// A pending reply from a previously enqueued remote ask request.
///
/// This is returned by [`RemoteAskRequest::enqueue`] and [`RemoteAskRequest::try_enqueue`].
#[cfg(feature = "remote")]
#[allow(missing_debug_implementations)]
#[must_use = "reply wont be received without awaiting"]
pub struct RemotePendingReply<R>
where
    R: Reply,
{
    #[allow(clippy::type_complexity)]
    fut: BoxFuture<'static, Result<R::Ok, error::RemoteSendError<R::Error>>>,
}

#[cfg(feature = "remote")]
impl<R> Future for RemotePendingReply<R>
where
    R: Reply,
{
    type Output = Result<R::Ok, error::RemoteSendError<R::Error>>;

    fn poll(mut self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}


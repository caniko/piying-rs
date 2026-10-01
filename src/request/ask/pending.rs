/// A pending reply from a previously enqueued ask request.
///
/// The actor will not progress until this has been awaited or dropped.
///
/// This is returned by [`AskRequest::enqueue`] and [`AskRequest::try_enqueue`].
#[allow(missing_debug_implementations)]
#[must_use = "reply wont be received without awaiting"]
pub struct PendingReply<M, R>
where
    R: Reply,
{
    #[allow(clippy::type_complexity)]
    fut: BoxFuture<'static, Result<R::Ok, SendError<M, R::Error>>>,
}

impl<M, R> Future for PendingReply<M, R>
where
    R: Reply,
{
    type Output = Result<R::Ok, SendError<M, R::Error>>;

    fn poll(mut self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

/// A pending reply from a previously enqueued ask request.
///
/// The actor will not progress until this has been awaited or dropped.
///
/// This is returned by [`AskRequest::blocking_enqueue`].
#[allow(missing_debug_implementations)]
#[must_use = "reply wont be received without calling .recv()"]
pub struct BlockingPendingReply<'a, M, R>
where
    R: Reply,
{
    #[allow(clippy::type_complexity)]
    f: Box<dyn FnOnce() -> Result<R::Ok, SendError<M, R::Error>> + 'a>,
}

impl<M, R> BlockingPendingReply<'_, M, R>
where
    R: Reply,
{
    /// Receives the reply in a blocking context.
    pub fn recv(self) -> Result<R::Ok, SendError<M, R::Error>> {
        (self.f)()
    }
}

async fn receive_reply<M, R, T>(
    rx: oneshot::Receiver<Result<BoxReply, BoxSendError>>,
    reply_timeout: T,
) -> Result<R::Ok, SendError<M, R::Error>>
where
    M: 'static,
    R: Reply,
    T: Into<Option<Duration>>,
{
    let reply = match reply_timeout.into() {
        Some(timeout) => tokio::time::timeout(timeout, rx).await??,
        None => rx.await?,
    };
    downcast_reply::<M, R>(reply)
}

fn downcast_reply<M: 'static, R: Reply>(
    reply: Result<BoxReply, BoxSendError>,
) -> Result<R::Ok, SendError<M, R::Error>> {
    reply
        .map(|val| R::downcast_ok(val))
        .map_err(R::downcast_err)
}

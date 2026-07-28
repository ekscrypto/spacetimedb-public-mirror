//! TCP-level byte counter for upstream WebSocket progress.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::status::ByteCounter;

/// Wraps an async stream and counts bytes observed on `poll_read`.
pub struct ByteCountStream<S> {
    inner: S,
    counter: ByteCounter,
}

impl<S> ByteCountStream<S> {
    pub fn new(inner: S, counter: ByteCounter) -> Self {
        Self { inner, counter }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for ByteCountStream<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let n = buf.filled().len().saturating_sub(before) as u64;
            self.counter.record_read(n);
        }
        result
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for ByteCountStream<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

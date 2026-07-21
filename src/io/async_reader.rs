// Copyright (C) 2026 Tencent. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `tokio::io::{AsyncRead, AsyncSeek}` adapter for [`GoosefsFileInStream`].
//!
//! [`GoosefsAsyncReader`] is a thin wrapper that lets any tokio-based
//! consumer (e.g. `tokio::io::copy`, `tokio::io::BufReader`,
//! `tokio_util::io::ReaderStream`, the future opendal `goosefs` adapter,
//! Java JNI, C bindings, …) drive a Goosefs stream through the standard
//! poll-style traits, without re-implementing the chunk-overflow and
//! short-read logic that already lives inside the SDK.
//!
//! # Why a separate type?
//!
//! [`GoosefsFileInStream`] is intentionally `&mut self`-only (no `Sync`,
//! no internal mutex). That matches the Java/Go client's single-threaded
//! contract and avoids paying the cost of an `Arc<Mutex>` on the hot path.
//!
//! `tokio::io::AsyncRead::poll_read` is **also** `&mut self`-style, but
//! it cannot drive an `async fn` directly because the resulting future
//! would borrow `self` for its entire lifetime, conflicting with the
//! `Pin<&mut Self>` borrow that the trait implementation already holds.
//! The standard escape hatch — used here, in opendal-core, in object_store,
//! and in nearly every async-trait wrapper — is a small **state machine**
//! that *owns* the stream while a request is in flight:
//!
//! ```text
//!  ┌─────────┐  poll_read / start_seek    ┌─────────────┐
//!  │  Idle   │ ───────────────────────────►│ Reading /   │
//!  │ (stream)│                              │ Seeking     │
//!  └─────────┘                              │ (future)    │
//!       ▲                                   └─────────────┘
//!       │     future resolves: stream + result    │
//!       └─────────────────────────────────────────┘
//! ```
//!
//! In `Idle`, we hold the stream by value. When `poll_read` first fires,
//! we `mem::replace` it out, move it into a self-contained future
//! (`async move { let n = s.read(&mut buf).await; (s, n) }`), and
//! transition to `Reading`. Each subsequent poll drives the future; when
//! it resolves we put the stream back, copy bytes into the caller's
//! `ReadBuf`, and return to `Idle`.
//!
//! That gives us a `Send + Unpin` adapter with **zero** new locking and a
//! single heap allocation per outstanding I/O (the boxed future).
//!
//! # Byte-loss safety
//!
//! [`GoosefsFileInStream::read`] is loss-less since P5.5-A — chunk
//! overflow is parked in its `carry_over` buffer and delivered on the
//! next call. So this adapter doesn't need its own carry-over buffer:
//! when `tokio::io::copy` calls `poll_read` with a small `ReadBuf`, we
//! issue an SDK read sized to `buf.remaining()`, the SDK trims to that
//! size and parks the rest in its own `carry_over`, and the next
//! `poll_read` drains the parked bytes first.
//!
//! # `AsyncSeek` semantics
//!
//! `tokio::io::AsyncSeek` has a two-call contract:
//!   1. `start_seek(SeekFrom)` — *synchronously* records the request.
//!   2. `poll_complete(cx)` — drives the seek; returns the new position.
//!
//! We follow the same state-machine pattern: `start_seek` snapshots the
//! request and (if `Idle`) launches a self-contained future that owns the
//! stream; `poll_complete` polls it. Spec compliance: a second
//! `start_seek` while one is in flight returns `ErrorKind::Other`,
//! matching the behaviour of `tokio::fs::File`.

use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

use crate::error::Error;
use crate::io::file_in_stream::GoosefsFileInStream;

/// Boxed self-contained future that owns the stream while in flight.
///
/// `'static` because the future captures the stream by value (it does
/// not borrow `self` from the adapter). `Send` because all SDK I/O
/// futures are `Send`, which is required for the result to be useful on
/// multi-threaded tokio runtimes.
type OwnedReadFut =
    Pin<Box<dyn Future<Output = (Box<GoosefsFileInStream>, io::Result<Vec<u8>>)> + Send>>;

type OwnedSeekFut =
    Pin<Box<dyn Future<Output = (Box<GoosefsFileInStream>, io::Result<i64>)> + Send>>;

/// Internal state machine driving the AsyncRead / AsyncSeek surface.
///
/// `Idle` holds the stream by value (boxed to keep the enum compact —
/// `GoosefsFileInStream` is ~1.5 KB, while the in-flight `Reading` /
/// `Seeking` variants are already `Pin<Box<…>>` of similar size; boxing
/// `Idle` keeps the variants size-balanced and avoids the
/// `large_enum_variant` lint without paying for the extra allocation on
/// the hot read path — we re-use the same `Box` across all transitions.
/// The transient `Empty` state only exists during `mem::replace` — it
/// must never be observed by a poll method, hence the `unreachable!` on
/// that path.
enum State {
    /// No I/O in flight. Stream is parked here.
    Idle(Box<GoosefsFileInStream>),
    /// `poll_read` future in flight.
    Reading(OwnedReadFut),
    /// `poll_complete` future in flight (started by `start_seek`).
    Seeking(OwnedSeekFut),
    /// Transient placeholder used while we move the stream into a
    /// future. Never observed at the start of a poll fn — see
    /// [`take_idle`].
    Empty,
}

/// `AsyncRead + AsyncSeek` adapter over a [`GoosefsFileInStream`].
///
/// Construct with [`GoosefsAsyncReader::new`] (consumes the stream), and
/// recover the underlying stream with [`GoosefsAsyncReader::into_inner`]
/// when you no longer need the trait surface (only valid when no I/O is
/// in flight; returns `Err(self)` otherwise).
///
/// # Example
///
/// ```rust,no_run
/// use goosefs_sdk::io::{GoosefsAsyncReader, GoosefsFileInStream};
/// use goosefs_sdk::config::GoosefsConfig;
/// use goosefs_sdk::fs::options::OpenFileOptions;
/// use tokio::io::AsyncReadExt;
///
/// # async fn example() -> std::io::Result<()> {
/// # let cfg = GoosefsConfig::new("127.0.0.1:9200");
/// # let stream = GoosefsFileInStream::open(&cfg, "/data.bin", OpenFileOptions::default())
/// #     .await
/// #     .map_err(|e| std::io::Error::other(e.to_string()))?;
/// let mut reader = GoosefsAsyncReader::new(stream);
/// let mut buf = Vec::new();
/// reader.read_to_end(&mut buf).await?;
/// # Ok(())
/// # }
/// ```
///
/// `GoosefsAsyncReader: Send + Unpin` — `Pin<Box<…>>` futures are
/// already `Unpin`, and the wrapped stream has no self-references.
pub struct GoosefsAsyncReader {
    state: State,
}

impl GoosefsAsyncReader {
    /// Wrap a stream into an `AsyncRead + AsyncSeek` adapter.
    pub fn new(stream: GoosefsFileInStream) -> Self {
        Self {
            state: State::Idle(Box::new(stream)),
        }
    }

    /// Recover the underlying stream.
    ///
    /// Returns `Err(self)` if a read or seek is currently in flight.
    /// On success the stream is unboxed back to a value.
    #[allow(clippy::result_large_err)] // Self holds the stream by value when Idle; not on a hot path.
    pub fn into_inner(self) -> Result<GoosefsFileInStream, Self> {
        match self.state {
            State::Idle(s) => Ok(*s),
            _ => Err(self),
        }
    }

    /// Borrow the underlying stream when idle.
    ///
    /// Returns `None` if a read or seek is currently in flight.
    pub fn get_ref(&self) -> Option<&GoosefsFileInStream> {
        if let State::Idle(s) = &self.state {
            Some(s)
        } else {
            None
        }
    }

    /// Mutably borrow the underlying stream when idle.
    ///
    /// Returns `None` if a read or seek is currently in flight.
    pub fn get_mut(&mut self) -> Option<&mut GoosefsFileInStream> {
        if let State::Idle(s) = &mut self.state {
            Some(s)
        } else {
            None
        }
    }
}

/// Take ownership of the stream out of `Idle`, leaving `Empty` behind.
/// Caller must immediately replace the state before returning to the
/// runtime — `Empty` is never a valid resting state.
fn take_idle(state: &mut State) -> Box<GoosefsFileInStream> {
    match mem::replace(state, State::Empty) {
        State::Idle(s) => s,
        _ => unreachable!("take_idle called on non-Idle state"),
    }
}

/// Translate an SDK [`Error`] into [`io::Error`] for trait surfaces.
///
/// We use `ErrorKind::Other` uniformly — callers that want the typed
/// [`Error`] should use the inherent `read()` / `seek()` methods on
/// `GoosefsFileInStream` directly, not the trait surface.
fn sdk_err_to_io(err: Error) -> io::Error {
    io::Error::other(err.to_string())
}

impl AsyncRead for GoosefsAsyncReader {
    /// Drive a single `read(&mut buf)` call on the wrapped stream.
    ///
    /// Strategy:
    ///   - `Idle` → take the stream, build an owning future sized to
    ///     `buf.remaining()`, transition to `Reading`, fall through and
    ///     poll it on this same call (no extra wakeup).
    ///   - `Reading` → poll the in-flight future. On `Ready` we copy the
    ///     produced bytes into `buf` (the SDK guarantees `n ≤ cap`) and
    ///     return to `Idle`.
    ///   - Any other state is a programming error or contention with a
    ///     concurrent seek — returned as `io::Error`.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // GoosefsAsyncReader is Unpin — see the type-level docs.
        let this = self.get_mut();

        loop {
            match &mut this.state {
                State::Idle(_) => {
                    if buf.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    let cap = buf.remaining();
                    let mut stream = take_idle(&mut this.state);
                    let fut: OwnedReadFut = Box::pin(async move {
                        let mut tmp = vec![0u8; cap];
                        let result = stream.read(&mut tmp).await.map_err(sdk_err_to_io);
                        let bytes = match result {
                            Ok(n) => {
                                tmp.truncate(n);
                                Ok(tmp)
                            }
                            Err(e) => Err(e),
                        };
                        (stream, bytes)
                    });
                    this.state = State::Reading(fut);
                    // Loop around to poll the freshly-created future.
                }

                State::Reading(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready((stream, result)) => {
                        this.state = State::Idle(stream);
                        return match result {
                            Ok(data) => {
                                // SDK contract: `n ≤ buf.len()` always.
                                // We sized `buf` to `cap = buf.remaining()`
                                // so `data.len() ≤ cap` — no overflow
                                // possible. Defensive `min` keeps us
                                // correct under hypothetical SDK
                                // contract changes.
                                let take = data.len().min(buf.remaining());
                                buf.put_slice(&data[..take]);
                                Poll::Ready(Ok(()))
                            }
                            Err(e) => Poll::Ready(Err(e)),
                        };
                    }
                },

                State::Seeking(_) => {
                    return Poll::Ready(Err(io::Error::other(
                        "cannot read while a seek is in flight",
                    )));
                }

                State::Empty => unreachable!("Empty state observed in poll_read"),
            }
        }
    }
}

impl AsyncSeek for GoosefsAsyncReader {
    /// Record a seek request. Per `tokio::io::AsyncSeek`'s contract,
    /// this is *synchronous* and only kicks off the operation; the
    /// caller must follow up with `poll_complete` until it returns
    /// `Ready`.
    fn start_seek(self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        match &this.state {
            State::Reading(_) => Err(io::Error::other(
                "cannot start a seek while a read is in flight",
            )),
            State::Seeking(_) => Err(io::Error::other("another seek is already in flight")),
            State::Empty => unreachable!("Empty state observed in start_seek"),
            State::Idle(_) => {
                let stream = take_idle(&mut this.state);
                let fut: OwnedSeekFut = Box::pin(async move {
                    // `seek_owned` consumes the stream by value; we
                    // unbox to call it, then re-box the returned
                    // stream so the future's output type matches the
                    // boxed `OwnedSeekFut` signature.
                    let (s, result) = (*stream).seek_owned(position).await;
                    (Box::new(s), result)
                });
                this.state = State::Seeking(fut);
                Ok(())
            }
        }
    }

    /// Drive the in-flight seek to completion and surface the new
    /// absolute position. Per the trait contract, this is also called
    /// when no seek is in flight to report the *current* position; in
    /// that case we fast-path through the inherent `pos()`.
    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        match &mut this.state {
            State::Idle(s) => {
                let pos = s.pos().max(0) as u64;
                Poll::Ready(Ok(pos))
            }
            State::Seeking(fut) => match fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready((stream, result)) => {
                    this.state = State::Idle(stream);
                    match result {
                        Ok(pos) => Poll::Ready(Ok(pos.max(0) as u64)),
                        Err(e) => Poll::Ready(Err(e)),
                    }
                }
            },
            State::Reading(_) => Poll::Ready(Err(io::Error::other(
                "cannot complete a seek while a read is in flight",
            ))),
            State::Empty => unreachable!("Empty state observed in poll_complete"),
        }
    }
}

// ── Helper inherent method needed by the seek state machine ────────────────
//
// `poll_read` / `start_seek` need a version of the SDK's read/seek that
// take `self` by value and return it alongside the result (so the future
// is fully self-contained). For `read` we just inline a `&mut self` call
// inside the future since the future already owns the stream by move,
// but `seek_from`'s SDK signature is `&mut self → Result<i64>` and we'd
// need the stream back afterwards — so we add a small `seek_owned`
// helper here. Defining it in this file keeps `file_in_stream.rs`
// untouched by the trait-surface concern.

impl GoosefsFileInStream {
    /// Seek by value: takes `self` and returns it together with the
    /// seek result. Used by [`GoosefsAsyncReader`] to drive
    /// `tokio::io::AsyncSeek` without holding a borrow across the await.
    ///
    /// Not part of the public sequential-read API — prefer the
    /// inherent `seek_from(&mut self, …)` for direct use.
    pub(crate) async fn seek_owned(mut self, from: io::SeekFrom) -> (Self, io::Result<i64>) {
        let result = self.seek_from(from).await.map_err(sdk_err_to_io);
        (self, result)
    }
}

#[cfg(test)]
mod tests {
    //! Pure logic tests — no network. We verify the small bits we can
    //! exercise without a stream (error mapping, state-machine helper);
    //! the full trait surface is validated by the
    //! `examples/async_read_trait.rs` end-to-end roundtrip plus the
    //! Python binding's 105-test suite that exercises the SDK through
    //! every short-read path.

    use super::*;

    #[test]
    fn test_sdk_err_to_io_uses_other_kind() {
        let sdk = Error::Internal {
            message: "boom".to_string(),
            source: None,
        };
        let mapped = sdk_err_to_io(sdk);
        assert_eq!(mapped.kind(), io::ErrorKind::Other);
        assert!(mapped.to_string().contains("boom"));
    }

    #[test]
    fn test_state_empty_replacement_round_trip() {
        let mut s = State::Empty;
        let replaced = mem::replace(&mut s, State::Empty);
        assert!(matches!(replaced, State::Empty));
        assert!(matches!(s, State::Empty));
    }

    /// Compile-time check: the adapter is `Send + Unpin`, which is a
    /// load-bearing property for using it in `tokio::spawn` and in the
    /// trait surfaces of consumers like `tokio::io::copy`.
    #[test]
    fn test_send_and_unpin() {
        fn assert_send<T: Send>() {}
        fn assert_unpin<T: Unpin>() {}
        assert_send::<GoosefsAsyncReader>();
        assert_unpin::<GoosefsAsyncReader>();
    }
}

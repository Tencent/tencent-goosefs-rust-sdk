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

//! Future that awaits completion of an io_uring operation.
//!
//! References: Lance `future.rs:16-46`. Unlike Lance's read-only future, this
//! is generic across all operation types (read/write/open/close/unlink) and
//! returns `(result_code, Bytes)`.

use super::requests::IoRequest;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Future returned by [`super::store::UringPageStore`] for each io_uring
/// operation.
///
/// `poll` checks `RequestState.completed`:
/// - `true`  → take the error or the buffer and return `Poll::Ready`
/// - `false` → store the waker and return `Poll::Pending`; the background
///   thread calls `waker.wake()` when the CQE arrives.
///
/// The `Output` is `(result_code, Bytes)`:
/// - On error: `(negative_errno, empty_bytes)`
/// - On success (Read): `(bytes_read, read_data)`
/// - On success (OpenAt): `(fd, empty_bytes)`
/// - On success (Write/Close/UnlinkAt): `(0, empty_bytes)`
pub struct UringOpFuture {
    pub request: Arc<IoRequest>,
}

/// Errno reported when a request failed without an OS error of its own —
/// submission channel full, or the ring thread is gone.
const FALLBACK_ERRNO: i32 = 5; // EIO

/// Errno reported when a future is polled again after its result was already
/// taken. Any non-zero value works; `EIO` keeps the surface small.
const CONSUMED_ERRNO: i32 = 5; // EIO

impl Future for UringOpFuture {
    type Output = (i32, Bytes);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.request.state.lock().unwrap();

        if state.completed {
            // Guard against double-poll (spurious wake, or an executor that
            // re-polls after `Ready`). The result was already handed to the
            // first poll and the buffer moved out, so there is nothing left to
            // return.
            //
            // This MUST be negative. Returning `0` let an `OP_OPENAT` caller
            // read it as "success, fd 0" and take ownership of stdin — the
            // same class of bug as the positive errno handled below.
            if state.consumed {
                return Poll::Ready((-CONSUMED_ERRNO, Bytes::new()));
            }
            state.consumed = true;
            match state.err.take() {
                Some(err) => {
                    // Every caller detects failure with `result < 0` and then
                    // negates the value back into an errno, so what we hand
                    // out here MUST be negative.
                    //
                    // Two traps made that easy to get wrong:
                    //  * `raw_os_error()` returns a *positive* errno — the
                    //    driver already negated the CQE result when it built
                    //    this error (`from_raw_os_error(-result)`).
                    //  * synthetic errors carry no errno at all, and the old
                    //    `unwrap_or(-1)` fallback was the only negative value
                    //    in the function, which is what disguised the bug.
                    //
                    // Returning the positive errno made `ENOENT` (2) look like
                    // a freshly opened fd 2, so `OP_OPENAT` callers wrapped
                    // stderr in an `OwnedFd` and closed it. The process then
                    // aborted once mio reused that fd number — with no
                    // diagnostic, because the abort message is written to the
                    // very fd that had been destroyed.
                    let code = err.raw_os_error().unwrap_or(FALLBACK_ERRNO);
                    Poll::Ready((-code.abs(), Bytes::new()))
                }
                None => {
                    let bytes = std::mem::take(&mut state.buffer).freeze();
                    let code = state.result_code;
                    Poll::Ready((code, bytes))
                }
            }
        } else {
            // Not yet complete — store waker and return Pending.
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    // `requests` is a sibling of `future` under `uring`, not a child of
    // this module — so go up two levels (`tests` → `future` → `uring`).
    use super::super::requests::{RequestState, UringOpType};
    use super::*;
    use bytes::BytesMut;
    use std::sync::Mutex;
    use std::task::{Context, Wake, Waker};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn noop_waker() -> Waker {
        Waker::from(Arc::new(NoopWaker))
    }

    fn completed_request(buffer: BytesMut, result_code: i32) -> Arc<IoRequest> {
        Arc::new(IoRequest {
            fd: -1,
            offset: 0,
            length: buffer.len(),
            op_type: UringOpType::Read,
            open_flags: 0,
            state: Mutex::new(RequestState {
                completed: true,
                consumed: false,
                waker: None,
                err: None,
                buffer,
                bytes_transferred: 0,
                result_code,
            }),
        })
    }

    #[test]
    fn double_poll_after_ready_returns_error_not_fd_zero() {
        let data = BytesMut::from(&b"abcdef"[..]);
        let request = completed_request(data, 6);
        let mut fut = UringOpFuture {
            request: Arc::clone(&request),
        };
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let first = Pin::new(&mut fut).poll(&mut cx);
        match first {
            Poll::Ready((code, bytes)) => {
                assert_eq!(code, 6);
                assert_eq!(&bytes[..], b"abcdef");
            }
            Poll::Pending => panic!("first poll of completed request must be Ready"),
        }

        // Spurious re-poll after Ready must not panic or resurface a stale
        // result_code paired with an empty buffer.
        let second = Pin::new(&mut fut).poll(&mut cx);
        assert_eq!(
            second,
            Poll::Ready((-CONSUMED_ERRNO, Bytes::new())),
            "second poll must report an error, never a 0 that reads as fd 0"
        );
        assert!(request.state.lock().unwrap().consumed);
    }

    #[test]
    fn pending_until_completed_then_ready() {
        let request = Arc::new(IoRequest {
            fd: -1,
            offset: 0,
            length: 4,
            op_type: UringOpType::Read,
            open_flags: 0,
            state: Mutex::new(RequestState {
                completed: false,
                consumed: false,
                waker: None,
                err: None,
                buffer: BytesMut::new(),
                bytes_transferred: 0,
                result_code: 0,
            }),
        });
        let mut fut = UringOpFuture {
            request: Arc::clone(&request),
        };
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));
        assert!(request.state.lock().unwrap().waker.is_some());

        {
            let mut state = request.state.lock().unwrap();
            state.buffer = BytesMut::from(&b"wxyz"[..]);
            state.result_code = 4;
            state.completed = true;
        }

        let ready = Pin::new(&mut fut).poll(&mut cx);
        match ready {
            Poll::Ready((code, bytes)) => {
                assert_eq!(code, 4);
                assert_eq!(&bytes[..], b"wxyz");
            }
            Poll::Pending => panic!("completed request must be Ready"),
        }
    }

    #[test]
    fn completed_error_returns_negative_errno_and_empty_bytes() {
        let request = Arc::new(IoRequest {
            fd: -1,
            offset: 0,
            length: 0,
            op_type: UringOpType::OpenAt,
            open_flags: 0,
            state: Mutex::new(RequestState {
                completed: true,
                consumed: false,
                waker: None,
                err: Some(std::io::Error::from_raw_os_error(2)),
                buffer: BytesMut::new(),
                bytes_transferred: 0,
                result_code: 0,
            }),
        });
        let mut fut = UringOpFuture {
            request: Arc::clone(&request),
        };
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready((code, bytes)) => {
                // Negative: callers detect failure with `result < 0`. A
                // positive 2 here used to be read as "opened fd 2", which
                // made the caller take ownership of stderr.
                assert_eq!(code, -2);
                assert!(bytes.is_empty());
            }
            Poll::Pending => panic!("completed error must be Ready"),
        }
    }
}

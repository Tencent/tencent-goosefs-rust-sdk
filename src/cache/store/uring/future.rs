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

impl Future for UringOpFuture {
    type Output = (i32, Bytes);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.request.state.lock().unwrap();

        if state.completed {
            // Guard against double-poll: if the future was already consumed
            // (e.g. by a spurious wake or executor re-poll after Ready),
            // return a harmless empty result instead of a stale result_code
            // paired with an empty buffer (which would cause a length mismatch
            // panic in the caller).
            if state.consumed {
                return Poll::Ready((0, Bytes::new()));
            }
            state.consumed = true;
            match state.err.take() {
                Some(err) => {
                    let raw_err = err.raw_os_error().unwrap_or(-1);
                    Poll::Ready((raw_err, Bytes::new()))
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
    use super::requests::{RequestState, UringOpType};
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
    fn double_poll_after_ready_returns_empty_success() {
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
            Poll::Ready((0, Bytes::new())),
            "second poll must return a harmless empty success"
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
    fn completed_error_returns_raw_os_error_and_empty_bytes() {
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
                assert_eq!(code, 2);
                assert!(bytes.is_empty());
            }
            Poll::Pending => panic!("completed error must be Ready"),
        }
    }
}

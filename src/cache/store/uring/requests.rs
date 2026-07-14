//! Protocol types for communication between `UringPageStore` and the io_uring
//! background thread.
//!
//! Mirrors Lance `requests.rs` but extends the request type to cover write,
//! open, close, and unlink operations (Lance only implements read).
//!
//! See `docs/CLIENT_PAGE_CACHE_IO_URING_DESIGN.md` §3.1.

use bytes::BytesMut;
use std::io;
use std::os::unix::io::RawFd;
use std::sync::Mutex;
use std::task::Waker;

/// Shared state for a single IO operation.
///
/// The submitting async task constructs an [`IoRequest`], sends it to the
/// background thread via a channel, and awaits a [`super::future::UringOpFuture`].
/// The background thread pushes an SQE, and when the CQE arrives it updates
/// this state and calls `waker.wake()`. The future's `poll` checks `completed`.
///
/// References: Lance `requests.rs:13-20`.
pub struct RequestState {
    /// Whether the CQE has been reaped.
    pub completed: bool,
    /// Whether the future has already consumed the result (prevents
    /// double-take on spurious wake / re-poll after `Poll::Ready`).
    pub consumed: bool,
    /// The waiting task's waker (set by `Future::poll`, consumed by the driver).
    pub waker: Option<Waker>,
    /// Kernel error (set when CQE `result < 0`).
    pub err: Option<io::Error>,
    /// Read: destination buffer (allocated by the submitter).
    /// Write: source buffer (the page bytes).
    /// OpenAt/UnlinkAt: the NUL-terminated path string.
    /// Close: unused.
    pub buffer: BytesMut,
    /// Accumulated bytes transferred across short-read / short-write retries.
    pub bytes_transferred: usize,
    /// The raw CQE result code. For `OpenAt` this is the returned fd; for
    /// `Read`/`Write` it is the byte count of the last operation; for
    /// `Close`/`UnlinkAt` it is 0 on success.
    pub result_code: i32,
}

/// A single IO operation shared between the submitter, the background thread,
/// and the [`super::future::UringOpFuture`] via `Arc`.
///
/// References: Lance `requests.rs:24-38`.
pub struct IoRequest {
    /// File descriptor (provided by the caller; for `OpenAt` this is
    /// `AT_FDCWD`).
    pub fd: RawFd,
    /// Read/write offset within the file.
    pub offset: u64,
    /// Total bytes to read or write.
    pub length: usize,
    /// Operation kind.
    pub op_type: UringOpType,
    /// Open flags for `OpenAt` (e.g. `O_RDONLY`, `O_WRONLY | O_CREAT`). Ignored
    /// for other operation types.
    pub open_flags: i32,
    /// Shared completion state.
    pub state: Mutex<RequestState>,
}

/// io_uring operation kinds used by the page cache.
pub enum UringOpType {
    /// `pread` — positioned read (single syscall for seek + read).
    Read,
    /// `pwrite` — positioned write.
    Write,
    /// `openat` — open a file relative to a directory fd.
    OpenAt,
    /// `close` — close a file descriptor.
    Close,
    /// `unlinkat` — remove a directory entry.
    UnlinkAt,
}

impl IoRequest {
    /// Mark this request as failed and wake any waiting future.
    ///
    /// Used when a request cannot be submitted (e.g. SQ full) or when the
    /// background thread has died.
    ///
    /// References: Lance `requests.rs:45-53`.
    pub fn fail(&self, err: io::Error) {
        let mut state = self.state.lock().unwrap();
        state.err = Some(err);
        state.completed = true;
        if let Some(waker) = state.waker.take() {
            drop(state);
            waker.wake();
        }
    }
}

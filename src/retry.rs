//! Retry policies for Goosefs client operations.
//!
//! Provides configurable retry strategies modelled after the Java Goosefs client:
//!
//! - [`ExponentialTimeBoundedRetry`] — retries with exponential backoff bounded by
//!   a maximum total duration (mirrors Java `ExponentialTimeBoundedRetry`).
//! - [`ExponentialBackoffRetry`] — retries with exponential backoff bounded by
//!   a maximum number of attempts (mirrors Java `ExponentialBackoffRetry`).
//!
//! Both strategies add a small random jitter (0–10 %) to the sleep time to
//! avoid thundering-herd issues in multi-client deployments.

use std::time::{Duration, Instant};

use rand::Rng;

// ---------------------------------------------------------------------------
// RetryPolicy trait
// ---------------------------------------------------------------------------

/// A policy that decides whether another retry attempt should be made.
pub trait RetryPolicy: Send + Sync {
    /// Returns `true` if another attempt is allowed.
    ///
    /// Implementations may update internal state (e.g. increment a counter or
    /// record elapsed time) on each call.
    fn should_retry(&mut self) -> bool;

    /// The number of attempts made so far (including the initial one).
    fn attempt_count(&self) -> u32;

    /// The duration to sleep before the **next** attempt.
    ///
    /// Should be called *after* [`should_retry`](Self::should_retry) returns `true`.
    fn next_sleep(&self) -> Duration;
}

// ---------------------------------------------------------------------------
// ExponentialTimeBoundedRetry
// ---------------------------------------------------------------------------

/// Exponential-backoff retry bounded by total elapsed time.
///
/// Mirrors Java's `ExponentialTimeBoundedRetry`:
/// - Keeps retrying as long as `elapsed < max_duration`.
/// - The sleep time doubles each attempt: `initial_sleep → 2× → 4× → …`
///   capped at `max_sleep`.
/// - A random jitter of 0–10 % is added to each sleep.
/// - One *final* attempt is always allowed even if the deadline is about to
///   expire.
pub struct ExponentialTimeBoundedRetry {
    max_duration: Duration,
    max_sleep: Duration,
    start: Instant,
    attempts: u32,
    current_sleep: Duration,
}

impl ExponentialTimeBoundedRetry {
    /// Create a new time-bounded retry policy.
    ///
    /// # Arguments
    /// - `max_duration` — total time budget for all retries (default: 2 min).
    /// - `initial_sleep` — first backoff interval (default: 50 ms).
    /// - `max_sleep` — ceiling on the backoff interval (default: 3 s).
    pub fn new(max_duration: Duration, initial_sleep: Duration, max_sleep: Duration) -> Self {
        Self {
            max_duration,
            max_sleep,
            start: Instant::now(),
            attempts: 0,
            current_sleep: initial_sleep,
        }
    }

    /// Convenience constructor with Goosefs default parameters.
    ///
    /// Defaults: `max_duration = 2 min`, `initial_sleep = 50 ms`, `max_sleep = 3 s`.
    pub fn with_defaults() -> Self {
        Self::new(
            Duration::from_secs(120),
            Duration::from_millis(50),
            Duration::from_secs(3),
        )
    }
}

impl RetryPolicy for ExponentialTimeBoundedRetry {
    fn should_retry(&mut self) -> bool {
        if self.attempts == 0 {
            // Always allow the first attempt.
            self.attempts = 1;
            return true;
        }

        let elapsed = self.start.elapsed();
        if elapsed >= self.max_duration {
            return false;
        }

        self.attempts += 1;

        // Double the sleep on every retry beyond the first, capped at
        // `max_sleep`. `saturating_mul` prevents the panic that
        // `current_sleep * 2` raises on extreme configurations.
        // Doubling starts at attempt 2 (the 2nd retry, i.e. 3rd attempt
        // overall) — the very first retry uses the initial `current_sleep`
        // unchanged, which matches the documented semantics.
        if self.attempts >= 2 {
            self.current_sleep =
                std::cmp::min(self.current_sleep.saturating_mul(2), self.max_sleep);
        }

        true
    }

    fn attempt_count(&self) -> u32 {
        self.attempts
    }

    fn next_sleep(&self) -> Duration {
        add_jitter(self.current_sleep)
    }
}

// ---------------------------------------------------------------------------
// ExponentialBackoffRetry
// ---------------------------------------------------------------------------

/// Exponential-backoff retry bounded by a maximum number of attempts.
///
/// Mirrors Java's `ExponentialBackoffRetry`:
/// - Allows up to `max_retries` retries (so `max_retries + 1` total attempts).
/// - Sleep = `base_sleep * 2^(attempt-1)`, capped at `max_sleep`.
/// - A random jitter of 0–10 % is added.
pub struct ExponentialBackoffRetry {
    max_sleep: Duration,
    max_retries: u32,
    attempts: u32,
    current_sleep: Duration,
}

impl ExponentialBackoffRetry {
    /// Create a new count-bounded retry policy.
    ///
    /// # Arguments
    /// - `base_sleep` — initial backoff duration.
    /// - `max_sleep` — ceiling on the backoff duration.
    /// - `max_retries` — maximum number of *retries* (total attempts = `max_retries + 1`).
    pub fn new(base_sleep: Duration, max_sleep: Duration, max_retries: u32) -> Self {
        Self {
            max_sleep,
            max_retries,
            attempts: 0,
            current_sleep: base_sleep,
        }
    }
}

impl RetryPolicy for ExponentialBackoffRetry {
    fn should_retry(&mut self) -> bool {
        if self.attempts == 0 {
            self.attempts = 1;
            return true;
        }

        if self.attempts > self.max_retries {
            return false;
        }

        self.attempts += 1;

        // Double the sleep on every retry beyond the first, capped at
        // `max_sleep`. `saturating_mul` prevents overflow panics under
        // extreme configurations.
        if self.attempts >= 2 {
            self.current_sleep =
                std::cmp::min(self.current_sleep.saturating_mul(2), self.max_sleep);
        }

        true
    }

    fn attempt_count(&self) -> u32 {
        self.attempts
    }

    fn next_sleep(&self) -> Duration {
        add_jitter(self.current_sleep)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Add a random jitter of 0–10 % to a duration.
fn add_jitter(base: Duration) -> Duration {
    let mut rng = rand::rng();
    let jitter_fraction: f64 = rng.random_range(0.0..0.1);
    let jitter = base.mul_f64(jitter_fraction);
    base + jitter
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_bounded_first_attempt_always_allowed() {
        let mut policy = ExponentialTimeBoundedRetry::new(
            Duration::from_millis(0), // zero budget
            Duration::from_millis(10),
            Duration::from_millis(100),
        );
        // First attempt always succeeds.
        assert!(policy.should_retry());
        assert_eq!(policy.attempt_count(), 1);
        // Second attempt should fail because budget is 0.
        assert!(!policy.should_retry());
    }

    #[test]
    fn test_time_bounded_multiple_retries() {
        let mut policy = ExponentialTimeBoundedRetry::new(
            Duration::from_secs(10), // generous budget
            Duration::from_millis(10),
            Duration::from_millis(200),
        );
        // Should allow several attempts.
        for _ in 0..5 {
            assert!(policy.should_retry());
        }
        assert!(policy.attempt_count() == 5);
    }

    #[test]
    fn test_time_bounded_sleep_grows() {
        let initial = Duration::from_millis(50);
        let max_sleep = Duration::from_secs(3);
        let mut policy =
            ExponentialTimeBoundedRetry::new(Duration::from_secs(60), initial, max_sleep);

        assert!(policy.should_retry()); // attempt 1
        let s1 = policy.next_sleep();

        assert!(policy.should_retry()); // attempt 2
        let _s2 = policy.next_sleep();

        assert!(policy.should_retry()); // attempt 3
        let s3 = policy.next_sleep();

        // Sleep should generally grow (allowing for jitter).
        // After the off-by-one fix:
        //   s1 ≈ initial          (first retry, no doubling)
        //   s2 ≈ initial * 2      (second retry, FIRST doubling — was `initial` pre-fix)
        //   s3 ≈ initial * 4
        assert!(s1 <= initial + initial.mul_f64(0.11)); // within 10% jitter
        assert!(s3 >= initial); // after doubling
    }

    /// **Regression for the off-by-one in `should_retry`**: the doubling
    /// MUST kick in on the *second* retry. Pre-fix the condition was
    /// `attempts > 2` so the second retry still used the base sleep,
    /// contradicting the documented "every retry doubles" semantics.
    #[test]
    fn test_backoff_second_retry_doubles_base_sleep() {
        let base = Duration::from_millis(50);
        let max_sleep = Duration::from_secs(60); // out of the way
        let mut policy = ExponentialBackoffRetry::new(base, max_sleep, 10);

        // 1st retry: sleep stays at base.
        assert!(policy.should_retry());
        let s1 = policy.next_sleep();
        assert!(
            s1 >= base && s1 <= base + base.mul_f64(0.11),
            "1st retry sleep must be base ± 10% jitter, got {:?}",
            s1
        );

        // 2nd retry: sleep MUST be base * 2 (off-by-one fix).
        assert!(policy.should_retry());
        let s2 = policy.next_sleep();
        let expected_s2 = base * 2;
        assert!(
            s2 >= expected_s2 && s2 <= expected_s2 + expected_s2.mul_f64(0.11),
            "2nd retry MUST use base*2 ± jitter ({:?} ± 10%), got {:?} — off-by-one regression",
            expected_s2,
            s2
        );

        // 3rd retry: base * 4.
        assert!(policy.should_retry());
        let s3 = policy.next_sleep();
        let expected_s3 = base * 4;
        assert!(
            s3 >= expected_s3 && s3 <= expected_s3 + expected_s3.mul_f64(0.11),
            "3rd retry must use base*4 ± jitter ({:?} ± 10%), got {:?}",
            expected_s3,
            s3
        );
    }

    /// **Regression for the `current_sleep * 2` panic**: pathological
    /// configurations used to overflow `Duration` multiplication and
    /// panic. The fix uses `saturating_mul`, so doubling clamps at
    /// `Duration::MAX` no matter how many retries fire.
    #[test]
    fn test_backoff_saturating_mul_no_overflow() {
        // base * 2 would overflow Duration::from_secs(u64) — saturating_mul
        // must clamp to MAX instead of panicking.
        let base = Duration::from_secs(u64::MAX / 2 + 1);
        let mut policy = ExponentialBackoffRetry::new(base, Duration::MAX, 5);

        // Drive several doublings — pre-fix this panicked on the second
        // call (current_sleep * 2 overflow). We deliberately do NOT call
        // `next_sleep()` because `add_jitter` would itself overflow with
        // these extreme inputs; the panic we are guarding against is
        // inside `should_retry`'s `current_sleep * 2`.
        for _ in 0..5 {
            // Must not panic — that's the entire assertion.
            policy.should_retry();
        }
    }

    #[test]
    fn test_backoff_retry_max_retries() {
        let mut policy = ExponentialBackoffRetry::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            3, // max 3 retries → 4 total attempts
        );

        assert!(policy.should_retry()); // 1st attempt
        assert!(policy.should_retry()); // 1st retry
        assert!(policy.should_retry()); // 2nd retry
        assert!(policy.should_retry()); // 3rd retry
        assert!(!policy.should_retry()); // exceeded
        assert_eq!(policy.attempt_count(), 4);
    }

    #[test]
    fn test_backoff_retry_zero_retries() {
        let mut policy =
            ExponentialBackoffRetry::new(Duration::from_millis(10), Duration::from_millis(100), 0);
        assert!(policy.should_retry()); // 1st attempt
        assert!(!policy.should_retry()); // no retries allowed
        assert_eq!(policy.attempt_count(), 1);
    }

    #[test]
    fn test_backoff_sleep_capped() {
        let base = Duration::from_millis(50);
        let max_sleep = Duration::from_millis(100);
        let mut policy = ExponentialBackoffRetry::new(base, max_sleep, 10);

        // Burn through attempts to trigger doubling.
        for _ in 0..6 {
            assert!(policy.should_retry());
        }

        let sleep = policy.next_sleep();
        // After several doublings, sleep must not exceed max_sleep + 10% jitter.
        assert!(sleep <= max_sleep + max_sleep.mul_f64(0.11));
    }

    #[test]
    fn test_jitter_within_bounds() {
        let base = Duration::from_millis(100);
        for _ in 0..100 {
            let result = add_jitter(base);
            assert!(result >= base);
            assert!(result <= base + base.mul_f64(0.11)); // 10% + tiny float margin
        }
    }
}

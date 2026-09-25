// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Crossbeam timers and receive deadlines that follow a clock.

use crate::Clock;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

impl Clock {
    /// Returns a receiver that gets the instant `duration` from now once this
    /// clock reaches it.
    ///
    /// It mirrors `crossbeam_channel::after`, which it is on the real clock. On
    /// a test clock, an advance to the deadline sends it, and the channel stays
    /// connected while the clock lives. A duration past the end of `Instant`'s
    /// range returns a receiver that never fires.
    ///
    /// Wait for a job or a timeout, driven from a test through `wait_timers`,
    /// since `wait_blocked` cannot see a thread blocked in `select!`:
    ///
    /// ```
    /// # #[cfg(feature = "test-clock")] {
    /// use darkbio_clock::TestClock;
    /// use darkbio_clock::crossbeam_channel::{select, unbounded};
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut tester = TestClock::new();
    /// let clock = tester.clock();
    /// let (_jobs, queue) = unbounded::<u32>();
    ///
    /// let worker = thread::spawn(move || {
    ///     select! {
    ///         recv(queue) -> job => job.ok(),
    ///         recv(clock.after(Duration::from_secs(5))) -> _ => None,
    ///     }
    /// });
    /// tester.wait_timers(1);
    /// tester.advance(Duration::from_secs(5));
    /// assert_eq!(worker.join().unwrap(), None);
    /// # }
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "crossbeam")))]
    pub fn after(&self, duration: Duration) -> Receiver<Instant> {
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return match paused.now().checked_add(duration) {
                Some(deadline) => paused.at(deadline),
                None => crossbeam_channel::never(),
            };
        }
        crossbeam_channel::after(duration)
    }

    /// Returns a receiver that gets `deadline` once this clock reaches it, at
    /// once if it already has.
    ///
    /// It mirrors `crossbeam_channel::at`, which it is on the real clock. On a
    /// test clock, an advance to the deadline sends it, even one that overshoots,
    /// and the channel stays connected while the clock lives.
    #[cfg_attr(docsrs, doc(cfg(feature = "crossbeam")))]
    pub fn at(&self, deadline: Instant) -> Receiver<Instant> {
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return paused.at(deadline);
        }
        crossbeam_channel::at(deadline)
    }

    /// Receives a message, waiting up to `timeout` on this clock.
    ///
    /// It mirrors crossbeam's `Receiver::recv_timeout`, which it is on the real
    /// clock. A buffered message or a disconnection wins over the timeout. A
    /// timeout past the end of `Instant`'s range waits like `Receiver::recv`.
    #[cfg_attr(docsrs, doc(cfg(feature = "crossbeam")))]
    pub fn recv_timeout<T>(
        &self,
        receiver: &Receiver<T>,
        timeout: Duration,
    ) -> Result<T, RecvTimeoutError> {
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return match paused.now().checked_add(timeout) {
                Some(deadline) => paused.recv_deadline(receiver, deadline),
                None => receiver.recv().map_err(RecvTimeoutError::from),
            };
        }
        receiver.recv_timeout(timeout)
    }

    /// Receives a message, waiting until this clock reaches `deadline`.
    ///
    /// It mirrors crossbeam's `Receiver::recv_deadline`, which it is on the real
    /// clock. A buffered message or a disconnection wins over the deadline. On a
    /// test clock, a waiting receive arms a timer, which `wait_timers` counts
    /// until it fires or the receive returns.
    #[cfg_attr(docsrs, doc(cfg(feature = "crossbeam")))]
    pub fn recv_deadline<T>(
        &self,
        receiver: &Receiver<T>,
        deadline: Instant,
    ) -> Result<T, RecvTimeoutError> {
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return paused.recv_deadline(receiver, deadline);
        }
        receiver.recv_deadline(deadline)
    }
}

// The tests live in src/tests, loaded from here so that they keep this
// module's private items in reach
#[cfg(all(test, not(loom)))]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "tests/timers.rs"]
mod tests;

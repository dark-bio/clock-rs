// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Pull in the README as the package doc
#![doc = include_str!("../README.md")]
// Enable the experimental doc_cfg feature
#![cfg_attr(docsrs, feature(doc_cfg))]
// Allow excluding test code from coverage measurements on nightly
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
// The crate only builds on safe synchronization and never needs unsafe
#![forbid(unsafe_code)]

pub mod sync;

// Every internal lock comes from here, so loom can swap in its own when model
// checking the waits
mod primitives;

#[cfg(feature = "crossbeam")]
mod timers;

/// The crossbeam-channel crate that the clock's timers use, for naming its
/// types at the same version.
#[cfg(feature = "crossbeam")]
#[cfg_attr(docsrs, doc(cfg(feature = "crossbeam")))]
pub use crossbeam_channel;

// Test clocks exist only in tests, so a build without the test-clock feature
// cannot stop its own time
#[cfg(any(test, feature = "test-clock"))]
mod paused;

#[cfg(feature = "test-clock")]
pub use paused::TestClock;
#[cfg(all(test, not(feature = "test-clock")))]
use paused::TestClock;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use primitives::{Condvar, Mutex, MutexGuard};

/// A clock that reads real time, or a test's time that moves only on command.
///
/// Clones share one clock. Equality compares identity, so all real clocks are
/// equal and a test clock equals only the handles of its own `TestClock`.
#[derive(Clone)]
pub struct Clock {
    /// Shared state of a test clock, or nothing for the real clock.
    #[cfg(any(test, feature = "test-clock"))]
    paused: Option<Arc<paused::Paused>>,
}

impl Clock {
    /// Returns the real clock, which reads the system's monotonic and wall times.
    pub const fn real() -> Self {
        Self {
            #[cfg(any(test, feature = "test-clock"))]
            paused: None,
        }
    }

    /// Returns the clock's current monotonic time.
    pub fn now(&self) -> Instant {
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return paused.now();
        }
        Instant::now()
    }

    /// Returns the time since `since`, or zero if it is later than now.
    pub fn elapsed(&self, since: Instant) -> Duration {
        self.now().saturating_duration_since(since)
    }

    /// Returns the clock's current wall time.
    pub fn system_time(&self) -> SystemTime {
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return paused.system_time();
        }
        SystemTime::now()
    }

    /// Blocks the thread until `duration` has passed on this clock.
    ///
    /// On a test clock, only its advances end the sleep.
    ///
    /// # Panics
    ///
    /// Panics on a test clock if the deadline overflows [`Instant`].
    pub fn sleep(&self, duration: Duration) {
        #[cfg(any(test, feature = "test-clock"))]
        if self.paused.is_some() {
            self.sleep_until(self.now() + duration);
            return;
        }
        std::thread::sleep(duration);
    }

    /// Blocks the thread until this clock reaches `deadline`, returning at once
    /// if it already has.
    ///
    /// On a test clock, only its advances end the sleep.
    pub fn sleep_until(&self, deadline: Instant) {
        self.waiter().wait_until(Some(deadline), || None::<()>);
    }

    /// Returns the shared state's address, or nothing for the real clock.
    fn identity(&self) -> Option<*const ()> {
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return Some(Arc::as_ptr(paused).cast());
        }
        None
    }

    /// Creates a waiter that measures deadlines on this clock.
    fn waiter(&self) -> Waiter {
        // Register the signal before any wait can observe the clock
        let signal = Arc::new(Signal::default());
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            paused.register(&signal);
        }

        // Keep the registration alive for the waiter's lifetime
        Waiter {
            clock: self.clone(),
            signal,
        }
    }
}

// A production clock carries no state, so passing one around costs nothing
#[cfg(not(any(test, feature = "test-clock")))]
const _: () = assert!(size_of::<Clock>() == 0);

impl PartialEq for Clock {
    /// Compares identity, with all real clocks equal to each other.
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for Clock {}

impl fmt::Debug for Clock {
    /// Shows whether the clock is paused, its advance and its wall time.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut clock = f.debug_struct("Clock");
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return clock
                .field("paused", &true)
                .field("advanced", &paused.advanced())
                .field("system_time", &paused.system_time())
                .finish();
        }
        clock.field("paused", &false).finish()
    }
}

/// Blocks threads until a condition holds or a deadline passes on a clock.
struct Waiter {
    /// Clock that decides when deadlines pass.
    clock: Clock,
    /// Wakeups registered with a test clock until this waiter drops.
    signal: Arc<Signal>,
}

impl Waiter {
    /// Blocks until `ready` returns a value or the clock reaches `deadline`.
    ///
    /// A ready value wins over an expired deadline. Without a deadline, only
    /// a ready value ends the wait. The callback runs without crate locks held.
    fn wait_until<T>(
        &self,
        deadline: Option<Instant>,
        mut ready: impl FnMut() -> Option<T>,
    ) -> Option<T> {
        // Pick the real timer once, so the test seam reports what every park uses
        let timer = self.timer(deadline);

        // Check and park in turns, until a value arrives or the deadline passes
        loop {
            // Take the generation before checking, so a change after the check cuts the park short
            let seen = self.signal.generation();
            if let Some(value) = ready() {
                return Some(value);
            }
            if deadline.is_some_and(|deadline| self.clock.now() >= deadline) {
                return None;
            }
            drop(self.park(self.signal.lock(), seen, deadline, timer));
        }
    }

    /// Returns the deadline as a real timer on a real clock, and no timer on a
    /// test clock, whose advances end its waits.
    fn timer(&self, deadline: Option<Instant>) -> Option<Instant> {
        #[cfg(any(test, feature = "test-clock"))]
        if self.clock.paused.is_some() {
            return None;
        }
        deadline
    }

    /// Parks until the generation moves on from `seen` or the clock reaches
    /// `deadline`, and returns the state locked again.
    ///
    /// Only a real clock's park uses a real timer. In tests, one-shot hooks run
    /// before its first wait and before a wait after a spurious wakeup, with
    /// the signal unlocked.
    fn park<'a>(
        &'a self,
        mut state: MutexGuard<'a, SignalState>,
        seen: u64,
        deadline: Option<Instant>,
        timer: Option<Instant>,
    ) -> MutexGuard<'a, SignalState> {
        // Count the park once, keeping it counted through spurious wakeups
        #[cfg(any(test, feature = "test-clock"))]
        let mut blocked = None;

        loop {
            // Stop once the generation moves or the clock reaches the deadline
            let now = self.clock.now();
            if state.generation != seen || deadline.is_some_and(|deadline| now >= deadline) {
                break;
            }

            // Let tests act before a wait, then check again for any change they made
            #[cfg(test)]
            if let Some(hook) = self
                .clock
                .paused
                .as_ref()
                .and_then(|paused| paused.take_hook(blocked.is_some()))
            {
                drop(state);
                hook(timer);
                state = self.signal.lock();
                continue;
            }

            // Count the park while the signal is locked, so that an advance seen after
            // the count still wakes it
            #[cfg(any(test, feature = "test-clock"))]
            if blocked.is_none() {
                blocked = self
                    .clock
                    .paused
                    .as_ref()
                    .map(|paused| paused.block(deadline));
            }
            #[cfg(test)]
            {
                state.parks += 1;
                self.signal.parked.notify_all();
            }

            // Wait for a wakeup or the real timer, leaving expiry to the check above
            state = match timer {
                None => self
                    .signal
                    .changed
                    .wait(state)
                    .expect("waiter signal not poisoned"),
                Some(timer) => {
                    self.signal
                        .changed
                        .wait_timeout(state, timer - now)
                        .expect("waiter signal not poisoned")
                        .0
                }
            };
        }

        // Uncount the park before the caller acts on its wakeup
        #[cfg(any(test, feature = "test-clock"))]
        drop(blocked);
        state
    }
}

#[cfg(any(test, feature = "test-clock"))]
impl Drop for Waiter {
    /// Removes the waiter's registration as soon as it is no longer live.
    fn drop(&mut self) {
        if let Some(paused) = &self.clock.paused {
            paused.unregister(&self.signal);
        }
    }
}

impl fmt::Debug for Waiter {
    /// Shows the clock, never the wakeup bookkeeping.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Waiter")
            .field("clock", &self.clock)
            .finish_non_exhaustive()
    }
}

/// Wakes threads through a generation read before each condition check.
#[derive(Default)]
struct Signal {
    /// Counters and the test park count, guarded together.
    state: Mutex<SignalState>,
    /// Wakes parked threads when the generation moves.
    changed: Condvar,
    /// Reports new parks to tests, without waking parked threads.
    #[cfg(test)]
    parked: Condvar,
}

/// Mutable part of a signal.
#[derive(Default)]
struct SignalState {
    /// Number of notifications and clock advances, wrapping on overflow.
    generation: u64,
    /// Number of notifications alone, which end condvar waits, wrapping on overflow.
    notifications: u64,
    /// Total parks that found the generation unchanged, for test synchronization.
    #[cfg(test)]
    parks: usize,
}

impl Signal {
    /// Locks the state, which no code panics under.
    fn lock(&self) -> MutexGuard<'_, SignalState> {
        self.state.lock().expect("waiter signal not poisoned")
    }

    /// Returns the current generation, to compare against when parking.
    fn generation(&self) -> u64 {
        self.lock().generation
    }

    /// Counts a notification and wakes one parked thread.
    ///
    /// Every waiting thread sees the count, so another one may return too,
    /// as a spurious wakeup, the next time it wakes.
    fn notify_one(&self) {
        let mut state = self.lock();
        state.generation = state.generation.wrapping_add(1);
        state.notifications = state.notifications.wrapping_add(1);
        self.changed.notify_one();
    }

    /// Counts a notification and wakes every parked thread.
    fn notify_all(&self) {
        let mut state = self.lock();
        state.generation = state.generation.wrapping_add(1);
        state.notifications = state.notifications.wrapping_add(1);
        self.changed.notify_all();
    }

    /// Wakes every parked thread to recheck the time, without counting a notification.
    #[cfg(any(test, feature = "test-clock"))]
    fn advance(&self) {
        let mut state = self.lock();
        state.generation = state.generation.wrapping_add(1);
        self.changed.notify_all();
    }
}

// The tests live in src/tests, apart from the code they check
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

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

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use primitives::{Condvar, Mutex, MutexGuard};

/// Longest real wait handed to the OS in one call, since Windows waits forever
/// on timeouts of about 49.7 days or more.
const MAX_REAL_WAIT: Duration = Duration::from_secs(24 * 60 * 60);

/// A clock that reads real time, or a test's time that moves only on command.
///
/// Clones share one clock. Equality compares identity, so all real clocks are
/// equal and a test clock equals only the handles of its own `TestClock`.
///
/// It cannot be built from a struct literal and it has a destructor in every
/// build, so code that compiles without `test-clock` compiles with it too.
#[derive(Clone)]
#[non_exhaustive]
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
    #[must_use]
    pub fn now(&self) -> Instant {
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return paused.now();
        }
        Instant::now()
    }

    /// Returns the time since `since`, or zero if it is later than now.
    #[must_use]
    pub fn elapsed(&self, since: Instant) -> Duration {
        self.now().saturating_duration_since(since)
    }

    /// Returns the clock's current wall time.
    #[must_use]
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
        // A test clock's sleep waits for the deadline this duration gives
        #[cfg(any(test, feature = "test-clock"))]
        if self.paused.is_some() {
            self.sleep_until(self.now() + duration);
            return;
        }

        // A real sleep waits in capped slices, rechecking the time slept after each
        let start = Instant::now();
        loop {
            let elapsed = start.elapsed();
            if elapsed >= duration {
                return;
            }
            std::thread::sleep((duration - elapsed).min(MAX_REAL_WAIT));
        }
    }

    /// Blocks the thread until this clock reaches `deadline`, returning at once
    /// if it already has.
    ///
    /// On a test clock, only its advances end the sleep.
    pub fn sleep_until(&self, deadline: Instant) {
        // A test clock's sleep parks until an advance reaches its deadline
        #[cfg(any(test, feature = "test-clock"))]
        if self.paused.is_some() {
            self.waiter().wait(Some(deadline));
            return;
        }

        // A real sleep waits in capped slices, rechecking the deadline after each
        loop {
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            std::thread::sleep((deadline - now).min(MAX_REAL_WAIT));
        }
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
        Waiter {
            clock: self.clone(),
            signal: Arc::new(Signal::default()),
        }
    }
}

// A production clock carries no state, so passing one around costs nothing
#[cfg(not(any(test, feature = "test-clock")))]
const _: () = assert!(size_of::<Clock>() == 0);

// A production clock still has a destructor, as a test build's does
#[cfg(not(any(test, feature = "test-clock")))]
const _: () = assert!(std::mem::needs_drop::<Clock>());

impl Drop for Clock {
    /// Does nothing, but gives every build a destructor, so enabling
    /// `test-clock` does not change which const contexts accept a clock.
    fn drop(&mut self) {}
}

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
        // Read both times under one lock before calling the formatter's writer
        #[cfg(any(test, feature = "test-clock"))]
        let snapshot = self.paused.as_ref().map(|paused| paused.snapshot());

        // Format without the clock lock, since the writer may read this clock
        let mut clock = f.debug_struct("Clock");
        #[cfg(any(test, feature = "test-clock"))]
        if let Some((advanced, system_time)) = snapshot {
            return clock
                .field("paused", &true)
                .field("advanced", &advanced)
                .field("system_time", &system_time)
                .finish();
        }
        clock.field("paused", &false).finish()
    }
}

/// Blocks threads until notified or a deadline passes on a clock.
struct Waiter {
    /// Clock that decides when deadlines pass.
    clock: Clock,
    /// Notifications for this waiter's waits, and the wakeups that deliver them.
    signal: Arc<Signal>,
}

impl Waiter {
    /// Starts a wait and parks until a notification ends it or the clock
    /// reaches `deadline`, returning whether a notification ended it.
    #[cfg(any(test, feature = "test-clock"))]
    fn wait(&self, deadline: Option<Instant>) -> bool {
        let mut state = self.signal.lock();
        let start = state.start();
        self.park(state, start, deadline).1
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

    /// Parks the wait that started at `start` until a notification ends it or
    /// the clock reaches `deadline`. Returns the state locked again, and
    /// whether a notification ended the wait.
    ///
    /// On a test clock, the park counts as blocked and lists its deadline from
    /// its first wait until it returns, however often it wakes in between. Only
    /// a real clock's park uses a real timer. In tests, one-shot hooks run
    /// before its first wait and before a wait after a spurious wakeup, with
    /// the signal unlocked.
    fn park<'a>(
        &'a self,
        mut state: MutexGuard<'a, SignalState>,
        start: u64,
        deadline: Option<Instant>,
    ) -> (MutexGuard<'a, SignalState>, bool) {
        // Only a real clock times its waits, since a test clock's advances end them
        let timer = self.timer(deadline);

        // Count the park once, keeping it counted through every wakeup until it returns
        #[cfg(any(test, feature = "test-clock"))]
        let mut blocked = None;

        let notified = loop {
            // Take a notification first, so that one wins over a reached deadline
            if state.claim(start) {
                break true;
            }
            let remaining =
                deadline.map(|deadline| deadline.saturating_duration_since(self.clock.now()));
            if remaining.is_some_and(|remaining| remaining.is_zero()) {
                break false;
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

            // Count the park under the clock lock, so an advance either finds it or has
            // already passed its deadline
            #[cfg(any(test, feature = "test-clock"))]
            if blocked.is_none() {
                if let Some(paused) = &self.clock.paused {
                    blocked = paused.block(deadline, &self.signal);
                    if blocked.is_none() {
                        break false;
                    }
                }
            }
            #[cfg(test)]
            {
                state.parks += 1;
                self.signal.parked.notify_all();
            }

            // Wait for a wakeup or the real timer, leaving expiry to the check above
            state = match timer.and(remaining) {
                None => self
                    .signal
                    .changed
                    .wait(state)
                    .expect("waiter signal not poisoned"),
                Some(remaining) => {
                    self.signal
                        .changed
                        .wait_timeout(state, remaining.min(MAX_REAL_WAIT))
                        .expect("waiter signal not poisoned")
                        .0
                }
            };
        };

        // Retire and unlist the wait under the signal lock before its caller acts
        state.retire(start);
        #[cfg(any(test, feature = "test-clock"))]
        drop(blocked);
        (state, notified)
    }
}

/// Hands notifications to a waiter's waits and wakes parked threads to check
/// for one or for their deadline.
#[derive(Default)]
struct Signal {
    /// Notification bookkeeping and the test park count, guarded together.
    state: Mutex<SignalState>,
    /// Wakes parked threads for a notification or a reached deadline.
    changed: Condvar,
    /// Reports new parks to tests, without waking parked threads.
    #[cfg(test)]
    parked: Condvar,
    /// Clock wakes delivered, so tests can check which waits an advance reached.
    #[cfg(test)]
    wakes: AtomicUsize,
}

/// Mutable part of a signal.
///
/// Notifications are numbered in the order they are sent, and a wait can only
/// take one sent after it started. Each queued `notify_one` can be matched to
/// a distinct counted wait that started before it and will check again, so
/// none is ever left behind. A `u64` count cannot wrap within any run.
#[derive(Default)]
struct SignalState {
    /// Number of the latest notification, one-shot or broadcast.
    notifications: u64,
    /// Number of the latest `notify_all`, which ends every wait started before it.
    broadcast: u64,
    /// Waits started and neither finished nor ended by a broadcast.
    waiting: usize,
    /// Numbers of the `notify_one` calls no wait has taken yet, oldest first.
    pending: VecDeque<u64>,
    /// Total physical waits entered, for test synchronization.
    #[cfg(test)]
    parks: usize,
}

impl SignalState {
    /// Counts a new wait, returning its start, the number after which it
    /// can take notifications.
    fn start(&mut self) -> u64 {
        self.waiting += 1;
        self.notifications
    }

    /// Returns whether a notification ends the wait that started at `start`,
    /// taking the oldest queued one sent after it unless a broadcast ended it.
    fn claim(&mut self, start: u64) -> bool {
        // A broadcast ends every wait started before it, and takes no queued notification
        if self.broadcast > start {
            return true;
        }

        // Take the oldest notification this wait can, leaving earlier ones to earlier waits
        if let Some(index) = self.pending.iter().position(|&sent| sent > start) {
            self.pending.remove(index);
            return true;
        }
        false
    }

    /// Uncounts a finished wait, unless the broadcast that ended it already did.
    fn retire(&mut self, start: u64) {
        if self.broadcast <= start {
            self.waiting -= 1;
        }
    }
}

impl Signal {
    /// Locks the state, which no code panics under.
    fn lock(&self) -> MutexGuard<'_, SignalState> {
        self.state.lock().expect("waiter signal not poisoned")
    }

    /// Queues a notification for one of the waits already started, and wakes a
    /// parked thread to take it. Queues none once every counted wait has one.
    fn notify_one(&self) {
        let mut state = self.lock();
        state.notifications += 1;
        if state.pending.len() < state.waiting {
            let sent = state.notifications;
            state.pending.push_back(sent);
            self.changed.notify_one();
        }
    }

    /// Ends every wait started so far, dropping their queued notifications,
    /// and wakes every parked thread.
    fn notify_all(&self) {
        // Uncount the ended waits at once, so that no later notify_one queues one for them
        let mut state = self.lock();
        state.notifications += 1;
        state.broadcast = state.notifications;
        state.pending.clear();
        state.waiting = 0;
        self.changed.notify_all();
    }

    /// Wakes every parked thread to recheck the time, without counting a notification.
    #[cfg(any(test, feature = "test-clock"))]
    fn wake(&self) {
        #[cfg(test)]
        self.wakes.fetch_add(1, Ordering::SeqCst);

        // Take the lock, so a park that read the time before the advance is waiting by now
        let _state = self.lock();
        self.changed.notify_all();
    }
}

// The tests live in src/tests, apart from the code they check
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

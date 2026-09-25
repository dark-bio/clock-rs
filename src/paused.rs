// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Test clocks, which move only when their owner advances them.

use crate::{Clock, Signal};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant, SystemTime};

/// Owns a clock that moves only when advanced, for tests.
///
/// [`Self::clock`] hands out handles that read and sleep on its time. Only the
/// owner moves it, through methods that take `&mut self`, so each test clock
/// has one driver.
#[cfg_attr(docsrs, doc(cfg(feature = "test-clock")))]
pub struct TestClock {
    /// State shared with every clock handle.
    paused: Arc<Paused>,
}

impl TestClock {
    /// Creates a stopped clock at the current real monotonic and wall times.
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            paused: Arc::new(Paused {
                start: now,
                state: Mutex::new(PausedState {
                    now,
                    system_time: SystemTime::now(),
                    signals: BTreeMap::new(),
                    blocked: 0,
                }),
                changed: Condvar::new(),
                #[cfg(test)]
                before_park: Mutex::new(None),
            }),
        }
    }

    /// Returns a handle that reads and sleeps on this clock.
    pub fn clock(&self) -> Clock {
        Clock {
            paused: Some(self.paused.clone()),
        }
    }

    /// Moves both times forward by `by` and wakes every parked wait.
    ///
    /// Returns once waits are notified, without waiting for them to act.
    /// A zero advance does nothing.
    ///
    /// # Panics
    ///
    /// Panics if either time would overflow, before changing either one.
    pub fn advance(&mut self, by: Duration) {
        self.advance_with(|now| {
            now.checked_add(by)
                .expect("clock advance overflows Instant")
        });
    }

    /// Moves monotonic time to `target` and wall time by the same amount.
    ///
    /// Wakes every parked wait and returns without waiting for them to act.
    /// Advancing to the current time does nothing.
    ///
    /// # Panics
    ///
    /// Panics if `target` is before now or wall time would overflow.
    /// Neither time changes after a panic.
    pub fn advance_to(&mut self, target: Instant) {
        self.advance_with(|now| {
            assert!(target >= now, "clock cannot go backwards");
            target
        });
    }

    /// Sets wall time forwards or backwards without moving monotonic time.
    ///
    /// This wakes no waits, since their deadlines use monotonic time.
    pub fn set_system_time(&mut self, time: SystemTime) {
        self.paused.lock().system_time = time;
    }

    /// Blocks until at least `count` threads are parked in this clock's sleeps.
    ///
    /// A thread parked earlier counts too, so the count proves no progress on
    /// its own. Nothing bounds the wait, so the test runner ends a hang.
    pub fn wait_blocked(&self, count: usize) {
        let mut state = self.paused.lock();
        while state.blocked < count {
            state = self
                .paused
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Validates both new times before updating state and notifying waiters.
    fn advance_with(&mut self, next: impl FnOnce(Instant) -> Instant) {
        // Compute and check the new times outside the lock, so a failed check's
        // panic runs no hook under it. Only the owner advances, so nothing
        // changes the times in between.
        let (now, system_time) = {
            let state = self.paused.lock();
            (state.now, state.system_time)
        };
        let next = next(now);
        if next == now {
            return;
        }
        let system_time = system_time
            .checked_add(next - now)
            .expect("clock advance overflows SystemTime");

        // Publish both times and collect the live waiters in one critical section
        let mut state = self.paused.lock();
        let signals: Vec<_> = state.signals.values().filter_map(Weak::upgrade).collect();
        state.now = next;
        state.system_time = system_time;
        drop(state);

        // Wake outside the clock lock, since parking takes the signal lock first
        for signal in signals {
            signal.notify_all();
        }
    }
}

impl Default for TestClock {
    /// Creates a stopped clock at the current real monotonic and wall times.
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TestClock {
    /// Shows the advance, wall time and number of parked threads.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (advanced, system_time, blocked) = {
            let state = self.paused.lock();
            (
                state.now - self.paused.start,
                state.system_time,
                state.blocked,
            )
        };
        f.debug_struct("TestClock")
            .field("advanced", &advanced)
            .field("system_time", &system_time)
            .field("blocked", &blocked)
            .finish()
    }
}

/// A test clock's state, shared by its owner, handles and waiters.
pub(crate) struct Paused {
    /// Time the clock started at, to show how far it has advanced.
    start: Instant,
    /// Current times, live waiters and parked thread count.
    pub(crate) state: Mutex<PausedState>,
    /// Wakes drivers when the parked thread count increases.
    changed: Condvar,
    /// One-shot test hook between a failed deadline check and its park.
    #[cfg(test)]
    pub(crate) before_park: Mutex<Option<BeforePark>>,
}

/// Mutable part of a test clock.
pub(crate) struct PausedState {
    /// Current monotonic time.
    now: Instant,
    /// Current wall time, independent of monotonic time when set explicitly.
    system_time: SystemTime,
    /// Live waiters indexed by signal address, removed when each waiter drops.
    pub(crate) signals: BTreeMap<usize, Weak<Signal>>,
    /// Threads committed to parking while holding their signal's lock.
    pub(crate) blocked: usize,
}

impl Paused {
    /// Returns the clock's current monotonic time.
    pub(crate) fn now(&self) -> Instant {
        self.lock().now
    }

    /// Returns the clock's current wall time.
    pub(crate) fn system_time(&self) -> SystemTime {
        self.lock().system_time
    }

    /// Returns how far the clock has advanced since its creation.
    pub(crate) fn advanced(&self) -> Duration {
        self.lock().now - self.start
    }

    /// Registers a waiter's signal before its first generation check.
    pub(crate) fn register(&self, signal: &Arc<Signal>) {
        self.lock()
            .signals
            .insert(Arc::as_ptr(signal) as usize, Arc::downgrade(signal));
    }

    /// Removes a waiter's signal without retaining storage for dead waiters.
    pub(crate) fn unregister(&self, signal: &Arc<Signal>) {
        self.lock().signals.remove(&(Arc::as_ptr(signal) as usize));
    }

    /// Counts a park until its guard drops, while the caller holds its signal lock.
    pub(crate) fn block(&self) -> Blocked<'_> {
        self.lock().blocked += 1;
        self.changed.notify_all();
        Blocked { paused: self }
    }

    /// Runs a test hook without holding any clock or signal lock.
    #[cfg(test)]
    pub(crate) fn check_park(&self, timer: Option<Instant>) {
        let hook = self.before_park.lock().unwrap().take();
        if let Some(hook) = hook {
            hook(timer);
        }
    }

    /// Locks the state, recovering it from poisoning, since no update under the
    /// lock can stop halfway.
    fn lock(&self) -> MutexGuard<'_, PausedState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Counts one parked thread until its signal lock is retaken after waking.
pub(crate) struct Blocked<'a> {
    /// Clock whose parked count includes this thread.
    paused: &'a Paused,
}

impl Drop for Blocked<'_> {
    /// Removes this thread from the clock's parked count.
    fn drop(&mut self) {
        self.paused.lock().blocked -= 1;
    }
}

/// Pauses a test sleep before parking and exposes its real timer, if any.
#[cfg(test)]
type BeforePark = Box<dyn FnOnce(Option<Instant>) + Send>;

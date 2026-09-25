// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Paused clocks, which move only when a test advances them.

use crate::{Clock, Signal};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

#[cfg_attr(docsrs, doc(cfg(feature = "test-clock")))]
impl Clock {
    /// Creates a paused clock, set to the current real time. It moves only
    /// when [`Clock::advance`] or [`Clock::advance_to`] moves it.
    pub fn paused() -> Self {
        let now = Instant::now();
        Self {
            paused: Some(Arc::new(Paused {
                start: now,
                state: Mutex::new(PausedState {
                    now,
                    signals: Vec::new(),
                }),
            })),
        }
    }

    /// Moves a paused clock forward by `by` and wakes all its waiters. It
    /// returns once they are notified, without waiting for them to act. A zero
    /// step does nothing.
    ///
    /// # Panics
    ///
    /// Panics on a real clock, or if the new time overflows [`Instant`]. The time
    /// is unchanged after a panic.
    pub fn advance(&self, by: Duration) {
        self.advance_with(|now| {
            now.checked_add(by)
                .expect("clock advance overflows Instant")
        });
    }

    /// Moves a paused clock forward to `target` and wakes all its waiters. It
    /// returns once they are notified, without waiting for them to act.
    /// Advancing to the current time does nothing.
    ///
    /// # Panics
    ///
    /// Panics on a real clock, or if `target` is earlier than the current time.
    /// The time is unchanged after a panic.
    pub fn advance_to(&self, target: Instant) {
        self.advance_with(|now| {
            assert!(target >= now, "clock cannot go backwards");
            target
        });
    }

    /// Moves a paused clock to the time `next` picks from the current one, then
    /// wakes its waiters. `next` panics on a bad step before anything changes.
    fn advance_with(&self, next: impl FnOnce(Instant) -> Instant) {
        let Some(paused) = &self.paused else {
            panic!("real clock cannot be advanced");
        };
        let mut state = paused.lock();
        let now = next(state.now);
        if now == state.now {
            return;
        }
        state.now = now;
        let mut signals = Vec::with_capacity(state.signals.len());
        state.signals.retain(|signal| match signal.upgrade() {
            Some(signal) => {
                signals.push(signal);
                true
            }
            None => false,
        });
        drop(state);

        // Wake outside the clock lock, so woken threads can read the time at once
        for signal in signals {
            signal.notify_all();
        }
    }
}

impl PartialEq for Clock {
    /// Compares identity. Real clocks are all equal; a paused clock equals only
    /// its own clones.
    fn eq(&self, other: &Self) -> bool {
        match (&self.paused, &other.paused) {
            (None, None) => true,
            (Some(ours), Some(theirs)) => Arc::ptr_eq(ours, theirs),
            _ => false,
        }
    }
}

impl Eq for Clock {}

/// A paused clock's state, shared by its clones and its waiters.
pub(crate) struct Paused {
    /// Time the clock started at, to show how far it has advanced.
    start: Instant,
    /// Current time and the waiters to wake when it moves.
    state: Mutex<PausedState>,
}

/// Mutable part of a paused clock.
struct PausedState {
    /// Current time of the clock.
    now: Instant,
    /// Signals of the clock's waiters. Entries of dropped waiters are pruned
    /// when advancing and before the list grows.
    signals: Vec<Weak<Signal>>,
}

impl Paused {
    /// Returns the clock's current time.
    pub(crate) fn now(&self) -> Instant {
        self.lock().now
    }

    /// Returns how far the clock has advanced since its creation.
    pub(crate) fn advanced(&self) -> Duration {
        self.lock().now - self.start
    }

    /// Registers a waiter's signal to wake on every advance.
    pub(crate) fn register(&self, signal: &Arc<Signal>) {
        let mut state = self.lock();
        // Prune dropped waiters before the list grows, so it tracks the live ones
        if state.signals.len() == state.signals.capacity() {
            state.signals.retain(|signal| signal.strong_count() > 0);
        }
        state.signals.push(Arc::downgrade(signal));
    }

    /// Locks the state. An advance panics before it writes anything, so a lock
    /// poisoned by that panic still guards a consistent state.
    fn lock(&self) -> MutexGuard<'_, PausedState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

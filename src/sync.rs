// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Mutexes and condition variables whose waits follow a clock.
//!
//! Both mirror std's. A condvar takes its clock when created and waits on this
//! module's mutex, which otherwise works like std's.
//!
//! Wait for a condition until a deadline, checking it again after every return:
//!
//! ```
//! # #[cfg(feature = "test-clock")] {
//! use darkbio_clock::TestClock;
//! use darkbio_clock::sync::{Condvar, Mutex};
//! use std::thread;
//! use std::time::Duration;
//!
//! let mut tester = TestClock::new();
//! let clock = tester.clock();
//! let (ready, condvar) = (Mutex::new(false), Condvar::new(&clock));
//! let deadline = clock.now() + Duration::from_secs(5);
//!
//! thread::scope(|scope| {
//!     let worker = scope.spawn(|| {
//!         let mut guard = ready.lock().unwrap();
//!         while !*guard {
//!             let (next, result) = condvar.wait_deadline(guard, deadline).unwrap();
//!             guard = next;
//!             if result.timed_out() {
//!                 return false;
//!             }
//!         }
//!         true
//!     });
//!     tester.wait_blocked(1);
//!     tester.advance(Duration::from_secs(5));
//!     assert!(!worker.join().unwrap());
//! });
//! # }
//! ```

use crate::{Clock, Waiter, primitives};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{LockResult, PoisonError, TryLockError, TryLockResult};
use std::time::Instant;

/// A mutual exclusion lock that this module's condvars can wait on.
///
/// It wraps std's mutex, keeping its locking and poisoning.
pub struct Mutex<T: ?Sized> {
    /// The wrapped std mutex, or loom's under model checking, which holds the value.
    inner: primitives::Mutex<T>,
}

impl<T> Mutex<T> {
    /// Creates an unlocked mutex holding `value`.
    #[cfg(not(all(test, loom)))]
    pub const fn new(value: T) -> Self {
        Self {
            inner: primitives::Mutex::new(value),
        }
    }

    /// Creates an unlocked mutex holding `value`, without the `const` that
    /// loom's mutex cannot offer.
    #[cfg(all(test, loom))]
    pub fn new(value: T) -> Self {
        Self {
            inner: primitives::Mutex::new(value),
        }
    }

    /// Consumes the mutex and returns its value, inside an error if the mutex
    /// is poisoned.
    pub fn into_inner(self) -> LockResult<T> {
        self.inner.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Blocks until the mutex is free, then locks it.
    ///
    /// If a thread panicked while holding the mutex, the guard comes back
    /// inside an error.
    pub fn lock(&self) -> LockResult<MutexGuard<'_, T>> {
        match self.inner.lock() {
            Ok(inner) => Ok(MutexGuard { inner, mutex: self }),
            Err(err) => Err(PoisonError::new(MutexGuard {
                inner: err.into_inner(),
                mutex: self,
            })),
        }
    }

    /// Locks the mutex if it is free, without blocking.
    ///
    /// Fails with `WouldBlock` while the mutex is locked. If a thread panicked
    /// while holding the mutex, the guard comes back inside `Poisoned`.
    pub fn try_lock(&self) -> TryLockResult<MutexGuard<'_, T>> {
        match self.inner.try_lock() {
            Ok(inner) => Ok(MutexGuard { inner, mutex: self }),
            Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
            Err(TryLockError::Poisoned(err)) => {
                Err(TryLockError::Poisoned(PoisonError::new(MutexGuard {
                    inner: err.into_inner(),
                    mutex: self,
                })))
            }
        }
    }

    /// Reports whether a thread panicked while holding the mutex.
    #[cfg(not(all(test, loom)))]
    pub fn is_poisoned(&self) -> bool {
        self.inner.is_poisoned()
    }

    /// Clears the poisoned state, marking the value as recovered.
    #[cfg(not(all(test, loom)))]
    pub fn clear_poison(&self) {
        self.inner.clear_poison();
    }

    /// Borrows the value mutably, inside an error if the mutex is poisoned.
    ///
    /// The mutable borrow of the mutex rules out other users, so this takes
    /// no lock.
    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        self.inner.get_mut()
    }
}

impl<T: Default> Default for Mutex<T> {
    /// Creates an unlocked mutex holding the value type's default.
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    /// Creates an unlocked mutex holding `value`.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    /// Shows the value if the mutex is free, never blocking, as std does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// Exclusive access to a mutex's value, which unlocks the mutex on drop.
///
/// Like std's guard, it cannot be sent to another thread.
#[must_use = "if unused the Mutex will immediately unlock"]
pub struct MutexGuard<'a, T: ?Sized + 'a> {
    /// The wrapped guard, which unlocks the mutex and poisons it on a panic.
    inner: primitives::MutexGuard<'a, T>,
    /// The mutex a condvar wait relocks.
    mutex: &'a Mutex<T>,
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    /// The value the mutex protects.
    type Target = T;

    /// Borrows the value.
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    /// Borrows the value mutably.
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    /// Formats the value, as std's guard does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    /// Formats the value, as std's guard does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

/// A condition variable whose deadline waits follow a clock.
///
/// It mirrors std's, except that it takes its clock when created, and its
/// timed wait takes an absolute deadline. A wait releases the mutex while it
/// blocks and relocks it before returning. Notifications are not buffered, and
/// a wait may also return spuriously, so callers recheck their condition after
/// every return.
pub struct Condvar {
    /// Parks and wakes this condvar's waits, registered with a test clock.
    waiter: Waiter,
}

impl Condvar {
    /// Creates a condition variable whose deadlines are read from `clock`.
    pub fn new(clock: &Clock) -> Self {
        Self {
            waiter: clock.waiter(),
        }
    }

    /// Releases the mutex and blocks until notified, then relocks it.
    ///
    /// If a thread panicked while holding the mutex, the relocked guard comes
    /// back inside an error.
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> LockResult<MutexGuard<'a, T>> {
        self.wait_inner(guard, None)
    }

    /// Blocks while `condition` holds, checking it with the mutex locked.
    ///
    /// Returns with the mutex locked once `condition` is false. If a thread
    /// panicked while holding the mutex, the relocked guard comes back inside
    /// an error.
    pub fn wait_while<'a, T, F>(
        &self,
        mut guard: MutexGuard<'a, T>,
        mut condition: F,
    ) -> LockResult<MutexGuard<'a, T>>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut *guard) {
            guard = self.wait(guard)?;
        }
        Ok(guard)
    }

    /// Releases the mutex and blocks until notified or until the clock reaches
    /// `deadline`, then relocks it.
    ///
    /// A deadline already reached returns at once. The result reports whether
    /// the clock had reached the deadline once the mutex was relocked. If a
    /// thread panicked while holding the mutex, the guard and the result come
    /// back inside an error.
    pub fn wait_deadline<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        deadline: Instant,
    ) -> LockResult<(MutexGuard<'a, T>, WaitTimeoutResult)> {
        // Decide expiry by the clock after the relock, never by how the wait ended
        let guard = self.wait_inner(guard, Some(deadline));
        let result = WaitTimeoutResult(self.waiter.clock.now() >= deadline);

        // Hand back the guard and the result even from a poisoned mutex
        match guard {
            Ok(guard) => Ok((guard, result)),
            Err(err) => Err(PoisonError::new((err.into_inner(), result))),
        }
    }

    /// Wakes one thread waiting on this condvar, if any.
    pub fn notify_one(&self) {
        self.waiter.signal.notify_one();
    }

    /// Wakes every thread waiting on this condvar.
    pub fn notify_all(&self) {
        self.waiter.signal.notify_all();
    }

    /// Waits with the mutex released until notified or until the clock reaches
    /// `deadline`, then relocks it.
    fn wait_inner<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        deadline: Option<Instant>,
    ) -> LockResult<MutexGuard<'a, T>> {
        // Pick the real timer once, so the test seam reports what every park uses
        let timer = self.waiter.timer(deadline);

        // Start the wait under the caller's mutex, so that a notification sent
        // after the caller's last check cannot slip past it
        let MutexGuard { inner, mutex } = guard;
        let mut state = self.waiter.signal.lock();
        let notifications = state.notifications;
        #[cfg(test)]
        if let Some(hook) = self
            .waiter
            .clock
            .paused
            .as_ref()
            .and_then(|paused| paused.take_hook(false))
        {
            // Let tests act while the caller's mutex is still held
            drop(state);
            hook(timer);
            state = self.waiter.signal.lock();
        }

        // Release the caller's mutex, keeping the signal locked until the park
        drop(inner);

        // Park until notified or the deadline passes, parking again after any
        // advance short of it
        loop {
            let seen = state.generation;
            if state.notifications != notifications
                || deadline.is_some_and(|deadline| self.waiter.clock.now() >= deadline)
            {
                break;
            }
            state = self.waiter.park(state, seen, deadline, timer);
        }

        // Unlock the signal first, since notifiers take the two locks the other way round
        drop(state);
        mutex.lock()
    }
}

impl fmt::Debug for Condvar {
    /// Shows the clock, never the wakeup bookkeeping.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condvar")
            .field("clock", &self.waiter.clock)
            .finish_non_exhaustive()
    }
}

/// Whether a deadline wait's clock had reached the deadline when it returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitTimeoutResult(
    /// Whether the clock had reached the deadline once the mutex was relocked.
    bool,
);

impl WaitTimeoutResult {
    /// Returns whether the clock had reached the deadline once the mutex was
    /// relocked.
    pub fn timed_out(&self) -> bool {
        self.0
    }
}

// The tests live in src/tests, loaded from here so that they keep this
// module's private items in reach
#[cfg(all(test, not(loom)))]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "tests/sync.rs"]
mod tests;

// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Mutexes and condition variables whose waits follow a clock.
//!
//! Both mirror std's. A condvar takes its clock when created and waits on this
//! module's mutex, which otherwise works like std's.

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

/// Checks std parity for the mutex, and condvar waits that follow a clock,
/// without timed polling.
#[cfg(all(test, not(loom)))]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::TestClock;
    use crate::tests::{blocked, pause_before_park};
    use std::sync::{self, Arc, mpsc};
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    /// Poisons a mutex by panicking on another thread while `lock`'s guard is held.
    fn poison<G>(lock: impl FnOnce() -> G + Send) {
        thread::scope(|scope| {
            let panicked = scope
                .spawn(|| {
                    let _guard = lock();
                    panic!("poison the mutex");
                })
                .join();
            assert!(panicked.is_err());
        });
    }

    /// Starts a thread running `wait` with the pair's mutex locked, and returns
    /// once the wait has released the mutex.
    fn start_wait<T, R>(
        pair: &Arc<(Mutex<T>, Condvar)>,
        wait: impl for<'a> FnOnce(&'a Condvar, MutexGuard<'a, T>) -> R + Send + 'static,
    ) -> JoinHandle<R>
    where
        T: Send + 'static,
        R: Send + 'static,
    {
        // Lock the mutex on the new thread and announce it before waiting
        let (locked, locks) = mpsc::sync_channel(0);
        let waiting = thread::spawn({
            let pair = pair.clone();
            move || {
                let guard = pair.0.lock().unwrap();
                locked.send(()).unwrap();
                wait(&pair.1, guard)
            }
        });

        // The mutex frees up only once the wait has begun, so notifications reach it
        locks.recv().unwrap();
        drop(pair.0.lock().unwrap());
        waiting
    }

    /// Blocks until no thread is parked on the clock, catching a woken wait
    /// before it relocks its mutex.
    fn wait_unblocked(clock: &Clock) {
        let paused = clock.paused.as_ref().unwrap();
        let mut state = paused.state.lock().unwrap();
        while state.blocked != 0 {
            state = paused.changed.wait(state).unwrap();
        }
    }

    // A held mutex reports contention, and dropping its guard lets another lock through.
    #[test]
    fn test_mutex_try_lock_reports_contention() {
        // Change the value under a guard and try taking the same lock again
        let mutex = Mutex::new(3);
        let mut guard = mutex.lock().unwrap();
        *guard = 7;
        assert!(matches!(mutex.try_lock(), Err(TryLockError::WouldBlock)));

        // Dropping the guard unlocks the mutex and keeps the change
        drop(guard);
        assert_eq!(*mutex.try_lock().unwrap(), 7);
    }

    // A poisoned mutex hands its guard out inside errors until the poison is cleared.
    #[test]
    fn test_mutex_poisoning_preserves_guard_and_recovers() {
        // Panic under the lock to poison the mutex
        let mutex = Mutex::new(3);
        poison(|| mutex.lock().unwrap());
        assert!(mutex.is_poisoned());

        // Both lock and try_lock return a usable guard inside their errors
        let mut guard = mutex.lock().unwrap_err().into_inner();
        assert_eq!(*guard, 3);
        *guard = 7;
        drop(guard);
        match mutex.try_lock() {
            Err(TryLockError::Poisoned(err)) => assert_eq!(*err.into_inner(), 7),
            result => panic!("expected a poison error, got {result:?}"),
        }

        // Clearing the poison keeps the repaired value and restores plain locking
        mutex.clear_poison();
        assert!(!mutex.is_poisoned());
        assert_eq!(*mutex.lock().unwrap(), 7);
    }

    // Exclusive and consuming access work on healthy and poisoned mutexes alike.
    #[test]
    fn test_mutex_accessors_preserve_poisoned_values() {
        // A healthy mutex gives up its value without locking
        let mut healthy = Mutex::<Vec<u8>>::default();
        healthy.get_mut().unwrap().push(7);
        assert_eq!(healthy.into_inner().unwrap(), vec![7]);

        // A poisoned one still does, inside errors, and stays poisoned
        let mut poisoned = Mutex::from(vec![3]);
        poison(|| poisoned.lock().unwrap());
        poisoned.get_mut().unwrap_err().into_inner().push(7);
        assert!(poisoned.is_poisoned());
        assert_eq!(poisoned.into_inner().unwrap_err().into_inner(), vec![3, 7]);
    }

    // Mutexes and guards format like std's, without blocking, even when held or poisoned.
    #[test]
    fn test_mutex_and_guards_format_like_std_without_blocking() {
        // Compare a free mutex with std's
        let mutex = Mutex::new(7);
        let standard = sync::Mutex::new(7);
        assert_eq!(format!("{mutex:?}"), format!("{standard:?}"));

        // Held mutexes format without waiting for their guards, which show the value
        let guard = mutex.lock().unwrap();
        let standard_guard = standard.lock().unwrap();
        assert_eq!(format!("{mutex:?}"), format!("{standard:?}"));
        assert_eq!(format!("{guard:?}"), format!("{standard_guard:?}"));
        assert_eq!(format!("{guard}"), format!("{standard_guard}"));
        drop((guard, standard_guard));

        // Poisoned mutexes still show their value
        poison(|| mutex.lock().unwrap());
        poison(|| standard.lock().unwrap());
        assert_eq!(format!("{mutex:?}"), format!("{standard:?}"));
    }

    // Two notify_one calls, or one notify_all, end two waits on either kind of clock.
    #[test]
    fn test_notifications_end_waits() {
        for all in [false, true] {
            for clock in [Clock::real(), TestClock::new().clock()] {
                // Start two untimed waits that report when they return
                let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
                let (done, returned) = mpsc::channel();
                let threads: Vec<_> = (0..2)
                    .map(|_| {
                        let done = done.clone();
                        start_wait(&pair, move |condvar, guard| {
                            drop(condvar.wait(guard).unwrap());
                            done.send(()).unwrap();
                        })
                    })
                    .collect();

                // One notify_one per wait, or a single notify_all, lets both return
                if all {
                    pair.1.notify_all();
                } else {
                    for _ in 0..2 {
                        pair.1.notify_one();
                        returned.recv().unwrap();
                    }
                }
                for waiting in threads {
                    waiting.join().unwrap();
                }
            }
        }
    }

    // A notification makes wait_while check again, and it returns once the check passes.
    #[test]
    fn test_wait_while_rechecks_predicate_after_notifications() {
        // Start a wait for a gate to open, reporting every check of the gate
        let test = TestClock::new();
        let pair = Arc::new((Mutex::new(false), Condvar::new(&test.clock())));
        let (checked, checks) = mpsc::channel();
        let waiting = start_wait(&pair, move |condvar, guard| {
            let guard = condvar
                .wait_while(guard, |open| {
                    checked.send(*open).unwrap();
                    !*open
                })
                .unwrap();
            assert!(*guard);
        });
        assert!(!checks.recv().unwrap());
        test.wait_blocked(1);

        // A notification with the gate still closed leads to another check and park
        pair.1.notify_one();
        assert!(!checks.recv().unwrap());
        test.wait_blocked(1);

        // Opening the gate under the mutex lets the next notification end the wait
        *pair.0.lock().unwrap() = true;
        pair.1.notify_one();
        assert!(checks.recv().unwrap());
        waiting.join().unwrap();
    }

    // An advance sends an untimed wait back to park, and only a notification ends it.
    #[test]
    fn test_untimed_wait_parks_again_after_advances() {
        // Park an untimed wait on a test clock
        let mut test = TestClock::new();
        let clock = test.clock();
        let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
        let waiting = start_wait(&pair, |condvar, guard| drop(condvar.wait(guard).unwrap()));
        test.wait_blocked(1);

        // Catch the wait on its way back to park after an advance
        let (checked, resume) = pause_before_park(&clock);
        test.advance(Duration::from_secs(60));
        assert_eq!(checked.recv().unwrap(), None);
        resume.send(()).unwrap();

        // Parked again, it returns on a notification
        test.wait_blocked(1);
        pair.1.notify_one();
        waiting.join().unwrap();
    }

    // A deadline wait outlasts short advances and times out exactly at its deadline.
    #[test]
    fn test_wait_deadline_reparks_until_exact_deadline() {
        // Park a wait on a deadline taken from its own clock
        let mut test = TestClock::new();
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(5);
        let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
        let waiting = start_wait(&pair, move |condvar, guard| {
            condvar.wait_deadline(guard, deadline).unwrap().1
        });
        test.wait_blocked(1);
        assert_eq!(test.next_deadline(), Some(deadline));

        // Catch the wait between parks, holding no real timer and no listed deadline
        let (checked, resume) = pause_before_park(&clock);
        test.advance(Duration::from_secs(2));
        assert_eq!(checked.recv().unwrap(), None);
        assert_eq!(test.next_deadline(), None);
        assert_eq!(blocked(&clock), 0);
        resume.send(()).unwrap();
        test.wait_blocked(1);
        assert_eq!(test.next_deadline(), Some(deadline));

        // Reaching the deadline ends the wait, timed out, with no notification
        test.advance_to(deadline);
        assert!(waiting.join().unwrap().timed_out());
        assert_eq!(clock.now(), deadline);
        assert_eq!(test.next_deadline(), None);
    }

    // A notification ends a deadline wait early, not timed out, on either kind of clock.
    #[test]
    fn test_wait_deadline_returns_early_on_notification() {
        let test = TestClock::new();
        for clock in [Clock::real(), test.clock()] {
            // Start a wait whose deadline is a day away
            let deadline = clock.now() + Duration::from_secs(86400);
            let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
            let waiting = start_wait(&pair, move |condvar, guard| {
                condvar.wait_deadline(guard, deadline).unwrap().1
            });

            // A notification ends it long before then, leaving no deadline listed
            pair.1.notify_one();
            assert!(!waiting.join().unwrap().timed_out(), "{clock:?}");
            assert_eq!(test.next_deadline(), None, "{clock:?}");
        }
    }

    // Deadlines already reached return at once, timed out, without parking.
    #[test]
    fn test_wait_deadline_returns_for_reached_deadlines() {
        // Move a test clock past a known instant, which is in the past for both clocks
        let mut test = TestClock::new();
        let past = test.clock().now();
        test.advance(Duration::from_secs(1));

        // A past and the current deadline both return at once, timed out, with the guard
        for clock in [Clock::real(), test.clock()] {
            let mutex = Mutex::new(7);
            let condvar = Condvar::new(&clock);
            for deadline in [past, clock.now()] {
                let (guard, result) = condvar
                    .wait_deadline(mutex.lock().unwrap(), deadline)
                    .unwrap();
                assert!(result.timed_out(), "{clock:?} {deadline:?}");
                assert_eq!(*guard, 7, "{clock:?} {deadline:?}");
            }
            assert_eq!(condvar.waiter.signal.lock().parks, 0, "{clock:?}");
        }
    }

    // An advance to the deadline before the first park ends the wait, with no overshoot.
    #[test]
    fn test_wait_deadline_observes_advance_before_parking() {
        // Stop a wait after it takes the caller's deadline, before it can park
        let mut test = TestClock::new();
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(5);
        let (checked, resume) = pause_before_park(&clock);
        let waiting = thread::spawn({
            let clock = clock.clone();
            move || {
                let condvar = Condvar::new(&clock);
                let mutex = Mutex::new(());
                condvar
                    .wait_deadline(mutex.lock().unwrap(), deadline)
                    .unwrap()
                    .1
            }
        });
        assert_eq!(checked.recv().unwrap(), None);

        // Advance exactly to the deadline while nothing is parked
        assert_eq!(test.next_deadline(), None);
        test.advance_to(deadline);
        resume.send(()).unwrap();

        // The wait returns at this time instead of waiting for another advance
        assert!(waiting.join().unwrap().timed_out());
        assert_eq!(clock.now(), deadline);
        assert_eq!(test.next_deadline(), None);
    }

    // A deadline worker picks up an earlier deadline and times out at it.
    #[test]
    fn test_deadline_worker_recomputes_earliest_deadline() {
        // Start a worker on the earliest of a set of deadlines, reporting each pick
        let mut test = TestClock::new();
        let clock = test.clock();
        let later = clock.now() + Duration::from_secs(10);
        let earlier = clock.now() + Duration::from_secs(3);
        let pair = Arc::new((Mutex::new(vec![later]), Condvar::new(&clock)));
        let (picked, picks) = mpsc::channel();
        let waiting = start_wait(&pair, move |condvar, mut guard| {
            loop {
                // Pick the earliest deadline again after every return from the condvar
                let deadline = *guard.iter().min().unwrap();
                picked.send(deadline).unwrap();
                let (next, result) = condvar.wait_deadline(guard, deadline).unwrap();
                guard = next;

                // Stop once the clock has reached the picked deadline
                if result.timed_out() {
                    return deadline;
                }
            }
        });
        assert_eq!(picks.recv().unwrap(), later);
        test.wait_blocked(1);
        assert_eq!(test.next_deadline(), Some(later));

        // Publish an earlier deadline under the mutex and notify the worker
        pair.0.lock().unwrap().push(earlier);
        pair.1.notify_one();
        assert_eq!(picks.recv().unwrap(), earlier);
        test.wait_blocked(1);
        assert_eq!(test.next_deadline(), Some(earlier));

        // Advancing to the new deadline alone ends the worker
        test.advance_to(earlier);
        assert_eq!(waiting.join().unwrap(), earlier);
        assert_eq!(test.next_deadline(), None);
    }

    // A notification between the caller's check and the park is not lost.
    #[test]
    fn test_notification_between_predicate_and_park_is_observed() {
        // Stop the first wait after its predicate runs, before it parks
        let test = TestClock::new();
        let clock = test.clock();
        let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
        let (checked, resume) = pause_before_park(&clock);
        let waiting = thread::spawn({
            let pair = pair.clone();
            move || {
                let mut checks = 0;
                drop(
                    pair.1
                        .wait_while(pair.0.lock().unwrap(), |_| {
                            checks += 1;
                            checks == 1
                        })
                        .unwrap(),
                );
                checks
            }
        });
        assert_eq!(checked.recv().unwrap(), None);

        // The caller still holds its mutex, and a notification now must not be lost
        assert!(matches!(pair.0.try_lock(), Err(TryLockError::WouldBlock)));
        pair.1.notify_one();
        resume.send(()).unwrap();

        // The wait returns without parking, and the predicate runs again
        assert_eq!(waiting.join().unwrap(), 2);
        assert_eq!(pair.1.waiter.signal.lock().parks, 0);
    }

    // Waits return their guard inside an error when the mutex was poisoned meanwhile.
    #[test]
    fn test_wait_relocks_poisoned_mutex() {
        for timed in [false, true] {
            // Park a wait on a healthy mutex
            let test = TestClock::new();
            let deadline = test.clock().now() + Duration::from_secs(5);
            let pair = Arc::new((Mutex::new(7), Condvar::new(&test.clock())));
            let waiting = start_wait(&pair, move |condvar, guard| {
                if timed {
                    let (guard, result) = condvar
                        .wait_deadline(guard, deadline)
                        .unwrap_err()
                        .into_inner();
                    assert!(!result.timed_out());
                    *guard
                } else {
                    *condvar.wait(guard).unwrap_err().into_inner()
                }
            });
            test.wait_blocked(1);

            // Poison the mutex while the wait has it released, then notify the wait
            poison(|| pair.0.lock().unwrap());
            pair.1.notify_one();

            // The wait still hands back the guard, and leaves no deadline listed
            assert_eq!(waiting.join().unwrap(), 7, "{timed}");
            assert!(pair.0.is_poisoned(), "{timed}");
            assert_eq!(test.next_deadline(), None, "{timed}");
        }
    }

    // A woken wait unlists its deadline at once, while its result reads the clock
    // after the relock.
    #[test]
    fn test_timeout_result_uses_clock_after_relocking() {
        // Park a timed wait
        let mut test = TestClock::new();
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(5);
        let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
        let waiting = start_wait(&pair, move |condvar, guard| {
            condvar.wait_deadline(guard, deadline).unwrap().1
        });
        test.wait_blocked(1);

        // Notify it while holding the mutex, so it wakes and then waits to relock
        let guard = pair.0.lock().unwrap();
        pair.1.notify_one();
        wait_unblocked(&clock);
        assert_eq!(test.next_deadline(), None);

        // Reach the deadline before the relock, so the notified wait reports a timeout
        test.advance_to(deadline);
        drop(guard);
        assert!(waiting.join().unwrap().timed_out());
    }

    // Parked sleeps and deadline waits, never untimed waits, list their deadlines
    // until their parks end.
    #[test]
    fn test_next_deadline_tracks_only_parked_timed_waits() {
        // An untimed wait lists no deadline
        let mut test = TestClock::new();
        let clock = test.clock();
        let start = clock.now();
        assert_eq!(test.next_deadline(), None);
        let untimed = Arc::new((Mutex::new(()), Condvar::new(&clock)));
        let untimed_thread = start_wait(&untimed, |condvar, guard| {
            drop(condvar.wait(guard).unwrap())
        });
        test.wait_blocked(1);
        assert_eq!(test.next_deadline(), None);

        // Park both sleeps and two deadline waits, one sharing its deadline with a sleep
        let sleep_thread = thread::spawn({
            let clock = clock.clone();
            move || clock.sleep(Duration::from_secs(3))
        });
        let until_thread = thread::spawn({
            let clock = clock.clone();
            move || clock.sleep_until(start + Duration::from_secs(7))
        });
        let waits: Vec<_> = [1, 3]
            .into_iter()
            .map(|seconds| {
                let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
                let deadline = start + Duration::from_secs(seconds);
                let waiting = start_wait(&pair, move |condvar, guard| {
                    condvar.wait_deadline(guard, deadline).unwrap().1
                });
                (pair, waiting)
            })
            .collect();
        test.wait_blocked(5);
        assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(1)));

        // Ending the earliest wait exposes the deadline its sibling shares with a sleep
        let mut waits = waits.into_iter();
        let (pair, waiting) = waits.next().unwrap();
        pair.1.notify_one();
        assert!(!waiting.join().unwrap().timed_out());
        assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(3)));

        // Ending the sibling leaves the sleep's entry at the same instant
        let (pair, waiting) = waits.next().unwrap();
        pair.1.notify_one();
        assert!(!waiting.join().unwrap().timed_out());
        assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(3)));

        // End the untimed wait, so that only the last sleep parks again after the advance
        untimed.1.notify_one();
        untimed_thread.join().unwrap();

        // Catch the last sleep on its way back to park, once the first sleep has returned
        let (checked, resume) = pause_before_park(&clock);
        test.advance(Duration::from_secs(3));
        sleep_thread.join().unwrap();
        assert_eq!(checked.recv().unwrap(), None);
        assert_eq!(test.next_deadline(), None);
        resume.send(()).unwrap();
        test.wait_blocked(1);
        assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(7)));

        // Ending the last sleep leaves no deadline behind
        test.advance_to(start + Duration::from_secs(7));
        until_thread.join().unwrap();
        assert_eq!(test.next_deadline(), None);
    }

    // Dropping a condvar ends its registration with the clock.
    #[test]
    fn test_condvar_registry_tracks_lifetime() {
        let test = TestClock::new();
        let clock = test.clock();
        let condvar = Condvar::new(&clock);
        let paused = clock.paused.as_ref().unwrap();
        assert_eq!(paused.state.lock().unwrap().signals.len(), 1);
        drop(condvar);
        assert!(paused.state.lock().unwrap().signals.is_empty());
    }

    // A real-clock deadline wait expires unnotified, with no upper bound on its duration.
    #[test]
    fn test_real_wait_deadline_expires() {
        // Wait a millisecond on a condvar nobody notifies
        let clock = Clock::real();
        let condvar = Condvar::new(&clock);
        let mutex = Mutex::new(());
        let deadline = clock.now() + Duration::from_millis(1);
        let (guard, result) = condvar
            .wait_deadline(mutex.lock().unwrap(), deadline)
            .unwrap();

        // The result agrees with the clock after the relock
        assert!(result.timed_out());
        assert!(clock.now() >= deadline);
        drop(guard);
    }
}

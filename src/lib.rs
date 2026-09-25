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
// The crate only builds on std's safe synchronization and never needs unsafe
#![forbid(unsafe_code)]

// Pausing is test-only, so a build without the test-clock feature cannot stop its own time
#[cfg(any(test, feature = "test-clock"))]
mod paused;

use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

/// A monotonic clock, either the real one or a paused one that a test
/// advances by hand.
///
/// Clones share one clock. Equality is identity, not the current time, so
/// all real clocks are equal and a paused clock equals only its own clones.
#[derive(Clone)]
// Without pausing every clock is the real one, so all are equal; paused.rs compares identity
#[cfg_attr(not(any(test, feature = "test-clock")), derive(PartialEq, Eq))]
pub struct Clock {
    /// Shared state of a paused clock, or nothing for the real clock.
    #[cfg(any(test, feature = "test-clock"))]
    paused: Option<Arc<paused::Paused>>,
}

impl Clock {
    /// Returns the real monotonic clock, which reads [`Instant::now`].
    pub fn real() -> Self {
        Self {
            #[cfg(any(test, feature = "test-clock"))]
            paused: None,
        }
    }

    /// Returns the clock's current time.
    pub fn now(&self) -> Instant {
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return paused.now();
        }
        Instant::now()
    }

    /// Creates a waiter that measures deadlines on this clock. A paused clock
    /// wakes it on every advance.
    pub fn waiter(&self) -> Waiter {
        let signal = Arc::new(Signal::default());
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            paused.register(&signal);
        }
        Waiter {
            clock: self.clone(),
            signal,
        }
    }
}

impl fmt::Debug for Clock {
    /// Shows whether the clock is paused and, if so, how far it has advanced.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut clock = f.debug_struct("Clock");
        #[cfg(any(test, feature = "test-clock"))]
        if let Some(paused) = &self.paused {
            return clock
                .field("paused", &true)
                .field("advanced", &paused.advanced())
                .finish();
        }
        clock.field("paused", &false).finish()
    }
}

/// Blocks threads until a condition holds or a deadline passes on a clock.
///
/// Clones share one waiter, so threads can wait on it while others notify it.
/// Code that changes the state a condition reads calls [`Waiter::notify_all`]
/// after the change.
///
/// ```
/// use darkbio_clock::Clock;
/// use std::sync::{Arc, Mutex};
/// use std::thread;
///
/// let waiter = Clock::real().waiter();
/// let slot = Arc::new(Mutex::new(None));
/// let taker = thread::spawn({
///     let (waiter, slot) = (waiter.clone(), slot.clone());
///     move || waiter.wait_until(None, || slot.lock().unwrap().take())
/// });
/// *slot.lock().unwrap() = Some(42);
/// waiter.notify_all();
/// assert_eq!(taker.join().unwrap(), Some(42));
/// ```
#[derive(Clone)]
pub struct Waiter {
    /// Clock that decides when deadlines pass.
    clock: Clock,
    /// Wakeups shared by all clones, also registered with a paused clock.
    signal: Arc<Signal>,
}

impl Waiter {
    /// Wakes every thread waiting on this waiter to check its condition again.
    /// Call it after each change that can make a condition ready, with or
    /// without holding the lock that change took.
    pub fn notify_all(&self) {
        self.signal.notify_all();
    }

    /// Blocks until `ready` returns a value or the clock reaches `deadline`.
    ///
    /// Every check calls `ready` first and compares the deadline after, so a
    /// value that is ready wins over an expired deadline. The wait returns
    /// `None` when a check finds `ready` empty and the deadline reached. Without
    /// a deadline, only a value ends the wait. A plain delay passes a `ready`
    /// that always returns `None`.
    ///
    /// A check follows each notification and each advance of a paused clock,
    /// and sometimes a wakeup where nothing changed. Changes that land together
    /// share one check, so `ready` can run fewer times than there were changes.
    ///
    /// `ready` runs on the calling thread with no lock of the clock or the waiter
    /// held. It may take its own locks, notify, or advance a clock, and should
    /// return promptly.
    pub fn wait_until<T>(
        &self,
        deadline: Option<Instant>,
        mut ready: impl FnMut() -> Option<T>,
    ) -> Option<T> {
        // A paused clock wakes its waiters when it moves, so only real time needs a timer
        #[cfg(any(test, feature = "test-clock"))]
        let timer = deadline.filter(|_| self.clock.paused.is_none());
        #[cfg(not(any(test, feature = "test-clock")))]
        let timer = deadline;
        loop {
            // Take the generation before checking, so a change after the check cuts the park short
            let seen = self.signal.generation();
            if let Some(value) = ready() {
                return Some(value);
            }
            if deadline.is_some_and(|deadline| self.clock.now() >= deadline) {
                return None;
            }
            self.signal.park(seen, timer);
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

/// Wakes the threads parked on one waiter. Each notification bumps the
/// generation. A thread parks only while the generation it read before its
/// check is current, so no wakeup is lost in between.
#[derive(Default)]
struct Signal {
    /// Generation and parking count, guarded together.
    state: Mutex<SignalState>,
    /// Wakes parked threads when the generation moves.
    changed: Condvar,
}

/// Mutable part of a signal.
#[derive(Default)]
struct SignalState {
    /// Number of notifications and clock advances so far.
    generation: u64,
    /// Parks that found the generation unchanged, for tests to wait on. An
    /// untimed park counted here blocks until the next notification.
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

    /// Bumps the generation and wakes every parked thread.
    fn notify_all(&self) {
        self.lock().generation += 1;
        self.changed.notify_all();
    }

    /// Blocks until the generation moves past `seen`, or until real time
    /// reaches `timer`. The caller rechecks its condition and deadline after.
    fn park(&self, seen: u64, timer: Option<Instant>) {
        let mut state = self.lock();
        #[cfg(test)]
        if state.generation == seen {
            state.parks += 1;
            self.changed.notify_all();
        }
        while state.generation == seen {
            state = match timer {
                None => self
                    .changed
                    .wait(state)
                    .expect("waiter signal not poisoned"),
                Some(timer) => {
                    // Decide expiry by the time, since a wait's own timeout flag is unreliable
                    let now = Instant::now();
                    if now >= timer {
                        break;
                    }
                    self.changed
                        .wait_timeout(state, timer - now)
                        .expect("waiter signal not poisoned")
                        .0
                }
            };
        }
    }
}

/// Checks paused time, and that waits wake for every change they depend on.
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    /// Blocks until parks on the waiter have found the generation unchanged
    /// `count` times in total.
    fn parked(waiter: &Waiter, count: usize) {
        let mut state = waiter.signal.lock();
        while state.parks < count {
            state = waiter.signal.changed.wait(state).unwrap();
        }
    }

    // Clones share one paused clock, which moves only by its own advances.
    // Another paused clock keeps its own time, and equality follows sharing
    // even when the two clocks show the same time.
    #[test]
    fn test_paused_clock_sharing() {
        let other = Clock::paused();
        let clock = Clock::paused();
        let clone = clock.clone();
        let other_start = other.now();
        let start = clock.now();

        clone.advance(Duration::from_secs(1));
        assert_eq!(clock.now(), start + Duration::from_secs(1));
        clock.advance_to(start + Duration::from_secs(3));
        assert_eq!(clone.now(), start + Duration::from_secs(3));
        clock.advance(Duration::ZERO);
        clock.advance_to(start + Duration::from_secs(3));
        assert_eq!(clock.now(), start + Duration::from_secs(3));
        assert_eq!(other.now(), other_start);

        other.advance_to(clock.now());
        assert_eq!(other.now(), clock.now());
        assert_eq!(clock, clone);
        assert_ne!(clock, other);
        assert_ne!(clock, Clock::real());
        assert_eq!(Clock::real(), Clock::real());
    }

    // Advancing a real clock, going backwards and overflowing all panic without
    // moving the paused clock, which stays usable afterwards.
    #[test]
    fn test_failed_advance_keeps_time() {
        let clock = Clock::paused();
        let real = Clock::real();
        clock.advance(Duration::from_secs(1));
        let now = clock.now();

        let misuses: [(&str, &dyn Fn()); 3] = [
            ("real", &|| real.advance(Duration::from_secs(1))),
            ("backwards", &|| {
                clock.advance_to(now - Duration::from_secs(1))
            }),
            ("overflow", &|| clock.advance(Duration::MAX)),
        ];
        for (case, misuse) in misuses {
            assert!(
                panic::catch_unwind(AssertUnwindSafe(misuse)).is_err(),
                "{case}"
            );
            assert_eq!(clock.now(), now, "{case}");
        }
        clock.advance(Duration::from_secs(1));
        assert_eq!(clock.now(), now + Duration::from_secs(1));
    }

    // A ready value wins over an expired deadline, and an expired deadline with
    // nothing ready returns at once. A deadline equal to the current time counts.
    #[test]
    fn test_wait_checks_ready_first() {
        for clock in [Clock::real(), Clock::paused()] {
            let waiter = clock.waiter();
            let now = clock.now();
            assert_eq!(
                waiter.wait_until(Some(now), || Some(1)),
                Some(1),
                "{clock:?}"
            );
            assert_eq!(
                waiter.wait_until(Some(now), || None::<u8>),
                None,
                "{clock:?}"
            );
        }
    }

    // A notification or an advance landing between a failed check and the park
    // is not lost. The wait rechecks each time and returns at its deadline.
    #[test]
    fn test_wait_sees_changes_after_its_check() {
        let clock = Clock::paused();
        let waiter = clock.waiter();
        let deadline = clock.now() + Duration::from_secs(2);
        let mut checks = 0;
        let result = waiter.wait_until(Some(deadline), || {
            checks += 1;
            if checks == 1 {
                waiter.notify_all();
            } else if clock.now() < deadline {
                clock.advance(Duration::from_secs(1));
            }
            None::<()>
        });
        assert_eq!(result, None);
        assert_eq!(clock.now(), deadline);
    }

    // Advancing wakes every parked waiter of the clock, whichever clone made it
    // and whatever waiters were dropped around it. An advance short of the
    // deadline parks them again, and the one reaching the deadline ends them.
    #[test]
    fn test_advance_wakes_parked_waiters() {
        let clock = Clock::paused();
        let deadline = clock.now() + Duration::from_secs(60);
        // Interleave live and dropped waiters in the clock's registrations
        let mut waiters: Vec<_> = (0..4).map(|_| clock.waiter()).collect();
        waiters.truncate(1);
        waiters.push(clock.clone().waiter());
        drop(clock.waiter());

        let threads: Vec<_> = waiters
            .iter()
            .map(|waiter| {
                let waiter = waiter.clone();
                thread::spawn(move || waiter.wait_until(Some(deadline), || None::<()>))
            })
            .collect();
        for waiter in &waiters {
            parked(waiter, 1);
        }
        clock.advance(Duration::from_secs(30));
        for waiter in &waiters {
            parked(waiter, 2);
        }
        clock.advance(Duration::from_secs(30));
        for thread in threads {
            assert_eq!(thread.join().unwrap(), None);
        }
    }

    // A value that arrives while the waiter is parked wins over the deadline,
    // even when the advance past that deadline is what wakes the waiter.
    #[test]
    fn test_ready_value_survives_advance() {
        let clock = Clock::paused();
        let waiter = clock.waiter();
        let deadline = clock.now() + Duration::from_secs(1);
        let ready = Arc::new(AtomicBool::new(false));
        let waiting = thread::spawn({
            let (waiter, ready) = (waiter.clone(), ready.clone());
            move || {
                waiter.wait_until(Some(deadline), || {
                    ready.load(Ordering::SeqCst).then_some(())
                })
            }
        });
        parked(&waiter, 1);
        ready.store(true, Ordering::SeqCst);
        clock.advance(Duration::from_secs(1));
        assert_eq!(waiting.join().unwrap(), Some(()));
    }

    // One notification wakes every thread parked on the waiter or its clones,
    // on either clock, once the condition they check holds.
    #[test]
    fn test_notify_wakes_parked_waiters() {
        for clock in [Clock::real(), Clock::paused()] {
            let waiter = clock.waiter();
            let open = Arc::new(AtomicBool::new(false));
            let threads: Vec<_> = (0..2)
                .map(|_| {
                    let (waiter, open) = (waiter.clone(), open.clone());
                    thread::spawn(move || {
                        waiter.wait_until(None, || open.load(Ordering::SeqCst).then_some(()))
                    })
                })
                .collect();
            parked(&waiter, 2);
            open.store(true, Ordering::SeqCst);
            waiter.notify_all();
            for thread in threads {
                assert_eq!(thread.join().unwrap(), Some(()), "{clock:?}");
            }
        }
    }

    // The real clock ends a wait by its own timer once the deadline passes.
    // Only real time can end it, so this test waits 1 ms.
    #[test]
    fn test_real_deadline_expires() {
        let waiter = Clock::real().waiter();
        let deadline = Instant::now() + Duration::from_millis(1);
        assert_eq!(waiter.wait_until(Some(deadline), || None::<()>), None);
        assert!(Instant::now() >= deadline);
    }
}

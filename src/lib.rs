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

pub mod sync;

// Test clocks exist only in tests, so a build without the test-clock feature
// cannot stop its own time
#[cfg(any(test, feature = "test-clock"))]
mod paused;

#[cfg(feature = "test-clock")]
pub use paused::TestClock;
#[cfg(all(test, not(feature = "test-clock")))]
use paused::TestClock;

use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

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

/// Checks clock control, sleep races and the internal eventcount contract.
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::{self, Receiver, SyncSender};
    use std::thread;
    use std::time::UNIX_EPOCH;

    /// Blocks until the waiter has entered `count` parks in total.
    fn parked(waiter: &Waiter, count: usize) {
        let mut state = waiter.signal.lock();
        while state.parks < count {
            state = waiter.signal.parked.wait(state).unwrap();
        }
    }

    /// Returns the number of threads parked on a test clock.
    pub(crate) fn blocked(clock: &Clock) -> usize {
        clock.paused.as_ref().unwrap().state.lock().unwrap().blocked
    }

    /// Stops the next wait before it parks, and reports its real timer.
    pub(crate) fn pause_before_park(clock: &Clock) -> (Receiver<Option<Instant>>, SyncSender<()>) {
        pause(&clock.paused.as_ref().unwrap().before_park)
    }

    /// Stops the next park that waits again after a spurious wakeup, and reports
    /// its real timer.
    fn pause_before_rewait(clock: &Clock) -> (Receiver<Option<Instant>>, SyncSender<()>) {
        pause(&clock.paused.as_ref().unwrap().before_rewait)
    }

    /// Installs a one-shot hook that reports a wait's timer and holds the wait
    /// until resumed.
    fn pause(
        hook: &Mutex<Option<paused::BeforePark>>,
    ) -> (Receiver<Option<Instant>>, SyncSender<()>) {
        // Report the timer on one channel, and hold the wait until the other one fires
        let (checked, checks) = mpsc::sync_channel(0);
        let (resume, resumed) = mpsc::sync_channel(0);

        // Stop exactly one wait, reporting the timer that its park will use
        *hook.lock().unwrap() = Some(Box::new(move |timer| {
            checked.send(timer).unwrap();
            resumed.recv().unwrap();
        }));
        (checks, resume)
    }

    /// Finds the platform's last whole wall-clock second through std's checked arithmetic.
    fn last_system_time() -> SystemTime {
        // Add every power of two that still fits, from the largest down
        let mut time = UNIX_EPOCH;
        for bit in (0..64).rev() {
            if let Some(next) = time.checked_add(Duration::from_secs(1 << bit)) {
                time = next;
            }
        }
        time
    }

    // Handles share time and identity only with handles from the same test clock.
    #[test]
    fn test_clock_identity_and_shared_time() {
        /// A real clock can be constructed in a constant expression.
        const REAL: Clock = Clock::real();

        // Create the clocks between two real readings, to bound their start
        let before = Instant::now();
        let mut other = TestClock::new();
        let mut test = TestClock::new();
        let real_now = REAL.now();
        let after = Instant::now();

        // A test clock starts at the real time, with no offset
        let clock = test.clock();
        let clone = clock.clone();
        let other_start = other.clock().now();
        let start = clock.now();
        assert!((before..=after).contains(&start));
        assert!((before..=after).contains(&real_now));

        // Advancing one test clock moves all its handles and leaves the other alone
        test.advance(Duration::from_secs(1));
        assert_eq!(clone.now(), start + Duration::from_secs(1));
        assert_eq!(other.clock().now(), other_start);
        test.advance_to(start + Duration::from_secs(3));
        assert_eq!(clock.now(), start + Duration::from_secs(3));

        // Equal times do not make separate clocks equal, since equality is identity
        other.advance_to(clock.now());
        other.set_system_time(clock.system_time());
        assert_eq!(other.clock().now(), clock.now());
        assert_eq!(other.clock().system_time(), clock.system_time());
        assert_eq!(clock, clone);
        assert_eq!(clock, test.clock());
        assert_ne!(clock, other.clock());

        // Real clocks equal each other and never a test clock
        assert_ne!(clock, REAL);
        assert_ne!(REAL, clock);
        assert_eq!(REAL, Clock::real());
    }

    // Formatting a test clock lets the writer read the same clock without deadlocking.
    #[test]
    fn test_debug_writer_can_read_clock() {
        /// Reads the clock while collecting its formatted output.
        struct ClockWriter {
            /// Handle to the clock being formatted.
            clock: Clock,
            /// Text written by the formatter.
            output: String,
        }

        impl fmt::Write for ClockWriter {
            /// Reads the clock before appending each piece of output.
            fn write_str(&mut self, text: &str) -> fmt::Result {
                let _ = self.clock.now();
                self.output.push_str(text);
                Ok(())
            }
        }

        // Format a test clock into a writer that reads the same clock on every write
        let test = TestClock::new();
        let mut writer = ClockWriter {
            clock: test.clock(),
            output: String::new(),
        };
        fmt::write(&mut writer, format_args!("{test:?}")).unwrap();

        // Returning at all shows no lock was held while writing, and every field came out
        for field in ["advanced:", "system_time:", "blocked:"] {
            assert!(writer.output.contains(field), "{field}");
        }
    }

    // Elapsed time follows advances and saturates at zero for future instants.
    #[test]
    fn test_elapsed_saturates() {
        // Measure an advance from an instant captured before time moves
        let mut test = TestClock::new();
        let clock = test.clock();
        let start = clock.now();
        test.advance(Duration::from_secs(3));
        assert_eq!(clock.elapsed(start), Duration::from_secs(3));

        // Nothing has elapsed since now, nor since an instant still ahead
        assert_eq!(clock.elapsed(clock.now()), Duration::ZERO);
        assert_eq!(
            clock.elapsed(clock.now() + Duration::from_secs(1)),
            Duration::ZERO
        );
    }

    // Wall time follows both advance methods and can jump alone in either direction.
    #[test]
    fn test_system_time_follows_advances_and_jumps_alone() {
        // Create the clocks between two real wall readings, to bound their start
        let before = SystemTime::now();
        let mut test = TestClock::default();
        let real_time = Clock::real().system_time();
        let after = SystemTime::now();

        // Both the test clock and the real clock start at the real wall time
        let clock = test.clock();
        let start = clock.now();
        assert!((before..=after).contains(&clock.system_time()));
        assert!((before..=after).contains(&real_time));

        // Pin wall time to a known value, then check that both advances move it
        test.set_system_time(UNIX_EPOCH + Duration::from_secs(100));
        test.advance(Duration::from_secs(7));
        assert_eq!(clock.now(), start + Duration::from_secs(7));
        assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(107));
        test.advance_to(start + Duration::from_secs(10));
        assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(110));

        // Jump wall time forwards and then backwards, leaving monotonic time in place
        for seconds in [200, 50] {
            let time = UNIX_EPOCH + Duration::from_secs(seconds);
            test.set_system_time(time);
            assert_eq!(clock.system_time(), time, "{seconds}");
            assert_eq!(clock.now(), start + Duration::from_secs(10), "{seconds}");
        }

        // A later advance continues from the jumped wall time
        test.advance(Duration::from_secs(1));
        assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(51));
        assert_eq!(clock.now(), start + Duration::from_secs(11));
    }

    // Backwards targets and monotonic overflow leave both times unchanged and usable.
    #[test]
    fn test_failed_monotonic_advance_keeps_both_times() {
        // Keep a clock handle for checking both times after each rejected advance
        let mut test = TestClock::new();
        let clock = test.clock();

        // Move past the start, so the backwards target below is a valid instant
        test.advance(Duration::from_secs(1));
        for backwards in [true, false] {
            let now = clock.now();
            let wall = clock.system_time();

            // Advancing backwards or past the end of Instant's range must panic
            assert!(
                panic::catch_unwind(AssertUnwindSafe(|| {
                    if backwards {
                        test.advance_to(now - Duration::from_secs(1));
                    } else {
                        test.advance(Duration::MAX);
                    }
                }))
                .is_err(),
                "{backwards}"
            );

            // Neither time moved, and the clock still advances normally
            assert_eq!(clock.now(), now, "{backwards}");
            assert_eq!(clock.system_time(), wall, "{backwards}");
            test.advance(Duration::from_secs(1));
            assert_eq!(clock.now(), now + Duration::from_secs(1), "{backwards}");
            assert_eq!(
                clock.system_time(),
                wall + Duration::from_secs(1),
                "{backwards}"
            );
        }
    }

    // Wall-time overflow in either advance method leaves both times unchanged and usable.
    #[test]
    fn test_failed_wall_advance_keeps_both_times() {
        // Keep a clock handle for checking both times at the wall-time limit
        let mut test = TestClock::new();
        let clock = test.clock();

        // Find the last representable wall time, which no advance can move past
        let last = last_system_time();
        assert!(last.checked_add(Duration::from_secs(1)).is_none());
        for absolute in [false, true] {
            // Set wall time to its limit, so either advance method overflows it
            let now = clock.now();
            test.set_system_time(last);
            assert!(
                panic::catch_unwind(AssertUnwindSafe(|| {
                    if absolute {
                        test.advance_to(now + Duration::from_secs(1));
                    } else {
                        test.advance(Duration::from_secs(1));
                    }
                }))
                .is_err(),
                "{absolute}"
            );

            // Neither time moved, and the clock advances normally once wall time is back in range
            assert_eq!(clock.now(), now, "{absolute}");
            assert_eq!(clock.system_time(), last, "{absolute}");
            test.set_system_time(UNIX_EPOCH);
            test.advance(Duration::from_secs(1));
            assert_eq!(clock.now(), now + Duration::from_secs(1), "{absolute}");
            assert_eq!(
                clock.system_time(),
                UNIX_EPOCH + Duration::from_secs(1),
                "{absolute}"
            );
        }
    }

    // Both sleep methods observe an advance between the deadline check and the
    // park, without a real timer.
    #[test]
    fn test_sleep_observes_advance_before_parking() {
        for until in [false, true] {
            // Start a sleeper that stops right after its deadline check, before it parks
            let mut test = TestClock::new();
            let clock = test.clock();
            let deadline = clock.now() + Duration::from_secs(60);
            let (checked, resume) = pause_before_park(&clock);
            let waiting = thread::spawn(move || {
                if until {
                    clock.sleep_until(deadline);
                } else {
                    clock.sleep(Duration::from_secs(60));
                }
                clock.now()
            });

            // It is about to park without a real timer; advance to its deadline meanwhile
            assert_eq!(checked.recv().unwrap(), None, "{until}");
            test.advance_to(deadline);

            // Resumed, it must notice the advance instead of parking for good
            resume.send(()).unwrap();
            assert_eq!(waiting.join().unwrap(), deadline, "{until}");
        }
    }

    // A partial advance makes both sleep methods park again, without changing
    // their original deadline.
    #[test]
    fn test_sleep_reparks_after_partial_advance() {
        for until in [false, true] {
            // Start a sleeper and wait until it parks
            let mut test = TestClock::new();
            let clock = test.clock();
            let deadline = clock.now() + Duration::from_secs(5);
            let waiting = thread::spawn({
                let clock = clock.clone();
                move || {
                    if until {
                        clock.sleep_until(deadline);
                    } else {
                        clock.sleep(Duration::from_secs(5));
                    }
                    clock.now()
                }
            });
            test.wait_blocked(1);

            // Advance short of the deadline and catch the sleeper on its way back to park
            let (checked, resume) = pause_before_park(&clock);
            test.advance(Duration::from_secs(2));
            assert_eq!(checked.recv().unwrap(), None, "{until}");
            resume.send(()).unwrap();

            // Parked again, it wakes only once the original deadline is reached
            test.wait_blocked(1);
            test.advance_to(deadline);
            assert_eq!(waiting.join().unwrap(), deadline, "{until}");
        }
    }

    // An advance wakes every parked sleeper across cloned handles and releases
    // their registrations and counts.
    #[test]
    fn test_advance_wakes_all_parked_sleepers() {
        // Park six sleepers on clones of one handle, half with each sleep method
        let mut test = TestClock::new();
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(60);
        let threads: Vec<_> = (0..6)
            .map(|index| {
                let clock = clock.clone();
                thread::spawn(move || {
                    if index % 2 == 0 {
                        clock.sleep(Duration::from_secs(60));
                    } else {
                        clock.sleep_until(deadline);
                    }
                    clock.now()
                })
            })
            .collect();
        test.wait_blocked(6);
        assert_eq!(blocked(&clock), 6);

        // One advance releases all of them at the shared deadline
        test.advance(Duration::from_secs(60));
        for waiting in threads {
            assert_eq!(waiting.join().unwrap(), deadline);
        }

        // Their blocked counts and registrations are gone with them
        let state = clock.paused.as_ref().unwrap().state.lock().unwrap();
        assert_eq!(state.blocked, 0);
        assert!(state.signals.is_empty());
    }

    // Reached deadlines and zero-duration sleeps return without an advance or notification.
    #[test]
    fn test_sleep_returns_for_reached_deadlines() {
        // Advance past a known instant, giving a deadline already in the past
        let mut test = TestClock::new();
        let start = test.clock().now();
        test.advance(Duration::from_secs(1));

        // A past deadline, the current instant and a zero duration all return at once
        for clock in [Clock::real(), test.clock()] {
            clock.sleep_until(start);
            clock.sleep_until(clock.now());
            clock.sleep(Duration::ZERO);
        }
    }

    // Wall jumps and zero advances leave monotonic time and parked sleepers' generations untouched.
    #[test]
    fn test_wall_jumps_and_zero_advances_do_not_wake() {
        // Park a waiter three seconds ahead of its deadline and note its generation
        let mut test = TestClock::new();
        let clock = test.clock();
        let now = clock.now();
        let waiter = Arc::new(clock.waiter());
        let waiting = thread::spawn({
            let waiter = waiter.clone();
            move || waiter.wait_until(Some(now + Duration::from_secs(3)), || None::<()>)
        });
        test.wait_blocked(1);
        let seen = waiter.signal.generation();

        // Wall jumps and advances that leave monotonic time in place do not wake it
        for seconds in [100, 50] {
            let wall = UNIX_EPOCH + Duration::from_secs(seconds);
            test.set_system_time(wall);
            test.advance(Duration::ZERO);
            test.advance_to(now);
            assert_eq!(clock.now(), now, "{seconds}");
            assert_eq!(clock.system_time(), wall, "{seconds}");
            assert_eq!(waiter.signal.generation(), seen, "{seconds}");
        }

        // A real advance still reaches it
        test.advance(Duration::from_secs(3));
        assert_eq!(waiting.join().unwrap(), None);
    }

    // A spurious wakeup keeps the park counted and its deadline listed.
    #[test]
    fn test_spurious_wakeup_keeps_park_counted() {
        // Park a timed wait on a waiter the test can reach
        let mut test = TestClock::new();
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let waiter = Arc::new(clock.waiter());
        let waiting = thread::spawn({
            let waiter = waiter.clone();
            move || waiter.wait_until(Some(deadline), || None::<()>)
        });
        test.wait_blocked(1);

        // Wake it through std alone, locking the signal so the wakeup reaches the wait
        let (checked, resume) = pause_before_rewait(&clock);
        {
            let _state = waiter.signal.lock();
            waiter.signal.changed.notify_all();
        }
        assert_eq!(checked.recv().unwrap(), None);

        // Caught between its two waits, the park still counts and lists its deadline
        assert_eq!(blocked(&clock), 1);
        assert_eq!(test.next_deadline(), Some(deadline));
        resume.send(()).unwrap();

        // Reaching the deadline still ends it
        test.advance_to(deadline);
        assert_eq!(waiting.join().unwrap(), None);
    }

    // A poisoned shared lock still permits reading, setting, parking, counting and advancing time.
    #[test]
    fn test_poisoned_clock_remains_usable() {
        // Record both times before the shared clock lock is poisoned
        let mut test = TestClock::new();
        let clock = test.clock();
        let now = clock.now();
        let wall = clock.system_time();

        // Poison the shared lock by panicking while holding it
        let paused = clock.paused.as_ref().unwrap();
        assert!(
            panic::catch_unwind(AssertUnwindSafe(|| {
                let _state = paused.state.lock().unwrap();
                panic!("poison the shared clock lock");
            }))
            .is_err()
        );
        assert!(paused.state.is_poisoned());

        // Reading and setting the times still work
        assert_eq!(clock.now(), now);
        assert_eq!(clock.system_time(), wall);
        test.set_system_time(UNIX_EPOCH);

        // So do parking, counting and advancing
        let waiting = thread::spawn({
            let clock = clock.clone();
            move || clock.sleep(Duration::from_secs(1))
        });
        test.wait_blocked(1);
        test.advance_to(now + Duration::from_secs(1));
        waiting.join().unwrap();
        assert_eq!(clock.now(), now + Duration::from_secs(1));
        assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(1));
    }

    // A ready value wins over an expired deadline, while an empty check returns at the deadline.
    #[test]
    fn test_wait_checks_ready_first() {
        for clock in [Clock::real(), TestClock::new().clock()] {
            // An already reached deadline still gives the ready condition its first check
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

    // A notification during a failed condition check cannot be lost before parking.
    #[test]
    fn test_wait_observes_notification_during_check() {
        for clock in [Clock::real(), TestClock::new().clock()] {
            // The first check notifies the waiter itself and fails, so the park must see it
            let waiter = clock.waiter();
            let mut notified = false;
            let result = waiter.wait_until(None, || {
                if notified {
                    Some(42)
                } else {
                    waiter.signal.notify_all();
                    notified = true;
                    None
                }
            });
            assert_eq!(result, Some(42), "{clock:?}");
        }
    }

    // Advances during condition checks cause rechecks until the deadline is reached.
    #[test]
    fn test_wait_observes_advance_during_check() {
        // Set a deadline that needs two advances from the ready callback
        let mut test = TestClock::new();
        let clock = test.clock();
        let waiter = clock.waiter();
        let deadline = clock.now() + Duration::from_secs(2);

        // Each check advances one second, so the wait must notice both advances to end
        let result = waiter.wait_until(Some(deadline), || {
            test.advance(Duration::from_secs(1));
            None::<()>
        });
        assert_eq!(result, None);
        assert_eq!(clock.now(), deadline);
    }

    // A value arriving during a park wins over the deadline when an advance wakes the waiter.
    #[test]
    fn test_ready_value_survives_advance() {
        // Park a waiter whose condition is a flag nobody notifies about
        let mut test = TestClock::new();
        let clock = test.clock();
        let waiter = Arc::new(clock.waiter());
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
        test.wait_blocked(1);

        // Raise the flag silently, then wake the waiter by advancing past its deadline
        ready.store(true, Ordering::SeqCst);
        test.advance(Duration::from_secs(1));

        // The woken waiter checks its condition before the deadline, so the value wins
        assert_eq!(waiting.join().unwrap(), Some(()));
    }

    // One notification wakes every thread sharing a waiter on either kind of clock.
    #[test]
    fn test_notify_wakes_all_parked_waiters() {
        for clock in [Clock::real(), TestClock::new().clock()] {
            // Park two threads on one shared waiter
            let waiter = Arc::new(clock.waiter());
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

            // Opening the gate and notifying once releases both
            open.store(true, Ordering::SeqCst);
            waiter.signal.notify_all();
            for waiting in threads {
                assert_eq!(waiting.join().unwrap(), Some(()), "{clock:?}");
            }
        }
    }

    // Dropping waiters frees their registrations at once, despite churn and
    // outstanding signal references.
    #[test]
    fn test_waiter_registry_tracks_live_waiters() {
        // Inspect registrations on the clock that owns the waiters
        let test = TestClock::new();
        let clock = test.clock();
        let paused = clock.paused.as_ref().unwrap();

        // Keep three of 128 waiters, holding a dropped one's signal to show that a
        // registration ends with its waiter, not with its signal
        let mut waiters: Vec<_> = (0..128).map(|_| clock.waiter()).collect();
        let signal = waiters.last().unwrap().signal.clone();
        waiters.truncate(3);
        assert_eq!(paused.state.lock().unwrap().signals.len(), 3);

        // Churning through short-lived waiters leaves the registry as it was
        for _ in 0..256 {
            drop(clock.waiter());
        }
        assert_eq!(paused.state.lock().unwrap().signals.len(), 3);

        // Dropping the rest empties it
        drop(waiters);
        drop(signal);
        assert!(paused.state.lock().unwrap().signals.is_empty());
    }
}

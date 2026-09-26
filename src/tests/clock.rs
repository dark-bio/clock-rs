// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Checks clock control, sleep races and notification bookkeeping.

use super::helpers::{blocked, last_system_time, pause_before_park, pause_before_rewait, wakes};
use crate::{Clock, TestClock, Waiter};
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Blocks until `count` waits have started on the waiter. Each wait parks in
/// the signal lock hold that starts it, so its park report covers its start,
/// while a spurious wakeup's second park adds no start.
fn started(waiter: &Waiter, count: usize) {
    let mut state = waiter.signal.lock();
    while state.waiting < count {
        state = waiter.signal.parked.wait(state).unwrap();
    }
}

// Handles share time and identity only with handles from the same test clock.
#[test]
fn test_clock_identity_and_shared_time() {
    /// A real clock can be constructed in a constant expression.
    const REAL: Clock = Clock::real();

    // Create the clocks between two real readings, to bound their start
    let before = Instant::now();
    let mut other = TestClock::new();
    let mut tester = TestClock::new();
    let real_now = REAL.now();
    let after = Instant::now();

    // A test clock starts at the real time, with no offset
    let clock = tester.clock();
    let clone = clock.clone();
    let other_start = other.clock().now();
    let start = clock.now();
    assert!((before..=after).contains(&start));
    assert!((before..=after).contains(&real_now));

    // Advancing one test clock moves all its handles and leaves the other alone
    tester.advance(Duration::from_secs(1));
    assert_eq!(clone.now(), start + Duration::from_secs(1));
    assert_eq!(other.clock().now(), other_start);
    tester.advance_to(start + Duration::from_secs(3));
    assert_eq!(clock.now(), start + Duration::from_secs(3));

    // Equal times do not make separate clocks equal, since equality is identity
    other.advance_to(clock.now());
    other.set_system_time(clock.system_time());
    assert_eq!(other.clock().now(), clock.now());
    assert_eq!(other.clock().system_time(), clock.system_time());
    assert_eq!(clock, clone);
    assert_eq!(clock, tester.clock());
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
    let tester = TestClock::new();
    #[cfg(feature = "crossbeam")]
    let _timer = tester.clock().after(Duration::from_secs(5));
    let mut writer = ClockWriter {
        clock: tester.clock(),
        output: String::new(),
    };
    fmt::write(&mut writer, format_args!("{tester:?}")).unwrap();

    // Returning at all shows no lock was held while writing, and every field came out
    for field in ["advanced:", "system_time:", "blocked:"] {
        assert!(writer.output.contains(field), "{field}");
    }
    #[cfg(feature = "crossbeam")]
    assert!(writer.output.contains("timers: 1"));
}

// A clock's Debug shows both times from one moment, even when its writer
// advances the clock.
#[test]
fn test_clock_debug_uses_one_snapshot() {
    /// Advances the clock while collecting its formatted output.
    struct AdvancingWriter {
        /// Owner of the clock being formatted.
        tester: TestClock,
        /// Text written by the formatter.
        output: String,
    }

    impl fmt::Write for AdvancingWriter {
        /// Advances the clock before appending each piece of output.
        fn write_str(&mut self, text: &str) -> fmt::Result {
            self.tester.advance(Duration::from_secs(1));
            self.output.push_str(text);
            Ok(())
        }
    }

    // Set a known wall time and keep a handle while the writer owns the driver
    let mut tester = TestClock::new();
    tester.set_system_time(UNIX_EPOCH);
    let clock = tester.clock();
    let mut writer = AdvancingWriter {
        tester,
        output: String::new(),
    };

    // Each write advances the clock, but both displayed times precede those writes
    fmt::write(&mut writer, format_args!("{clock:?}")).unwrap();
    assert_eq!(
        writer.output,
        format!("Clock {{ paused: true, advanced: 0ns, system_time: {UNIX_EPOCH:?} }}")
    );
    assert!(clock.system_time() > UNIX_EPOCH);
}

// Elapsed time follows advances and saturates at zero for future instants.
#[test]
fn test_elapsed_saturates() {
    // Measure an advance from an instant captured before time moves
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let start = clock.now();
    tester.advance(Duration::from_secs(3));
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
    let mut tester = TestClock::default();
    let real_time = Clock::real().system_time();
    let after = SystemTime::now();

    // Both the test clock and the real clock start at the real wall time
    let clock = tester.clock();
    let start = clock.now();
    assert!((before..=after).contains(&clock.system_time()));
    assert!((before..=after).contains(&real_time));

    // Pin wall time to a known value, then check that both advances move it
    tester.set_system_time(UNIX_EPOCH + Duration::from_secs(100));
    tester.advance(Duration::from_secs(7));
    assert_eq!(clock.now(), start + Duration::from_secs(7));
    assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(107));
    tester.advance_to(start + Duration::from_secs(10));
    assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(110));

    // Jump wall time forwards and then backwards, leaving monotonic time in place
    for seconds in [200, 50] {
        let time = UNIX_EPOCH + Duration::from_secs(seconds);
        tester.set_system_time(time);
        assert_eq!(clock.system_time(), time, "{seconds}");
        assert_eq!(clock.now(), start + Duration::from_secs(10), "{seconds}");
    }

    // A later advance continues from the jumped wall time
    tester.advance(Duration::from_secs(1));
    assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(51));
    assert_eq!(clock.now(), start + Duration::from_secs(11));
}

// Backwards targets and monotonic overflow leave both times unchanged and usable.
#[test]
fn test_failed_monotonic_advance_keeps_both_times() {
    // Keep a clock handle for checking both times after each rejected advance
    let mut tester = TestClock::new();
    let clock = tester.clock();

    // Move past the start, so the backwards target below is a valid instant
    tester.advance(Duration::from_secs(1));
    for backwards in [true, false] {
        let now = clock.now();
        let wall = clock.system_time();

        // Advancing backwards or past the end of Instant's range must panic
        assert!(
            panic::catch_unwind(AssertUnwindSafe(|| {
                if backwards {
                    tester.advance_to(now - Duration::from_secs(1));
                } else {
                    tester.advance(Duration::MAX);
                }
            }))
            .is_err(),
            "{backwards}"
        );

        // Neither time moved, and the clock still advances normally
        assert_eq!(clock.now(), now, "{backwards}");
        assert_eq!(clock.system_time(), wall, "{backwards}");
        tester.advance(Duration::from_secs(1));
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
    let mut tester = TestClock::new();
    let clock = tester.clock();

    // Find the last representable wall time, which no advance can move past
    let last = last_system_time();
    assert!(last.checked_add(Duration::from_secs(1)).is_none());
    for absolute in [false, true] {
        // Set wall time to its limit, so either advance method overflows it
        let now = clock.now();
        tester.set_system_time(last);
        assert!(
            panic::catch_unwind(AssertUnwindSafe(|| {
                if absolute {
                    tester.advance_to(now + Duration::from_secs(1));
                } else {
                    tester.advance(Duration::from_secs(1));
                }
            }))
            .is_err(),
            "{absolute}"
        );

        // Neither time moved, and the clock advances normally once wall time is back in range
        assert_eq!(clock.now(), now, "{absolute}");
        assert_eq!(clock.system_time(), last, "{absolute}");
        tester.set_system_time(UNIX_EPOCH);
        tester.advance(Duration::from_secs(1));
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
        let mut tester = TestClock::new();
        let clock = tester.clock();
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
        tester.advance_to(deadline);

        // Resumed, it must notice the advance instead of parking for good
        resume.send(()).unwrap();
        assert_eq!(waiting.join().unwrap(), deadline, "{until}");
    }
}

// An advance wakes every parked sleeper across cloned handles and leaves
// none of them counted or listed.
#[test]
fn test_advance_wakes_all_parked_sleepers() {
    // Park six sleepers on clones of one handle, half with each sleep method
    let mut tester = TestClock::new();
    let clock = tester.clock();
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
    tester.wait_blocked(6);
    assert_eq!(blocked(&clock), 6);

    // One advance releases all of them at the shared deadline
    tester.advance(Duration::from_secs(60));
    for waiting in threads {
        assert_eq!(waiting.join().unwrap(), deadline);
    }

    // Their parks are uncounted and their deadlines unlisted with them
    assert_eq!(blocked(&clock), 0);
    assert_eq!(tester.next_deadline(), None);
}

// Reached deadlines and zero-duration sleeps return without an advance or notification.
#[test]
fn test_sleep_returns_for_reached_deadlines() {
    // Advance past a known instant, giving a deadline already in the past
    let mut tester = TestClock::new();
    let start = tester.clock().now();
    tester.advance(Duration::from_secs(1));

    // A past deadline, the current instant and a zero duration all return at once
    for clock in [Clock::real(), tester.clock()] {
        clock.sleep_until(start);
        clock.sleep_until(clock.now());
        clock.sleep(Duration::ZERO);
    }
}

// Wall jumps and zero advances leave monotonic time in place and wake no parked sleeper.
#[test]
fn test_wall_jumps_and_zero_advances_do_not_wake() {
    // Park a waiter three seconds ahead of its deadline
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let now = clock.now();
    let waiter = Arc::new(clock.waiter());
    let waiting = thread::spawn({
        let waiter = waiter.clone();
        move || waiter.wait(Some(now + Duration::from_secs(3)))
    });
    tester.wait_blocked(1);

    // Wall jumps and advances that leave monotonic time in place do not wake it
    for seconds in [100, 50] {
        let wall = UNIX_EPOCH + Duration::from_secs(seconds);
        tester.set_system_time(wall);
        tester.advance(Duration::ZERO);
        tester.advance_to(now);
        assert_eq!(clock.now(), now, "{seconds}");
        assert_eq!(clock.system_time(), wall, "{seconds}");
        assert_eq!(wakes(&waiter.signal), 0, "{seconds}");
    }

    // A real advance still reaches it
    tester.advance(Duration::from_secs(3));
    assert!(!waiting.join().unwrap());
}

// A spurious wakeup keeps the park counted and its deadline listed.
#[test]
fn test_spurious_wakeup_keeps_park_counted() {
    // Park a timed wait on a waiter the test can reach
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let deadline = clock.now() + Duration::from_secs(1);
    let waiter = Arc::new(clock.waiter());
    let waiting = thread::spawn({
        let waiter = waiter.clone();
        move || waiter.wait(Some(deadline))
    });
    tester.wait_blocked(1);

    // Wake it through std alone, locking the signal so the wakeup reaches the wait
    let (checked, resume) = pause_before_rewait(&clock);
    {
        let _state = waiter.signal.lock();
        waiter.signal.changed.notify_all();
    }
    assert_eq!(checked.recv().unwrap(), None);

    // Caught between its two waits, the park still counts and lists its deadline
    assert_eq!(blocked(&clock), 1);
    assert_eq!(tester.next_deadline(), Some(deadline));
    resume.send(()).unwrap();

    // Reaching the deadline still ends it
    tester.advance_to(deadline);
    assert!(!waiting.join().unwrap());
}

// A poisoned shared lock still permits reading, setting, parking, counting and advancing time.
#[test]
fn test_poisoned_clock_remains_usable() {
    // Record both times before the shared clock lock is poisoned
    let mut tester = TestClock::new();
    let clock = tester.clock();
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
    tester.set_system_time(UNIX_EPOCH);

    // So do parking, counting and advancing
    let waiting = thread::spawn({
        let clock = clock.clone();
        move || clock.sleep(Duration::from_secs(1))
    });
    tester.wait_blocked(1);
    tester.advance_to(now + Duration::from_secs(1));
    waiting.join().unwrap();
    assert_eq!(clock.now(), now + Duration::from_secs(1));
    assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(1));
}

// A park returns a timeout immediately for a reached deadline on either clock.
#[test]
fn test_park_returns_for_reached_deadline() {
    for clock in [Clock::real(), TestClock::new().clock()] {
        // Start and retire the wait without entering a physical park
        let waiter = clock.waiter();
        let now = clock.now();
        let mut state = waiter.signal.lock();
        let start = state.start();
        let (state, notified) = waiter.park(state, start, Some(now));
        assert!(!notified, "{clock:?}");
        assert_eq!(state.parks, 0, "{clock:?}");
        assert_eq!(state.waiting, 0, "{clock:?}");
    }
}

// A notification sent after a wait starts cannot be lost before parking.
#[test]
fn test_park_observes_notification_before_parking() {
    for clock in [Clock::real(), TestClock::new().clock()] {
        // Notify between starting the wait and entering its park
        let waiter = clock.waiter();
        let start = waiter.signal.lock().start();
        waiter.signal.notify_all();
        let (state, notified) = waiter.park(waiter.signal.lock(), start, None);
        assert!(notified, "{clock:?}");
        assert_eq!(state.parks, 0, "{clock:?}");
        assert_eq!(state.waiting, 0, "{clock:?}");
    }
}

// An advance between starting a wait and parking ends it without blocking.
#[test]
fn test_park_observes_advance_before_parking() {
    // Start a timed wait before advancing to its deadline
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let waiter = clock.waiter();
    let deadline = clock.now() + Duration::from_secs(2);
    let start = waiter.signal.lock().start();
    tester.advance_to(deadline);

    // The park sees the reached deadline and retires immediately
    let (state, notified) = waiter.park(waiter.signal.lock(), start, Some(deadline));
    assert!(!notified);
    assert_eq!(clock.now(), deadline);
    assert_eq!(state.parks, 0);
    assert_eq!(state.waiting, 0);
}

// One notification wakes every thread sharing a waiter on either kind of clock.
#[test]
fn test_notify_wakes_all_parked_waiters() {
    for clock in [Clock::real(), TestClock::new().clock()] {
        // Park two threads on one shared waiter
        let waiter = Arc::new(clock.waiter());
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let waiter = waiter.clone();
                thread::spawn(move || waiter.wait(None))
            })
            .collect();
        started(&waiter, 2);

        // Broadcasting once releases both
        waiter.signal.notify_all();
        for waiting in threads {
            assert!(waiting.join().unwrap(), "{clock:?}");
        }
    }
}

// A timed registration retains its signal only until the park retires.
#[test]
fn test_park_registration_retains_signal_until_retired() {
    // Register a park and release the waiter's ownership of its signal
    let tester = TestClock::new();
    let clock = tester.clock();
    let paused = clock.paused.as_ref().unwrap();
    let waiter = clock.waiter();
    let signal = Arc::downgrade(&waiter.signal);
    let deadline = clock.now() + Duration::from_secs(1);
    let registration = paused.block(Some(deadline), &waiter.signal).unwrap();
    drop(waiter);
    assert!(signal.upgrade().is_some());
    assert_eq!(tester.next_deadline(), Some(deadline));

    // Retiring the park removes its deadline and releases its signal
    drop(registration);
    assert!(signal.upgrade().is_none());
    assert_eq!(tester.next_deadline(), None);
    assert_eq!(blocked(&clock), 0);
}

// Retiring one park preserves another park with the same signal and deadline.
#[test]
fn test_retiring_park_keeps_shared_deadline_listed() {
    // Register two parks sharing both their signal and deadline
    let tester = TestClock::new();
    let clock = tester.clock();
    let waiter = clock.waiter();
    let paused = clock.paused.as_ref().unwrap();
    let deadline = clock.now() + Duration::from_secs(1);
    let survivor = paused.block(Some(deadline), &waiter.signal).unwrap();
    let dropped = paused.block(Some(deadline), &waiter.signal).unwrap();

    // Retiring the later registration leaves the earlier one listed and counted
    drop(dropped);
    assert_eq!(tester.next_deadline(), Some(deadline));
    assert_eq!(blocked(&clock), 1);
    drop(survivor);
    assert_eq!(tester.next_deadline(), None);
    assert_eq!(blocked(&clock), 0);
}

// Wall time follows the sum of the advances, without rounding each one.
#[test]
fn test_wall_time_accumulates_fractional_advances() {
    // Pin wall time and advance twice by 150 ns, which Windows' 100 ns wall time cannot hold
    let mut tester = TestClock::new();
    let clock = tester.clock();
    tester.set_system_time(UNIX_EPOCH);
    tester.advance(Duration::from_nanos(150));
    tester.advance(Duration::from_nanos(150));
    assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_nanos(300));

    // A wall jump starts the sum over from the new wall time
    tester.set_system_time(UNIX_EPOCH + Duration::from_secs(1));
    tester.advance(Duration::from_nanos(150));
    tester.advance(Duration::from_nanos(150));
    assert_eq!(
        clock.system_time(),
        UNIX_EPOCH + Duration::from_nanos(1_000_000_300)
    );
}

// A driver stepping through next_deadline ends every sleep in deadline order.
#[test]
fn test_next_deadline_drives_sleeps_in_order() {
    // Start three sleeps out of deadline order
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let start = clock.now();
    let mut sleeps: Vec<_> = [3, 1, 2]
        .into_iter()
        .map(|seconds| {
            let clock = clock.clone();
            let deadline = start + Duration::from_secs(seconds);
            (deadline, thread::spawn(move || clock.sleep_until(deadline)))
        })
        .collect();
    tester.wait_blocked(3);
    sleeps.sort_by_key(|(deadline, _)| *deadline);

    // Each advance wakes only the sleep it reaches, which returns before the next read
    for (index, (deadline, sleep)) in sleeps.into_iter().enumerate() {
        let next = tester.next_deadline().unwrap();
        assert_eq!(next, deadline, "{index}");
        tester.advance_to(next);
        sleep.join().unwrap();
        assert_eq!(blocked(&clock), 2 - index, "{index}");
    }
    assert_eq!(tester.next_deadline(), None);
    assert_eq!(blocked(&clock), 0);
}

// A real sleep lasts at least its duration, with no upper bound on how long.
#[test]
fn test_real_sleep_reaches_duration() {
    let clock = Clock::real();
    let start = Instant::now();
    clock.sleep(Duration::from_millis(1));
    assert!(clock.elapsed(start) >= Duration::from_millis(1));
}

// A real sleep_until returns only once its deadline has passed, with no upper
// bound on how long.
#[test]
fn test_real_sleep_until_reaches_deadline() {
    let clock = Clock::real();
    let deadline = Instant::now() + Duration::from_millis(1);
    clock.sleep_until(deadline);
    assert!(Instant::now() >= deadline);
}

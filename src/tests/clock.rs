// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Checks clock control, sleep races and the internal eventcount contract.

use super::helpers::{blocked, last_system_time, pause_before_park, pause_before_rewait};
use crate::{Clock, TestClock, Waiter};
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Blocks until the waiter has entered `count` parks in total.
fn parked(waiter: &Waiter, count: usize) {
    let mut state = waiter.signal.lock();
    while state.parks < count {
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
    #[cfg(feature = "crossbeam")]
    let _timer = test.clock().after(Duration::from_secs(5));
    let mut writer = ClockWriter {
        clock: test.clock(),
        output: String::new(),
    };
    fmt::write(&mut writer, format_args!("{test:?}")).unwrap();

    // Returning at all shows no lock was held while writing, and every field came out
    for field in ["advanced:", "system_time:", "blocked:"] {
        assert!(writer.output.contains(field), "{field}");
    }
    #[cfg(feature = "crossbeam")]
    assert!(writer.output.contains("timers: 1"));
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

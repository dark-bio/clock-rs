// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Exhaustive scheduling models of test-clock waits within loom's preemption bound.
//!
//! When a model finds a deadlock, loom names it in the first panic. The test binary
//! then aborts, since the clock's guards take a loom lock while unwinding, so run
//! one model at a time to see which others fail too.

use crate::sync::{Condvar, Mutex};
use crate::{Clock, TestClock};
use loom::thread;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// Checks that completed waits leave no parks, deadlines or registrations.
fn assert_idle(test: &TestClock, clock: &Clock) {
    // Read the public deadline before inspecting the remaining bookkeeping
    assert_eq!(test.next_deadline(), None);
    let state = clock.paused.as_ref().unwrap().state.lock().unwrap();
    assert_eq!(state.blocked, 0);
    assert!(state.signals.is_empty());
}

/// Checks a condition published under the caller's mutex with either notification order.
fn notification_model(holding: bool) {
    loom::model(move || {
        // Race a condition wait with a notifier using the same modeled mutex
        let test = TestClock::new();
        let clock = test.clock();
        let pair = Arc::new((Mutex::new(false), Condvar::new(&clock)));
        let waiting = thread::spawn({
            let pair = pair.clone();
            move || {
                let ready = pair.1.wait_while(pair.0.lock().unwrap(), |ready| !*ready);
                assert!(*ready.unwrap());
            }
        });

        // Publish the condition and notify on the chosen side of the unlock
        let mut ready = pair.0.lock().unwrap();
        *ready = true;
        if holding {
            pair.1.notify_one();
            drop(ready);
        } else {
            drop(ready);
            pair.1.notify_one();
        }

        // Both schedules finish and release the condvar's registration
        waiting.join().unwrap();
        drop(pair);
        assert_idle(&test, &clock);
    });
}

// A notification under the caller's mutex releases a concurrent condition wait.
#[test]
fn test_wait_observes_notification_under_mutex() {
    notification_model(true);
}

// A notification after unlocking releases a concurrent condition wait.
#[test]
fn test_wait_observes_notification_after_unlock() {
    notification_model(false);
}

/// Checks that one broadcast or two single notifications release two parked waiters.
fn two_waiters_model(broadcast: bool) {
    loom::model(move || {
        // Park two waiters on one condvar before any notification can arrive
        let test = TestClock::new();
        let clock = test.clock();
        let pair = Arc::new((Mutex::new(false), Condvar::new(&clock)));
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let pair = pair.clone();
                thread::spawn(move || {
                    let ready = pair.1.wait_while(pair.0.lock().unwrap(), |ready| !*ready);
                    assert!(*ready.unwrap());
                })
            })
            .collect();
        test.wait_blocked(2);

        // Publish one condition and wake both waiters through the selected operation
        *pair.0.lock().unwrap() = true;
        if broadcast {
            pair.1.notify_all();
        } else {
            pair.1.notify_one();
            pair.1.notify_one();
        }

        // Each waiter must finish without any further notification
        for waiter in waiters {
            waiter.join().unwrap();
        }
        drop(pair);
        assert_idle(&test, &clock);
    });
}

// Two single notifications release both waiters sharing a condvar.
#[test]
fn test_two_notify_one_calls_release_two_waiters() {
    two_waiters_model(false);
}

// One broadcast releases both waiters sharing a condvar.
#[test]
fn test_notify_all_releases_two_waiters() {
    two_waiters_model(true);
}

// A deadline wait expires at its exact deadline regardless of when it parks.
#[test]
fn test_wait_deadline_observes_exact_advance() {
    loom::model(|| {
        // Start a deadline wait without arranging which thread reaches it first
        let mut test = TestClock::new();
        test.set_system_time(UNIX_EPOCH);
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let waiting = thread::spawn({
            let clock = clock.clone();
            move || {
                let condvar = Condvar::new(&clock);
                let mutex = Mutex::new(());
                let (_guard, result) = condvar
                    .wait_deadline(mutex.lock().unwrap(), deadline)
                    .unwrap();
                assert!(result.timed_out());
                assert_eq!(clock.now(), deadline);
                assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(1));
            }
        });

        // Reaching the deadline must suffice even when it precedes the park
        test.advance_to(deadline);
        waiting.join().unwrap();
        assert_idle(&test, &clock);
    });
}

// A short advance does not end a deadline wait before its condition is notified.
#[test]
fn test_wait_deadline_observes_notification_after_short_advance() {
    loom::model(|| {
        // Park a timed wait whose condition the driver has not set
        let mut test = TestClock::new();
        let clock = test.clock();
        let start = clock.now();
        let deadline = start + Duration::from_secs(2);
        let pair = Arc::new((Mutex::new(false), Condvar::new(&clock)));
        let waiting = thread::spawn({
            let pair = pair.clone();
            move || {
                let (ready, result) = pair
                    .1
                    .wait_deadline(pair.0.lock().unwrap(), deadline)
                    .unwrap();
                assert!(!result.timed_out());
                assert!(*ready);
            }
        });
        test.wait_blocked(1);

        // Wake for a short advance before publishing and notifying the condition
        test.advance(Duration::from_secs(1));
        *pair.0.lock().unwrap() = true;
        pair.1.notify_one();

        // The notification finishes the wait without reaching its deadline
        waiting.join().unwrap();
        assert_eq!(clock.now(), start + Duration::from_secs(1));
        drop(pair);
        assert_idle(&test, &clock);
    });
}

// Sleeping until a fixed deadline observes an advance before or after parking.
#[test]
fn test_sleep_until_observes_exact_advance() {
    loom::model(|| {
        // Start a sleeper without ordering its checks against the advance
        let mut test = TestClock::new();
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let sleeper = thread::spawn({
            let clock = clock.clone();
            move || {
                clock.sleep_until(deadline);
                assert_eq!(clock.now(), deadline);
            }
        });

        // The deadline advance alone must finish the sleep and remove its waiter
        test.advance_to(deadline);
        sleeper.join().unwrap();
        assert_idle(&test, &clock);
    });
}

// A driver that observes a parked sleeper sees its deadline and can release it.
#[test]
fn test_wait_blocked_exposes_sleep_deadline() {
    loom::model(|| {
        // Race the driver's blocked-count wait against the sleeper's registration
        let mut test = TestClock::new();
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let sleeper = thread::spawn({
            let clock = clock.clone();
            move || {
                clock.sleep_until(deadline);
                assert_eq!(clock.now(), deadline);
            }
        });

        // Observing a park guarantees its deadline is visible and its wakeup reaches it
        test.wait_blocked(1);
        assert_eq!(test.next_deadline(), Some(deadline));
        test.advance_to(deadline);
        sleeper.join().unwrap();
        assert_idle(&test, &clock);
    });
}

// Condvar creation and drop leave no registration behind when they race an advance.
#[test]
fn test_condvar_drop_removes_registration_during_advance() {
    loom::model(|| {
        // Create and drop a condvar while the driver may collect its signal
        let mut test = TestClock::new();
        let clock = test.clock();
        let creating = thread::spawn({
            let clock = clock.clone();
            move || drop(Condvar::new(&clock))
        });

        // Even an advance retaining the signal cannot retain its registration
        test.advance(Duration::from_secs(1));
        creating.join().unwrap();
        assert_idle(&test, &clock);
    });
}

// A broadcast racing an advance releases timed and untimed waits on the same condvar.
#[test]
fn test_notify_all_races_advance_with_mixed_waiters() {
    loom::model(|| {
        // Park two waiters on separate conditions, so the untimed one can notify
        let mut test = TestClock::new();
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let pair = Arc::new((Mutex::new((false, false)), Condvar::new(&clock)));
        let timed = thread::spawn({
            let (pair, clock) = (pair.clone(), clock.clone());
            move || {
                let mut ready = pair.0.lock().unwrap();
                loop {
                    // Wait again after the driver's broadcast, until notified or expired
                    let (next, result) = pair.1.wait_deadline(ready, deadline).unwrap();
                    ready = next;
                    if result.timed_out() {
                        assert_eq!(clock.now(), deadline);
                        break;
                    }
                    if ready.1 {
                        break;
                    }
                }
            }
        });
        let untimed = thread::spawn({
            let pair = pair.clone();
            move || {
                // Wait for the driver to release this thread without imposing a deadline
                let mut ready = pair
                    .1
                    .wait_while(pair.0.lock().unwrap(), |ready| !ready.0)
                    .unwrap();
                assert!(ready.0);

                // Broadcast the timed wait's condition while the driver advances
                ready.1 = true;
                drop(ready);
                pair.1.notify_all();
            }
        });
        test.wait_blocked(2);

        // Release the untimed notifier and race its broadcast against the advance
        pair.0.lock().unwrap().0 = true;
        pair.1.notify_all();
        test.advance_to(deadline);

        // Both waiters finish and leave no counts or registrations behind
        timed.join().unwrap();
        untimed.join().unwrap();
        drop(pair);
        assert_idle(&test, &clock);
    });
}

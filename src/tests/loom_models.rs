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
use crate::{Clock, TestClock, Waiter};
use loom::sync::mpsc;
use loom::thread;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// Holds a registered wait outside its signal lock before its next notification check.
fn hold_wait(clock: &Clock, waiter: &Waiter) -> mpsc::Sender<()> {
    // Use modeled channels to hold the rewait without imposing a real schedule
    let (checked, checks) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    *clock.paused.as_ref().unwrap().before_rewait.lock().unwrap() = Some(Box::new(move |_| {
        checked.send(()).unwrap();
        resumed.recv().unwrap();
    }));

    // A raw wake reaches the hook without releasing the wait
    waiter.signal.wake();
    checks.recv().unwrap();
    resume
}

/// Checks that completed waits leave no registrations, deadlines or notifications.
fn assert_idle(tester: &TestClock, clock: &Clock) {
    // Read the public deadline before inspecting the remaining bookkeeping
    assert_eq!(tester.next_deadline(), None);
    let state = clock.paused.as_ref().unwrap().state.lock().unwrap();
    assert_eq!(state.registered, 0);
    assert!(state.parked(0));
}

/// Checks a condition published under the caller's mutex with either notification order.
fn notification_model(holding: bool) {
    loom::model(move || {
        // Race a condition wait with a notifier using the same modeled mutex
        let tester = TestClock::new();
        let clock = tester.clock();
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

        // Both schedules finish and retire the wait
        waiting.join().unwrap();
        drop(pair);
        assert_idle(&tester, &clock);
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

/// Checks that one broadcast or two single notifications release two registered waits.
fn two_waiters_model(broadcast: bool) {
    loom::model(move || {
        // Register two waits on one condvar before any notification can arrive
        let tester = TestClock::new();
        let clock = tester.clock();
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
        tester.wait_registered(2);

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
        assert_idle(&tester, &clock);
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

// A deadline wait expires at its exact deadline regardless of when it registers.
#[test]
fn test_wait_deadline_observes_exact_advance() {
    loom::model(|| {
        // Start a deadline wait without arranging which thread reaches it first
        let mut tester = TestClock::new();
        tester.set_system_time(UNIX_EPOCH);
        let clock = tester.clock();
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

        // Reaching the deadline must suffice even when it precedes registration
        tester.advance_to(deadline);
        waiting.join().unwrap();
        assert_idle(&tester, &clock);
    });
}

// A short advance does not end a deadline wait before its condition is notified.
#[test]
fn test_wait_deadline_observes_notification_after_short_advance() {
    loom::model(|| {
        // Register a timed wait whose condition the driver has not set
        let mut tester = TestClock::new();
        let clock = tester.clock();
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
        tester.wait_registered(1);

        // Advance short of the deadline before publishing and notifying the condition
        tester.advance(Duration::from_secs(1));
        *pair.0.lock().unwrap() = true;
        pair.1.notify_one();

        // The notification finishes the wait without reaching its deadline
        waiting.join().unwrap();
        assert_eq!(clock.now(), start + Duration::from_secs(1));
        drop(pair);
        assert_idle(&tester, &clock);
    });
}

// Sleeping until a fixed deadline observes an advance before or after registration.
#[test]
fn test_sleep_until_observes_exact_advance() {
    loom::model(|| {
        // Start a sleeper without ordering its checks against the advance
        let mut tester = TestClock::new();
        let clock = tester.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let sleeper = thread::spawn({
            let clock = clock.clone();
            move || {
                clock.sleep_until(deadline);
                assert_eq!(clock.now(), deadline);
            }
        });

        // The deadline advance alone must finish the sleep and remove its waiter
        tester.advance_to(deadline);
        sleeper.join().unwrap();
        assert_idle(&tester, &clock);
    });
}

// A driver that observes a registered sleeper sees its deadline and can release it.
#[test]
fn test_wait_registered_exposes_sleep_deadline() {
    loom::model(|| {
        // Race the driver's count check against the sleeper's registration
        let mut tester = TestClock::new();
        let clock = tester.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let sleeper = thread::spawn({
            let clock = clock.clone();
            move || {
                clock.sleep_until(deadline);
                assert_eq!(clock.now(), deadline);
            }
        });

        // Observing a registration guarantees its deadline is visible and its wakeup reaches it
        tester.wait_registered(1);
        assert_eq!(tester.next_deadline(), Some(deadline));
        tester.advance_to(deadline);
        sleeper.join().unwrap();
        assert_idle(&tester, &clock);
    });
}

// A broadcast racing an advance releases timed and untimed waits on the same condvar.
#[test]
fn test_notify_all_races_advance_with_mixed_waiters() {
    loom::model(|| {
        // Register two waits on separate conditions, so the untimed one can notify
        let mut tester = TestClock::new();
        let clock = tester.clock();
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
        tester.wait_registered(2);

        // Release the untimed notifier and race its broadcast against the advance
        pair.0.lock().unwrap().0 = true;
        pair.1.notify_all();
        tester.advance_to(deadline);

        // Both waiters finish and leave no counts or listed deadlines behind
        timed.join().unwrap();
        untimed.join().unwrap();
        drop(pair);
        assert_idle(&tester, &clock);
    });
}

// A partial advance never ends a sleep, whether it lands before or after the
// sleep registers, and the registered sleep stays listed at its own deadline.
#[test]
fn test_partial_advance_leaves_sleep_listed() {
    loom::model(|| {
        // Start a sleeper without ordering its registration against a partial advance
        let mut tester = TestClock::new();
        let clock = tester.clock();
        let deadline = clock.now() + Duration::from_secs(2);
        let sleeper = thread::spawn({
            let clock = clock.clone();
            move || {
                clock.sleep_until(deadline);
                assert_eq!(clock.now(), deadline);
            }
        });
        tester.advance(Duration::from_secs(1));

        // Once registered, the sleep is listed at its deadline, and reaching it ends it
        tester.wait_registered(1);
        assert_eq!(tester.next_deadline(), Some(deadline));
        tester.advance_to(deadline);
        sleeper.join().unwrap();
        assert_idle(&tester, &clock);
    });
}

// Reaching one deadline wait on a condvar keeps a later wait on the same condvar
// registered and listed, however the waits' registrations race the advance.
#[test]
fn test_reached_wait_keeps_later_wait_on_condvar_listed() {
    loom::model(|| {
        // Start an earlier and a later deadline wait on one condvar, racing the first advance
        let mut tester = TestClock::new();
        let clock = tester.clock();
        let earlier = clock.now() + Duration::from_secs(1);
        let later = clock.now() + Duration::from_secs(2);
        let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
        let waits: Vec<_> = [earlier, later]
            .into_iter()
            .map(|deadline| {
                let pair = pair.clone();
                thread::spawn(move || {
                    let (_guard, result) = pair
                        .1
                        .wait_deadline(pair.0.lock().unwrap(), deadline)
                        .unwrap();
                    assert!(result.timed_out());
                })
            })
            .collect();
        let mut waits = waits.into_iter();

        // Reaching the earlier deadline ends that wait while the later one registers
        tester.advance_to(earlier);
        waits.next().unwrap().join().unwrap();
        tester.wait_registered(1);
        assert_eq!(tester.next_deadline(), Some(later));

        // Reaching the later deadline ends it too
        tester.advance_to(later);
        waits.next().unwrap().join().unwrap();
        drop(pair);
        assert_idle(&tester, &clock);
    });
}

// A broadcast racing a reached deadline releases a registered untimed wait.
#[test]
fn test_notification_survives_advance_with_mixed_waiters() {
    loom::model(|| {
        // Register timed and untimed waits on one condvar before either wake can arrive
        let mut tester = TestClock::new();
        let clock = tester.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let pair = Arc::new((Mutex::new(false), Condvar::new(&clock)));
        let timed = thread::spawn({
            let (pair, clock) = (pair.clone(), clock.clone());
            move || {
                let (ready, result) = pair
                    .1
                    .wait_deadline(pair.0.lock().unwrap(), deadline)
                    .unwrap();
                if result.timed_out() {
                    assert_eq!(clock.now(), deadline);
                } else {
                    assert!(*ready);
                }
            }
        });
        let untimed = thread::spawn({
            let pair = pair.clone();
            move || {
                let ready = pair.1.wait_while(pair.0.lock().unwrap(), |ready| !*ready);
                assert!(*ready.unwrap());
            }
        });
        tester.wait_registered(2);

        // Race the first notification with the advance while both waits still count
        let notifying = thread::spawn({
            let pair = pair.clone();
            move || {
                *pair.0.lock().unwrap() = true;
                pair.1.notify_all();
            }
        });
        tester.advance_to(deadline);

        // The untimed wait must observe the notification without a second broadcast
        notifying.join().unwrap();
        timed.join().unwrap();
        untimed.join().unwrap();
        drop(pair);
        assert_idle(&tester, &clock);
    });
}

// A consumed single notification leaves an unreached sibling continuously listed.
#[test]
fn test_consumed_notification_keeps_unreached_sibling_listed() {
    loom::model(|| {
        // Register two waits that repeat earlier only after consuming a notification
        let mut tester = TestClock::new();
        let clock = tester.clock();
        let earlier = clock.now() + Duration::from_secs(1);
        let later = clock.now() + Duration::from_secs(2);
        let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
        let (report, reports) = mpsc::channel();
        let waits: Vec<_> = (0..2)
            .map(|_| {
                let (pair, report) = (pair.clone(), report.clone());
                thread::spawn(move || {
                    let (guard, result) =
                        pair.1.wait_deadline(pair.0.lock().unwrap(), later).unwrap();
                    report.send(Some(result.timed_out())).unwrap();
                    if !result.timed_out() {
                        let (_guard, result) = pair.1.wait_deadline(guard, earlier).unwrap();
                        report.send(Some(result.timed_out())).unwrap();
                    }
                })
            })
            .collect();
        tester.wait_registered(2);
        pair.1.notify_one();
        assert_eq!(reports.recv().unwrap(), Some(false));
        tester.wait_registered(2);

        // A rewait and a timeout distinguish a retained sibling from an early return
        *clock.paused.as_ref().unwrap().before_rewait.lock().unwrap() = Some(Box::new(move |_| {
            report.send(None).unwrap();
        }));
        tester.advance_to(earlier);
        let events = [reports.recv().unwrap(), reports.recv().unwrap()];
        assert!(events.contains(&None));
        assert!(events.contains(&Some(true)));
        assert_eq!(tester.next_deadline(), Some(later));

        // The remaining wait times out only at its own deadline
        tester.advance_to(later);
        assert_eq!(reports.recv().unwrap(), Some(true));
        for wait in waits {
            wait.join().unwrap();
        }
        assert_idle(&tester, &clock);
    });
}

// A later wait cannot take a pending notification when an advance wakes both waits.
#[test]
fn test_later_wait_cannot_take_pending_notification() {
    loom::model(|| {
        // Reserve a notification for a registered wait held before its next check
        let mut tester = TestClock::new();
        let clock = tester.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let waiter = Arc::new(clock.waiter());
        let first = thread::spawn({
            let waiter = waiter.clone();
            move || waiter.wait(Some(deadline))
        });
        tester.wait_registered(1);
        let resume = hold_wait(&clock, &waiter);
        waiter.signal.notify_one();

        // The later wait checks first after the advance and must leave that notification alone
        let next = thread::spawn({
            let waiter = waiter.clone();
            move || waiter.wait(Some(deadline))
        });
        tester.wait_registered(2);
        tester.advance_to(deadline);
        assert!(!next.join().unwrap());
        resume.send(()).unwrap();
        assert!(first.join().unwrap());
        assert_idle(&tester, &clock);
    });
}

// A single notification after a broadcast leaves no stranded credit for a later wait.
#[test]
fn test_broadcast_then_single_notification_preserves_later_wait() {
    loom::model(|| {
        // Keep the broadcast's waiter from checking until after the single notification
        let tester = TestClock::new();
        let clock = tester.clock();
        let deadline = clock.now() + Duration::from_secs(1);
        let waiter = Arc::new(clock.waiter());
        let first = thread::spawn({
            let waiter = waiter.clone();
            move || waiter.wait(Some(deadline))
        });
        tester.wait_registered(1);
        let resume = hold_wait(&clock, &waiter);
        waiter.signal.notify_all();
        waiter.signal.notify_one();
        resume.send(()).unwrap();
        assert!(first.join().unwrap());
        {
            let state = waiter.signal.lock();
            assert_eq!(state.waiting, 0);
            assert!(state.pending.is_empty());
        }

        // A later wait receives its own notification and retires normally
        let next = thread::spawn({
            let waiter = waiter.clone();
            move || waiter.wait(Some(deadline))
        });
        tester.wait_registered(1);
        waiter.signal.notify_one();
        assert!(next.join().unwrap());
        assert_idle(&tester, &clock);
    });
}

/// A notified wait stays registered but prevents the clock from reporting parked workers.
#[test]
fn test_notification_prevents_parked_barrier_with_registered_wait() {
    for broadcast in [false, true] {
        loom::model(move || {
            // Hold a timed worker before it can consume a notification
            let mut tester = TestClock::new();
            let clock = tester.clock();
            let first = clock.now() + Duration::from_secs(1);
            let next = clock.now() + Duration::from_secs(2);
            let waiter = Arc::new(clock.waiter());
            let waiting = thread::spawn({
                let waiter = waiter.clone();
                move || {
                    assert!(waiter.wait(Some(first)));
                    assert!(!waiter.wait(Some(next)));
                }
            });
            tester.wait_registered(1);
            let resume = hold_wait(&clock, &waiter);

            // Keep the registration visible while the notification prevents the barrier
            if broadcast {
                waiter.signal.notify_all();
            } else {
                waiter.signal.notify_one();
            }
            assert_eq!(tester.next_deadline(), Some(first));
            assert!(
                !clock
                    .paused
                    .as_ref()
                    .unwrap()
                    .state
                    .lock()
                    .unwrap()
                    .parked(1)
            );
            resume.send(()).unwrap();
            tester.wait_parked(1);
            assert_eq!(tester.next_deadline(), Some(next));
            tester.advance_to(next);
            waiting.join().unwrap();
            assert_idle(&tester, &clock);
        });
    }
}

/// A reached registration cannot satisfy the parked wait barrier before its thread runs.
#[test]
fn test_reached_wait_needs_new_registration_for_parked_barrier() {
    loom::model(|| {
        // Hold the first deadline wait until the driver has inspected expiry
        let mut tester = TestClock::new();
        let clock = tester.clock();
        let first = clock.now() + Duration::from_secs(1);
        let next = clock.now() + Duration::from_secs(2);
        let waiter = Arc::new(clock.waiter());
        let waiting = thread::spawn({
            let waiter = waiter.clone();
            move || {
                assert!(!waiter.wait(Some(first)));
                assert!(!waiter.wait(Some(next)));
            }
        });
        tester.wait_registered(1);
        let resume = hold_wait(&clock, &waiter);
        tester.advance_to(first);
        assert!(
            !clock
                .paused
                .as_ref()
                .unwrap()
                .state
                .lock()
                .unwrap()
                .parked(1)
        );
        resume.send(()).unwrap();
        tester.wait_parked(1);
        assert_eq!(tester.next_deadline(), Some(next));
        tester.advance_to(next);
        waiting.join().unwrap();
        assert_idle(&tester, &clock);
    });
}

/// Reaches two independent signals with one advance, ordering wakes reproducibly.
fn reached_signals_model(equal: bool, condvars: bool) {
    loom::model(move || {
        // Let both waits register and list their deadlines before the advance collects them
        let mut tester = TestClock::new();
        let clock = tester.clock();
        let earlier = clock.now() + Duration::from_secs(1);
        let later = clock.now() + Duration::from_secs(2);
        let waits: Vec<_> = [if equal { later } else { earlier }, later]
            .into_iter()
            .map(|deadline| {
                let clock = clock.clone();
                thread::spawn(move || {
                    if condvars {
                        let condvar = Condvar::new(&clock);
                        let mutex = Mutex::new(());
                        let (_guard, result) = condvar
                            .wait_deadline(mutex.lock().unwrap(), deadline)
                            .unwrap();
                        assert!(result.timed_out());
                    } else {
                        clock.sleep_until(deadline);
                    }
                    assert_eq!(clock.now(), later);
                })
            })
            .collect();
        tester.wait_registered(2);

        // One advance reaches both, regardless of their deadlines or allocation addresses
        tester.advance_to(later);
        for wait in waits {
            wait.join().unwrap();
        }
        assert_idle(&tester, &clock);
    });
}

// One advance reaches two sleeps on separate signals at different deadlines.
#[test]
fn test_advance_reaches_two_sleep_signals() {
    reached_signals_model(false, false);
}

// One advance reaches two sleeps on separate signals at equal deadlines.
#[test]
fn test_advance_reaches_equal_sleep_signals() {
    reached_signals_model(true, false);
}

// One advance reaches deadline waits on two independent condvars.
#[test]
fn test_advance_reaches_two_condvar_signals() {
    reached_signals_model(false, true);
}

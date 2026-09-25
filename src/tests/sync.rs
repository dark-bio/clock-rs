// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Checks std parity for the mutex, and condvar waits that follow a clock,
//! without timed polling.

use super::*;
use crate::TestClock;
use crate::tests::helpers::{blocked, pause_before_park};
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
    let tester = TestClock::new();
    let pair = Arc::new((Mutex::new(false), Condvar::new(&tester.clock())));
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
    tester.wait_blocked(1);

    // A notification with the gate still closed leads to another check and park
    pair.1.notify_one();
    assert!(!checks.recv().unwrap());
    tester.wait_blocked(1);

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
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
    let waiting = start_wait(&pair, |condvar, guard| drop(condvar.wait(guard).unwrap()));
    tester.wait_blocked(1);

    // Catch the wait on its way back to park after an advance
    let (checked, resume) = pause_before_park(&clock);
    tester.advance(Duration::from_secs(60));
    assert_eq!(checked.recv().unwrap(), None);
    resume.send(()).unwrap();

    // Parked again, it returns on a notification
    tester.wait_blocked(1);
    pair.1.notify_one();
    waiting.join().unwrap();
}

// A deadline wait outlasts short advances and times out exactly at its deadline.
#[test]
fn test_wait_deadline_reparks_until_exact_deadline() {
    // Park a wait on a deadline taken from its own clock
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let deadline = clock.now() + Duration::from_secs(5);
    let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
    let waiting = start_wait(&pair, move |condvar, guard| {
        condvar.wait_deadline(guard, deadline).unwrap().1
    });
    tester.wait_blocked(1);
    assert_eq!(tester.next_deadline(), Some(deadline));

    // Catch the wait between parks, holding no real timer and no listed deadline
    let (checked, resume) = pause_before_park(&clock);
    tester.advance(Duration::from_secs(2));
    assert_eq!(checked.recv().unwrap(), None);
    assert_eq!(tester.next_deadline(), None);
    assert_eq!(blocked(&clock), 0);
    resume.send(()).unwrap();
    tester.wait_blocked(1);
    assert_eq!(tester.next_deadline(), Some(deadline));

    // Reaching the deadline ends the wait, timed out, with no notification
    tester.advance_to(deadline);
    assert!(waiting.join().unwrap().timed_out());
    assert_eq!(clock.now(), deadline);
    assert_eq!(tester.next_deadline(), None);
}

// A notification ends a deadline wait early, not timed out, on either kind of clock.
#[test]
fn test_wait_deadline_returns_early_on_notification() {
    let tester = TestClock::new();
    for clock in [Clock::real(), tester.clock()] {
        // Start a wait whose deadline is a day away
        let deadline = clock.now() + Duration::from_secs(86400);
        let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
        let waiting = start_wait(&pair, move |condvar, guard| {
            condvar.wait_deadline(guard, deadline).unwrap().1
        });

        // A notification ends it long before then, leaving no deadline listed
        pair.1.notify_one();
        assert!(!waiting.join().unwrap().timed_out(), "{clock:?}");
        assert_eq!(tester.next_deadline(), None, "{clock:?}");
    }
}

// Deadlines already reached return at once, timed out, without parking.
#[test]
fn test_wait_deadline_returns_for_reached_deadlines() {
    // Move a test clock past a known instant, which is in the past for both clocks
    let mut tester = TestClock::new();
    let past = tester.clock().now();
    tester.advance(Duration::from_secs(1));

    // A past and the current deadline both return at once, timed out, with the guard
    for clock in [Clock::real(), tester.clock()] {
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
    let mut tester = TestClock::new();
    let clock = tester.clock();
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
    assert_eq!(tester.next_deadline(), None);
    tester.advance_to(deadline);
    resume.send(()).unwrap();

    // The wait returns at this time instead of waiting for another advance
    assert!(waiting.join().unwrap().timed_out());
    assert_eq!(clock.now(), deadline);
    assert_eq!(tester.next_deadline(), None);
}

// A deadline worker picks up an earlier deadline and times out at it.
#[test]
fn test_deadline_worker_recomputes_earliest_deadline() {
    // Start a worker on the earliest of a set of deadlines, reporting each pick
    let mut tester = TestClock::new();
    let clock = tester.clock();
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
    tester.wait_blocked(1);
    assert_eq!(tester.next_deadline(), Some(later));

    // Publish an earlier deadline under the mutex and notify the worker
    pair.0.lock().unwrap().push(earlier);
    pair.1.notify_one();
    assert_eq!(picks.recv().unwrap(), earlier);
    tester.wait_blocked(1);
    assert_eq!(tester.next_deadline(), Some(earlier));

    // Advancing to the new deadline alone ends the worker
    tester.advance_to(earlier);
    assert_eq!(waiting.join().unwrap(), earlier);
    assert_eq!(tester.next_deadline(), None);
}

// A notification between the caller's check and the park is not lost.
#[test]
fn test_notification_between_predicate_and_park_is_observed() {
    // Stop the first wait after its predicate runs, before it parks
    let tester = TestClock::new();
    let clock = tester.clock();
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
        let tester = TestClock::new();
        let deadline = tester.clock().now() + Duration::from_secs(5);
        let pair = Arc::new((Mutex::new(7), Condvar::new(&tester.clock())));
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
        tester.wait_blocked(1);

        // Poison the mutex while the wait has it released, then notify the wait
        poison(|| pair.0.lock().unwrap());
        pair.1.notify_one();

        // The wait still hands back the guard, and leaves no deadline listed
        assert_eq!(waiting.join().unwrap(), 7, "{timed}");
        assert!(pair.0.is_poisoned(), "{timed}");
        assert_eq!(tester.next_deadline(), None, "{timed}");
    }
}

// A woken wait unlists its deadline at once, while its result reads the clock
// after the relock.
#[test]
fn test_timeout_result_uses_clock_after_relocking() {
    // Park a timed wait
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let deadline = clock.now() + Duration::from_secs(5);
    let pair = Arc::new((Mutex::new(()), Condvar::new(&clock)));
    let waiting = start_wait(&pair, move |condvar, guard| {
        condvar.wait_deadline(guard, deadline).unwrap().1
    });
    tester.wait_blocked(1);

    // Notify it while holding the mutex, so it wakes and then waits to relock
    let guard = pair.0.lock().unwrap();
    pair.1.notify_one();
    wait_unblocked(&clock);
    assert_eq!(tester.next_deadline(), None);

    // Reach the deadline before the relock, so the notified wait reports a timeout
    tester.advance_to(deadline);
    drop(guard);
    assert!(waiting.join().unwrap().timed_out());
}

// Parked sleeps and deadline waits, never untimed waits, list their deadlines
// until their parks end.
#[test]
fn test_next_deadline_tracks_only_parked_timed_waits() {
    // An untimed wait lists no deadline
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let start = clock.now();
    assert_eq!(tester.next_deadline(), None);
    let untimed = Arc::new((Mutex::new(()), Condvar::new(&clock)));
    let untimed_thread = start_wait(&untimed, |condvar, guard| {
        drop(condvar.wait(guard).unwrap())
    });
    tester.wait_blocked(1);
    assert_eq!(tester.next_deadline(), None);

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
    tester.wait_blocked(5);
    assert_eq!(tester.next_deadline(), Some(start + Duration::from_secs(1)));

    // Ending the earliest wait exposes the deadline its sibling shares with a sleep
    let mut waits = waits.into_iter();
    let (pair, waiting) = waits.next().unwrap();
    pair.1.notify_one();
    assert!(!waiting.join().unwrap().timed_out());
    assert_eq!(tester.next_deadline(), Some(start + Duration::from_secs(3)));

    // Ending the sibling leaves the sleep's entry at the same instant
    let (pair, waiting) = waits.next().unwrap();
    pair.1.notify_one();
    assert!(!waiting.join().unwrap().timed_out());
    assert_eq!(tester.next_deadline(), Some(start + Duration::from_secs(3)));

    // End the untimed wait, so that only the last sleep parks again after the advance
    untimed.1.notify_one();
    untimed_thread.join().unwrap();

    // Catch the last sleep on its way back to park, once the first sleep has returned
    let (checked, resume) = pause_before_park(&clock);
    tester.advance(Duration::from_secs(3));
    sleep_thread.join().unwrap();
    assert_eq!(checked.recv().unwrap(), None);
    assert_eq!(tester.next_deadline(), None);
    resume.send(()).unwrap();
    tester.wait_blocked(1);
    assert_eq!(tester.next_deadline(), Some(start + Duration::from_secs(7)));

    // Ending the last sleep leaves no deadline behind
    tester.advance_to(start + Duration::from_secs(7));
    until_thread.join().unwrap();
    assert_eq!(tester.next_deadline(), None);
}

// Dropping a condvar ends its registration with the clock.
#[test]
fn test_condvar_registry_tracks_lifetime() {
    let tester = TestClock::new();
    let clock = tester.clock();
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

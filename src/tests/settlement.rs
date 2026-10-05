// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Registered waits and parked wait barriers through controlled worker schedules.

use super::helpers::{pause_before_rewait, registered};
use crate::sync::{Condvar, Mutex};
use crate::{Clock, TestClock, Waiter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

/// Holds a registered wait before its next notification or deadline check.
fn hold(clock: &Clock, waiter: &Waiter) -> mpsc::SyncSender<()> {
    let (checked, resume) = pause_before_rewait(clock);
    waiter.signal.wake();
    checked.recv().unwrap();
    resume
}

/// Reports whether the clock can satisfy a parked wait barrier.
fn parked(clock: &Clock, count: usize) -> bool {
    clock
        .paused
        .as_ref()
        .unwrap()
        .state
        .lock()
        .unwrap()
        .parked(count)
}

/// Resumes a held worker when the barrier blocks and reports whether it had to wait.
fn wait_then_resume(tester: &TestClock, count: usize, resume: mpsc::SyncSender<()>) -> bool {
    // Release the worker only after the public barrier observes pending work
    let clock = tester.clock();
    let paused = clock.paused.as_ref().unwrap();
    let waited = Arc::new(AtomicBool::new(false));
    *paused.before_parked_wait.lock().unwrap() = Some(Box::new({
        let waited = waited.clone();
        let resume = resume.clone();
        move || {
            waited.store(true, Ordering::SeqCst);
            resume.send(()).unwrap();
        }
    }));
    tester.wait_parked(count);

    // Release the worker for cleanup even if the barrier returned too early
    let waited = waited.load(Ordering::SeqCst);
    if !waited {
        paused.before_parked_wait.lock().unwrap().take();
        resume.send(()).unwrap();
    }
    waited
}

/// An outstanding broadcast preserves registration while holding the parked wait barrier.
#[test]
fn test_notification_keeps_registration_but_holds_parked_barrier() {
    // Hold a worker on a deadline that its next wait will replace
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let first = clock.now() + Duration::from_secs(5);
    let later = clock.now() + Duration::from_secs(9);
    let waiter = Arc::new(clock.waiter());
    let waiting = thread::spawn({
        let waiter = waiter.clone();
        move || {
            assert!(waiter.wait(Some(first)));
            assert!(!waiter.wait(Some(later)));
        }
    });
    tester.wait_registered(1);
    let resume = hold(&clock, &waiter);

    // A notification changes the barrier without hiding the known deadline
    waiter.signal.notify_all();
    assert_eq!(registered(&clock), 1);
    assert_eq!(tester.next_deadline(), Some(first));
    assert!(!parked(&clock, 1));
    tester.wait_registered(1);
    let waited = wait_then_resume(&tester, 1, resume);
    let next = tester.next_deadline();

    // Reaching the replacement deadline finishes the worker normally
    tester.advance_to(later);
    waiting.join().unwrap();
    assert!(waited);
    assert_eq!(next, Some(later));
    assert!(parked(&clock, 0));
}

/// A notified condvar worker picks its new deadline before the parked wait barrier returns.
#[test]
fn test_condvar_deadline_change_holds_parked_barrier() {
    // Start a worker that reads its deadline under the condvar's mutex
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let earlier = clock.now() + Duration::from_secs(1);
    let first = clock.now() + Duration::from_secs(5);
    let later = clock.now() + Duration::from_secs(9);
    let pair = Arc::new((Mutex::new(first), Condvar::new(&clock)));
    let worker = thread::spawn({
        let pair = pair.clone();
        move || {
            // Wait for the driver to replace the first deadline
            let guard = pair.0.lock().unwrap();
            let deadline = *guard;
            let (guard, result) = pair.1.wait_deadline(guard, deadline).unwrap();
            assert!(!result.timed_out());

            // Pick the replacement deadline after the condvar relocks the mutex
            let deadline = *guard;
            let (_, result) = pair.1.wait_deadline(guard, deadline).unwrap();
            result.timed_out()
        }
    });
    tester.wait_registered(1);

    // Register an earlier wait so its expiry can wake the worker without notifying it
    let earlier_wait = thread::spawn({
        let pair = pair.clone();
        move || {
            let (_, result) = pair
                .1
                .wait_deadline(pair.0.lock().unwrap(), earlier)
                .unwrap();
            result.timed_out()
        }
    });
    tester.wait_registered(2);
    let (checked, resume) = pause_before_rewait(&clock);
    tester.advance_to(earlier);
    checked.recv().unwrap();
    assert!(earlier_wait.join().unwrap());

    // Notify the held worker and require the barrier to wait for its new registration
    *pair.0.lock().unwrap() = later;
    pair.1.notify_all();
    assert_eq!(tester.next_deadline(), Some(first));
    let waited = wait_then_resume(&tester, 1, resume);
    let next = tester.next_deadline();

    // Finish the worker before checking the barrier's observation
    tester.advance_to(later);
    assert!(worker.join().unwrap());
    assert!(waited);
    assert_eq!(next, Some(later));
}

/// A reached wait stays registered while holding the parked wait barrier.
#[test]
fn test_reached_registration_cannot_satisfy_parked_barrier() {
    // Hold the first timed wait before its thread can observe expiry
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let first = clock.now() + Duration::from_secs(1);
    let later = clock.now() + Duration::from_secs(9);
    let waiter = Arc::new(clock.waiter());
    let waiting = thread::spawn({
        let waiter = waiter.clone();
        move || {
            assert!(!waiter.wait(Some(first)));
            assert!(!waiter.wait(Some(later)));
        }
    });
    tester.wait_registered(1);
    let resume = hold(&clock, &waiter);

    // Expiry holds the parked wait barrier before the worker checks the clock
    tester.advance_to(first);
    assert_eq!(registered(&clock), 1);
    assert_eq!(tester.next_deadline(), Some(first));
    assert!(!parked(&clock, 1));
    tester.wait_registered(1);
    let waited = wait_then_resume(&tester, 1, resume);
    let next = tester.next_deadline();

    // Finish the worker before checking the barrier's observation
    tester.advance_to(later);
    waiting.join().unwrap();
    assert!(waited);
    assert_eq!(next, Some(later));
}

/// A single notification leaves the unselected worker counted and listed.
#[test]
fn test_single_notification_leaves_unselected_wait_parked() {
    // Hold both waits independently before sending their shared notification
    let mut tester = TestClock::new();
    let clock = tester.clock();
    let deadline = clock.now() + Duration::from_secs(5);
    let waiter = Arc::new(clock.waiter());
    let first = thread::spawn({
        let waiter = waiter.clone();
        move || waiter.wait(Some(deadline))
    });
    tester.wait_registered(1);
    let first_resume = hold(&clock, &waiter);
    let second = thread::spawn({
        let waiter = waiter.clone();
        move || waiter.wait(Some(deadline))
    });
    tester.wait_registered(2);
    let second_resume = hold(&clock, &waiter);

    // One credit holds the parked wait barrier until its recipient returns
    waiter.signal.notify_one();
    assert_eq!(registered(&clock), 2);
    assert_eq!(tester.next_deadline(), Some(deadline));
    assert!(!parked(&clock, 1));
    tester.wait_registered(2);
    let waited = wait_then_resume(&tester, 1, first_resume);
    assert!(first.join().unwrap());
    assert_eq!(registered(&clock), 1);
    assert_eq!(tester.next_deadline(), Some(deadline));

    // The unselected wait remains parked until the driver reaches its deadline
    tester.advance_to(deadline);
    second_resume.send(()).unwrap();
    assert!(!second.join().unwrap());
    assert!(waited);
}

/// A notification on another condvar holds the barrier despite enough unreached waits.
#[test]
fn test_parked_barrier_waits_for_notifications_on_any_condvar() {
    for broadcast in [false, true] {
        // Register two independent untimed waits on the same clock
        let tester = TestClock::new();
        let clock = tester.clock();
        let stable = Arc::new(clock.waiter());
        let changed = Arc::new(clock.waiter());
        let stable_thread = thread::spawn({
            let waiter = stable.clone();
            move || waiter.wait(None)
        });
        let changed_thread = thread::spawn({
            let waiter = changed.clone();
            move || waiter.wait(None)
        });
        tester.wait_registered(2);
        let resume = hold(&clock, &changed);

        // Hold the notified wait while the other wait could satisfy the count alone
        if broadcast {
            changed.signal.notify_all();
        } else {
            changed.signal.notify_one();
        }
        tester.wait_registered(2);
        let waited = wait_then_resume(&tester, 1, resume);

        // Finish both workers before checking whether the barrier waited
        assert!(changed_thread.join().unwrap());
        stable.signal.notify_all();
        assert!(stable_thread.join().unwrap());
        assert!(waited, "{broadcast}");
    }
}

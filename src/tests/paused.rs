// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Checks timer delivery, retention and receive precedence without real deadlines.

use super::*;
use crate::tests::helpers::{blocked, pause_before_park};
use crossbeam_channel::{bounded, select};
use std::thread;
use std::time::UNIX_EPOCH;

// A timer fires exactly at its deadline and leaves the armed registry immediately.
#[test]
fn test_at_fires_at_exact_deadline() {
    // Arm a timer five seconds ahead on a stopped clock
    let mut test = TestClock::new();
    let clock = test.clock();
    let deadline = clock.now() + Duration::from_secs(5);
    let timer = clock.at(deadline);
    test.wait_timers(1);
    assert_eq!(test.next_deadline(), Some(deadline));
    assert_eq!(timer.try_recv(), Err(TryRecvError::Empty));

    // A short advance keeps the timer empty and armed
    test.advance(Duration::from_secs(4));
    assert_eq!(timer.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(test.paused.lock().timers.len(), 1);

    // Reaching the deadline delivers before returning and unlists the timer
    test.advance_to(deadline);
    assert_eq!(timer.try_recv(), Ok(deadline));
    assert!(test.paused.lock().timers.is_empty());
    assert_eq!(test.next_deadline(), None);
}

// One advance publishes both times before sending every due timer in deadline order.
#[test]
fn test_advance_publishes_time_and_fires_all_due_timers_in_order() {
    // Arm timers out of order, with two equal deadlines and one dropped receiver
    let mut test = TestClock::new();
    let clock = test.clock();
    let start = clock.now();
    test.set_system_time(UNIX_EPOCH);
    let late = clock.at(start + Duration::from_secs(3));
    let first = clock.at(start + Duration::from_secs(1));
    let equal = clock.at(start + Duration::from_secs(3));
    drop(clock.at(start + Duration::from_secs(2)));
    let future = clock.at(start + Duration::from_secs(7));
    test.wait_timers(5);

    // Inspect the very first send before the advance can send anything else
    let target = start + Duration::from_secs(5);
    *test.paused.after_timer_send.lock().unwrap() = Some(Box::new({
        let clock = clock.clone();
        let (first, late, equal) = (first.clone(), late.clone(), equal.clone());
        move || {
            assert_eq!(clock.now(), target);
            assert_eq!(clock.system_time(), UNIX_EPOCH + Duration::from_secs(5));
            assert_eq!(first.len(), 1);
            assert_eq!(late.try_recv(), Err(TryRecvError::Empty));
            assert_eq!(equal.try_recv(), Err(TryRecvError::Empty));
        }
    }));

    // An overshoot sends the deadlines themselves, keeping only delivered senders
    test.advance_to(target);
    assert_eq!(first.try_recv(), Ok(start + Duration::from_secs(1)));
    assert_eq!(late.try_recv(), Ok(start + Duration::from_secs(3)));
    assert_eq!(equal.try_recv(), Ok(start + Duration::from_secs(3)));
    assert_eq!(future.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(test.paused.lock().fired.len(), 3);
    assert_eq!(test.paused.lock().timers.len(), 1);
    assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(7)));
}

// Past deadlines, current deadlines and zero durations deliver without an advance.
#[test]
fn test_reached_timers_deliver_immediately() {
    // Move beyond a known instant before constructing the timers
    let mut test = TestClock::new();
    let clock = test.clock();
    let past = clock.now();
    test.advance(Duration::from_secs(2));
    let now = clock.now();

    // Each already-due timer sends once and stays connected after consumption
    for (name, timer, deadline) in [
        ("past", clock.at(past), past),
        ("current", clock.at(now), now),
        ("zero", clock.after(Duration::ZERO), now),
    ] {
        assert_eq!(timer.capacity(), Some(1), "{name}");
        assert_eq!(timer.try_recv(), Ok(deadline), "{name}");
        assert_eq!(timer.try_recv(), Err(TryRecvError::Empty), "{name}");
    }

    // Immediate deliveries never appear among unfired timers
    assert!(test.paused.lock().timers.is_empty());
    assert_eq!(test.paused.lock().fired.len(), 3);
    assert_eq!(test.next_deadline(), None);
}

// Relative timers take their deadline from the clock at each call.
#[test]
fn test_after_uses_time_at_call() {
    // Create equal durations on opposite sides of an advance
    let mut test = TestClock::new();
    let clock = test.clock();
    let start = clock.now();
    let first = clock.after(Duration::from_secs(5));
    test.advance(Duration::from_secs(2));
    let second = clock.after(Duration::from_secs(5));

    // The first deadline does not reach the second timer
    test.advance_to(start + Duration::from_secs(5));
    assert_eq!(first.try_recv(), Ok(start + Duration::from_secs(5)));
    assert_eq!(second.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(7)));

    // The second timer fires five seconds after its own call
    test.advance(Duration::from_secs(2));
    assert_eq!(second.try_recv(), Ok(start + Duration::from_secs(7)));
    assert_eq!(first.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(test.next_deadline(), None);
}

// An overflowing duration returns a never receiver, arming no timer.
#[test]
fn test_after_overflow_never_fires_or_counts() {
    // Request a duration outside the platform's instant range
    let mut test = TestClock::new();
    let clock = test.clock();
    assert!(clock.now().checked_add(Duration::MAX).is_none());
    let timer = clock.after(Duration::MAX);
    assert_eq!(timer.capacity(), Some(0));
    assert!(test.paused.lock().timers.is_empty());
    assert_eq!(test.next_deadline(), None);

    // Neither advancing nor dropping the shared state makes the receiver ready
    test.advance(Duration::from_secs(60));
    assert_eq!(timer.try_recv(), Err(TryRecvError::Empty));
    assert!(test.paused.lock().fired.is_empty());
    drop(test);
    drop(clock);
    assert_eq!(timer.try_recv(), Err(TryRecvError::Empty));
}

// Consumed timers stay connected until the owner and every clock handle have dropped.
#[test]
fn test_timer_connection_lasts_as_long_as_shared_clock() {
    for owner_last in [false, true] {
        // Fire a capacity-1 timer while keeping two handles alive
        let mut test = TestClock::new();
        let clock = test.clock();
        let clone = clock.clone();
        let deadline = clock.now() + Duration::from_secs(1);
        let timer = clock.at(deadline);
        test.advance_to(deadline);
        assert_eq!(timer.capacity(), Some(1), "{owner_last}");
        assert_eq!(timer.try_recv(), Ok(deadline), "{owner_last}");

        // Either the owner alone or a handle alone keeps the consumed timer connected
        drop(clock);
        if owner_last {
            drop(clone);
            assert_eq!(timer.try_recv(), Err(TryRecvError::Empty), "{owner_last}");
            drop(test);
        } else {
            drop(test);
            assert_eq!(timer.try_recv(), Err(TryRecvError::Empty), "{owner_last}");
            drop(clone);
        }
        assert_eq!(
            timer.try_recv(),
            Err(TryRecvError::Disconnected),
            "{owner_last}"
        );
    }
}

// Wall jumps and zero advances neither fire timers nor change their armed count.
#[test]
fn test_wall_jumps_and_zero_advances_leave_timers_armed() {
    // Arm a deadline that only a monotonic advance can reach
    let mut test = TestClock::new();
    let clock = test.clock();
    let now = clock.now();
    let deadline = now + Duration::from_secs(1);
    let timer = clock.at(deadline);

    // Move wall time both ways and exercise both forms of zero advance
    for seconds in [100, 50] {
        test.set_system_time(UNIX_EPOCH + Duration::from_secs(seconds));
        test.advance(Duration::ZERO);
        test.advance_to(now);
        assert_eq!(timer.try_recv(), Err(TryRecvError::Empty), "{seconds}");
        assert_eq!(test.paused.lock().timers.len(), 1, "{seconds}");
        assert_eq!(test.next_deadline(), Some(deadline), "{seconds}");
    }

    // The original monotonic deadline still fires normally
    test.advance_to(deadline);
    assert_eq!(timer.try_recv(), Ok(deadline));
}

// A rejected advance leaves timers and both times untouched and usable.
#[test]
fn test_invalid_advances_leave_timers_armed() {
    // Arm a timer and place wall time at its last whole second
    let mut test = TestClock::new();
    let clock = test.clock();
    let now = clock.now();
    let deadline = now + Duration::from_secs(1);
    let timer = clock.at(deadline);
    let last = crate::tests::helpers::last_system_time();
    test.set_system_time(last);

    // Backward and overflowing advances reject before changing any timer state
    for case in ["backward", "monotonic overflow", "wall overflow"] {
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match case {
                    "backward" => test.advance_to(now - Duration::from_secs(1)),
                    "monotonic overflow" => test.advance(Duration::MAX),
                    _ => test.advance_to(deadline),
                }
            }))
            .is_err(),
            "{case}"
        );
        assert_eq!(clock.now(), now, "{case}");
        assert_eq!(clock.system_time(), last, "{case}");
        assert_eq!(timer.try_recv(), Err(TryRecvError::Empty), "{case}");
        assert_eq!(test.paused.lock().timers.len(), 1, "{case}");
    }

    // Restoring wall time lets the original timer fire
    test.set_system_time(UNIX_EPOCH);
    test.advance_to(deadline);
    assert_eq!(timer.try_recv(), Ok(deadline));
}

// Timer counts mean at least and exclude fired timers and returned adapters.
#[test]
fn test_wait_timers_counts_only_armed_timers() {
    // Keep one timer armed and fire another before waiting for a new registration
    let mut test = TestClock::new();
    let clock = test.clock();
    let start = clock.now();
    test.wait_timers(0);
    let public = clock.at(start + Duration::from_secs(9));
    let fired = clock.at(start + Duration::from_secs(1));
    test.wait_timers(1);
    test.wait_timers(2);
    test.advance(Duration::from_secs(1));
    assert_eq!(fired.try_recv(), Ok(start + Duration::from_secs(1)));
    assert_eq!(test.paused.lock().timers.len(), 1);

    // A driver waits for a second armed timer before releasing a receive adapter
    let (sender, receiver) = bounded(0);
    let driver = thread::spawn(move || {
        test.wait_timers(2);
        assert_eq!(test.paused.lock().timers.len(), 2);
        sender.send(7).unwrap();
        test
    });
    assert_eq!(clock.recv_timeout(&receiver, Duration::from_secs(5)), Ok(7));
    let mut test = driver.join().unwrap();

    // The returned adapter leaves only the original public timer
    test.wait_timers(1);
    assert_eq!(test.paused.lock().timers.len(), 1);
    assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(9)));
    test.advance_to(start + Duration::from_secs(9));
    assert_eq!(public.try_recv(), Ok(start + Duration::from_secs(9)));
    assert!(test.paused.lock().timers.is_empty());
}

// The next deadline is the earliest timer or parked wait, and fired timers leave it.
#[test]
fn test_next_deadline_combines_timers_and_parked_waits() {
    // Park a sleep between two timer deadlines
    let mut test = TestClock::new();
    let clock = test.clock();
    let start = clock.now();
    let first = clock.at(start + Duration::from_secs(1));
    let last = clock.at(start + Duration::from_secs(3));
    let waiting = thread::spawn({
        let clock = clock.clone();
        move || clock.sleep_until(start + Duration::from_secs(2))
    });
    test.wait_blocked(1);
    test.wait_timers(2);
    assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(1)));

    // Hold the sleep between parks to observe the remaining timer on its own
    let (checked, resume) = pause_before_park(&clock);
    test.advance(Duration::from_secs(1));
    assert_eq!(first.try_recv(), Ok(start + Duration::from_secs(1)));
    assert_eq!(checked.recv().unwrap(), None);
    assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(3)));

    // Parking again puts the sleep ahead of the remaining timer
    resume.send(()).unwrap();
    test.wait_blocked(1);
    assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(2)));

    // Ending the sleep exposes the final timer, whose firing empties the deadlines
    test.advance_to(start + Duration::from_secs(2));
    waiting.join().unwrap();
    assert_eq!(test.next_deadline(), Some(start + Duration::from_secs(3)));
    test.advance_to(start + Duration::from_secs(3));
    assert_eq!(last.try_recv(), Ok(start + Duration::from_secs(3)));
    assert_eq!(test.next_deadline(), None);
}

// An advance wakes a select on a clock timer without counting it as a clock park.
#[test]
fn test_select_wakes_on_clock_timer() {
    // Create the timer on the selecting thread and observe its registration
    let mut test = TestClock::new();
    let clock = test.clock();
    let deadline = clock.now() + Duration::from_secs(5);
    let waiting = thread::spawn({
        let clock = clock.clone();
        move || {
            select! {
                recv(clock.at(deadline)) -> result => (result.unwrap(), clock.now()),
                recv(crossbeam_channel::never::<()>()) -> _ => unreachable!(),
            }
        }
    });
    test.wait_timers(1);
    assert_eq!(blocked(&clock), 0);

    // Sending the timer message wakes the select with the published clock time
    test.advance_to(deadline);
    assert_eq!(waiting.join().unwrap(), (deadline, deadline));
    assert!(test.paused.lock().timers.is_empty());
}

// Both adapters prefer buffered messages and disconnections to reached deadlines.
#[test]
fn test_receive_checks_channel_before_reached_deadline() {
    for relative in [false, true] {
        // Buffer a message on a disconnected channel at the reached deadline
        let test = TestClock::new();
        let clock = test.clock();
        let (sender, receiver) = bounded(1);
        sender.send(7).unwrap();
        drop(sender);
        let receive = || {
            if relative {
                clock.recv_timeout(&receiver, Duration::ZERO)
            } else {
                clock.recv_deadline(&receiver, clock.now())
            }
        };

        // The message comes first, then the disconnection, and no timer is armed
        assert_eq!(receive(), Ok(7), "{relative}");
        assert_eq!(receive(), Err(RecvTimeoutError::Disconnected), "{relative}");
        assert!(test.paused.lock().timers.is_empty(), "{relative}");
        assert!(test.paused.lock().fired.is_empty(), "{relative}");
    }
}

// Empty connected channels time out at reached deadlines without registering timers.
#[test]
fn test_receive_reached_deadline_times_out_without_timer() {
    // Keep an empty channel connected after advancing past a known instant
    let mut test = TestClock::new();
    let clock = test.clock();
    let past = clock.now();
    let (_sender, receiver) = bounded::<()>(1);
    test.advance(Duration::from_secs(1));

    // Past and current deadlines and a zero duration all expire immediately
    for deadline in [past, clock.now()] {
        assert_eq!(
            clock.recv_deadline(&receiver, deadline),
            Err(RecvTimeoutError::Timeout),
            "{deadline:?}"
        );
    }
    assert_eq!(
        clock.recv_timeout(&receiver, Duration::ZERO),
        Err(RecvTimeoutError::Timeout)
    );
    assert!(test.paused.lock().timers.is_empty());
    assert!(test.paused.lock().fired.is_empty());
}

// A message or disconnection ends a waiting receive, removing only its own timer.
#[test]
fn test_receive_message_or_disconnect_disarms_timer() {
    for relative in [false, true] {
        for disconnect in [false, true] {
            // Share a deadline with a public timer to exercise independent removal
            let mut test = TestClock::new();
            let clock = test.clock();
            let deadline = clock.now() + Duration::from_secs(5);
            let public = clock.at(deadline);
            let (sender, receiver) = bounded(0);
            let waiting = thread::spawn(move || {
                if relative {
                    clock.recv_timeout(&receiver, Duration::from_secs(5))
                } else {
                    clock.recv_deadline(&receiver, deadline)
                }
            });
            test.wait_timers(2);

            // Complete the receiver without advancing its clock
            if disconnect {
                drop(sender);
            } else {
                sender.send(7).unwrap();
            }
            let expected = if disconnect {
                Err(RecvTimeoutError::Disconnected)
            } else {
                Ok(7)
            };
            assert_eq!(waiting.join().unwrap(), expected, "{relative} {disconnect}");
            assert_eq!(
                test.paused.lock().timers.len(),
                1,
                "{relative} {disconnect}"
            );
            assert!(
                test.paused.lock().fired.is_empty(),
                "{relative} {disconnect}"
            );

            // The public timer still fires and no adapter sender is retained
            test.advance_to(deadline);
            assert_eq!(public.try_recv(), Ok(deadline), "{relative} {disconnect}");
            assert!(
                test.paused.lock().timers.is_empty(),
                "{relative} {disconnect}"
            );
            assert_eq!(test.paused.lock().fired.len(), 1, "{relative} {disconnect}");
            assert_eq!(test.next_deadline(), None, "{relative} {disconnect}");
        }
    }
}

// A waiting receive outlasts a short advance and times out exactly at its deadline.
#[test]
fn test_receive_times_out_at_exact_deadline() {
    for relative in [false, true] {
        // Advance before the call so relative timeouts must use the current clock
        let mut test = TestClock::new();
        test.advance(Duration::from_secs(2));
        let clock = test.clock();
        let deadline = clock.now() + Duration::from_secs(5);
        let (_sender, receiver) = bounded::<()>(1);
        let waiting = thread::spawn(move || {
            let result = if relative {
                clock.recv_timeout(&receiver, Duration::from_secs(5))
            } else {
                clock.recv_deadline(&receiver, deadline)
            };
            (result, clock.now())
        });
        test.wait_timers(1);
        assert_eq!(test.next_deadline(), Some(deadline), "{relative}");

        // A partial advance cannot deliver the timeout
        test.advance(Duration::from_secs(4));
        assert_eq!(test.paused.lock().timers.len(), 1, "{relative}");
        assert!(!waiting.is_finished(), "{relative}");

        // Reaching the deadline ends the receive with no armed or retained timer
        test.advance_to(deadline);
        assert_eq!(
            waiting.join().unwrap(),
            (Err(RecvTimeoutError::Timeout), deadline),
            "{relative}"
        );
        assert!(test.paused.lock().timers.is_empty(), "{relative}");
        assert!(test.paused.lock().fired.is_empty(), "{relative}");
        assert_eq!(test.next_deadline(), None, "{relative}");
    }
}

// A receive checks its receiver again after its timer wins the select.
#[test]
fn test_receive_rechecks_channel_after_timer_wins() {
    for relative in [false, true] {
        for disconnect in [false, true] {
            // Make the receiver ready only after the timer has been selected
            let mut test = TestClock::new();
            let clock = test.clock();
            let deadline = clock.now() + Duration::from_secs(5);
            let (sender, receiver) = bounded(1);
            *test.paused.after_timer_receive.lock().unwrap() = Some(Box::new({
                let clock = clock.clone();
                move || {
                    assert_eq!(clock.now(), deadline);
                    assert!(clock.paused.as_ref().unwrap().lock().timers.is_empty());
                    if !disconnect {
                        sender.send(7).unwrap();
                    }
                    drop(sender);
                }
            }));
            let waiting = thread::spawn(move || {
                if relative {
                    clock.recv_timeout(&receiver, Duration::from_secs(5))
                } else {
                    clock.recv_deadline(&receiver, deadline)
                }
            });
            test.wait_timers(1);

            // The final receiver check must override the timer's timeout
            test.advance_to(deadline);
            let expected = if disconnect {
                Err(RecvTimeoutError::Disconnected)
            } else {
                Ok(7)
            };
            assert_eq!(waiting.join().unwrap(), expected, "{relative} {disconnect}");
            assert!(
                test.paused.lock().timers.is_empty(),
                "{relative} {disconnect}"
            );
            assert!(
                test.paused.lock().fired.is_empty(),
                "{relative} {disconnect}"
            );
            assert_eq!(test.next_deadline(), None, "{relative} {disconnect}");
        }
    }
}

// An overflowing receive timeout waits like an untimed receive, beyond any advance.
#[test]
fn test_receive_timeout_overflow_waits_without_timer() {
    for disconnect in [false, true] {
        // Start an overflowing receive on a rendezvous channel
        let mut test = TestClock::new();
        let clock = test.clock();
        assert!(clock.now().checked_add(Duration::MAX).is_none());
        let (sender, receiver) = bounded(0);
        let waiting = thread::spawn(move || clock.recv_timeout(&receiver, Duration::MAX));

        // An advance leaves it waiting, and only its channel ends it
        test.advance(Duration::from_secs(60));
        if disconnect {
            drop(sender);
        } else {
            sender.send(7).unwrap();
        }
        let expected = if disconnect {
            Err(RecvTimeoutError::Disconnected)
        } else {
            Ok(7)
        };
        assert_eq!(waiting.join().unwrap(), expected, "{disconnect}");
        assert!(test.paused.lock().timers.is_empty(), "{disconnect}");
        assert!(test.paused.lock().fired.is_empty(), "{disconnect}");
    }
}

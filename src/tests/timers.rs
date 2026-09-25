// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Checks the real clock's timers and receives against crossbeam's, without waiting.

use super::*;
use crossbeam_channel::{TryRecvError, bounded};

// Real timers keep crossbeam's immediate delivery, capacity and connection semantics.
#[test]
fn test_real_timers_match_crossbeam_immediate_delivery() {
    // Use a reached instant so neither implementation waits on real time
    let clock = Clock::real();
    let deadline = clock.now();
    for (name, timer) in [
        ("clock", clock.at(deadline)),
        ("crossbeam", crossbeam_channel::at(deadline)),
    ] {
        assert_eq!(timer.capacity(), Some(1), "{name}");
        assert_eq!(timer.try_recv(), Ok(deadline), "{name}");
        assert_eq!(timer.try_recv(), Err(TryRecvError::Empty), "{name}");
    }

    // Zero-duration timers deliver an instant bounded by the surrounding reads
    let before = clock.now();
    let timers = [
        clock.after(Duration::ZERO),
        crossbeam_channel::after(Duration::ZERO),
    ];
    let after = clock.now();
    for (index, timer) in timers.into_iter().enumerate() {
        assert!(
            (before..=after).contains(&timer.try_recv().unwrap()),
            "{index}"
        );
        assert_eq!(timer.try_recv(), Err(TryRecvError::Empty), "{index}");
    }

    // Overflow keeps the never receiver's zero capacity and empty connection
    for (name, timer) in [
        ("clock", clock.after(Duration::MAX)),
        ("crossbeam", crossbeam_channel::after(Duration::MAX)),
    ] {
        assert_eq!(timer.capacity(), Some(0), "{name}");
        assert_eq!(timer.try_recv(), Err(TryRecvError::Empty), "{name}");
    }
}

// Real-clock receives keep crossbeam's precedence of messages and disconnections.
#[test]
fn test_real_receive_adapters_match_crossbeam_precedence() {
    for relative in [false, true] {
        // Feed identical messages to the adapter and to crossbeam directly
        let clock = Clock::real();
        let deadline = clock.now();
        let (sender, receiver) = bounded(1);
        let (reference_sender, reference) = bounded(1);
        let receive = || {
            if relative {
                clock.recv_timeout(&receiver, Duration::ZERO)
            } else {
                clock.recv_deadline(&receiver, deadline)
            }
        };
        let direct = || {
            if relative {
                reference.recv_timeout(Duration::ZERO)
            } else {
                reference.recv_deadline(deadline)
            }
        };
        sender.send(7).unwrap();
        reference_sender.send(7).unwrap();
        assert_eq!(receive(), direct(), "{relative}");

        // Empty connected channels expire immediately at the reached deadline
        assert_eq!(receive(), direct(), "{relative}");
        assert_eq!(receive(), Err(RecvTimeoutError::Timeout), "{relative}");

        // Disconnection takes precedence over the same deadline
        drop(sender);
        drop(reference_sender);
        assert_eq!(receive(), direct(), "{relative}");
        assert_eq!(receive(), Err(RecvTimeoutError::Disconnected), "{relative}");
    }

    // Overflow receives a buffered message and then reports disconnection
    let clock = Clock::real();
    let (sender, receiver) = bounded(1);
    sender.send(7).unwrap();
    drop(sender);
    assert_eq!(clock.recv_timeout(&receiver, Duration::MAX), Ok(7));
    assert_eq!(
        clock.recv_timeout(&receiver, Duration::MAX),
        Err(RecvTimeoutError::Disconnected)
    );
}

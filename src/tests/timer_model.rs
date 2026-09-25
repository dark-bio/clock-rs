// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Random operation sequences checked against a timer ledger in integer milliseconds.

#![cfg_attr(coverage_nightly, coverage(off))]

use crate::TestClock;
use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError, bounded};
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;
use std::time::{Duration, Instant, UNIX_EPOCH};

/// One public operation, with small offsets to produce equal and crossed deadlines.
#[derive(Clone, Debug)]
enum Operation {
    /// Creates an absolute timer before, at or after the current time.
    At(i8),
    /// Creates a relative timer, using no offset for an overflowing duration.
    After(Option<u8>),
    /// Moves time by a duration, including zero.
    Advance(u8),
    /// Moves time to an absolute target, including the current instant.
    AdvanceTo(u8),
    /// Sets wall time independently, on either side of the epoch.
    SetSystemTime(i16),
    /// Drops a receiver selected from the history of created timers.
    Drop(u8),
    /// Tries to consume a selected timer's message without blocking.
    TryRecv(u8),
    /// Receives twice from a helper channel at an already reached deadline.
    Receive {
        /// Uses a zero timeout instead of an absolute deadline.
        relative: bool,
        /// Starts with one buffered message.
        buffered: bool,
        /// Keeps the helper channel's sender alive.
        connected: bool,
        /// Places an absolute deadline this many milliseconds in the past.
        past: u8,
    },
}

/// Generates all operations without filtering away any channel state or deadline class.
fn operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        3 => (-4i8..=8).prop_map(Operation::At),
        3 => prop::option::of(0u8..=8).prop_map(Operation::After),
        2 => (0u8..=8).prop_map(Operation::Advance),
        2 => (0u8..=8).prop_map(Operation::AdvanceTo),
        1 => (-100i16..=100).prop_map(Operation::SetSystemTime),
        2 => any::<u8>().prop_map(Operation::Drop),
        3 => any::<u8>().prop_map(Operation::TryRecv),
        2 => (any::<bool>(), any::<bool>(), any::<bool>(), 0u8..=4)
            .prop_map(|(relative, buffered, connected, past)| Operation::Receive {
                relative,
                buffered,
                connected,
                past,
            }),
    ]
}

/// The expected life of one public timer, independent of the clock's internals.
#[derive(Debug)]
struct Timer {
    /// Absolute deadline in model milliseconds, or no deadline for a never timer.
    deadline: Option<i64>,
    /// Whether the test still owns the receiver.
    live: bool,
    /// A deadline delivered to the receiver but not yet consumed.
    message: Option<i64>,
    /// Whether delivery succeeded, requiring a sender for the rest of the clock's life.
    delivered: bool,
}

/// Expected time and timer history, with no ordering tree or sender objects.
struct Model {
    /// Monotonic milliseconds since the test's origin.
    now: i64,
    /// Wall-clock milliseconds since the Unix epoch.
    wall: i64,
    /// Every timer created, including receivers that were dropped.
    timers: Vec<Timer>,
}

impl Model {
    /// Records a new timer and predicts immediate delivery for reached deadlines.
    fn arm(&mut self, deadline: Option<i64>) {
        let delivered = deadline.is_some_and(|deadline| deadline <= self.now);
        self.timers.push(Timer {
            deadline,
            live: true,
            message: deadline.filter(|_| delivered),
            delivered,
        });
    }

    /// Predicts delivery to live receivers as time crosses each deadline.
    fn advance(&mut self, by: u8) {
        // Check each timer in creation order against the elapsed interval
        let next = self.now + i64::from(by);
        for timer in &mut self.timers {
            if timer.live
                && timer
                    .deadline
                    .is_some_and(|deadline| self.now < deadline && deadline <= next)
            {
                timer.message = timer.deadline;
                timer.delivered = true;
            }
        }

        // Both times move by the same duration until a separate wall-time operation
        self.now = next;
        self.wall += i64::from(by);
    }

    /// Predicts one nonblocking receive from a live timer.
    fn receive(&mut self, index: usize) -> Result<i64, TryRecvError> {
        self.timers[index].message.take().ok_or(TryRecvError::Empty)
    }
}

/// Converts the model's nonnegative monotonic milliseconds into an actual deadline.
fn instant(origin: Instant, millis: i64) -> Instant {
    origin + Duration::from_millis(millis.try_into().unwrap())
}

/// Runs a sequence and checks every operation against the public timer contract.
fn check_operations(operations: &[Operation]) -> TestCaseResult {
    // Start beyond the origin so generated past deadlines never need a negative instant
    let mut test = TestClock::new();
    let clock = test.clock();
    let origin = clock.now();
    test.set_system_time(UNIX_EPOCH);
    test.advance(Duration::from_millis(8));
    let mut model = Model {
        now: 8,
        wall: 8,
        timers: Vec::new(),
    };
    let mut receivers: Vec<Option<Receiver<Instant>>> = Vec::new();

    // Apply each generated operation to the clock and the independent ledger
    for (step, operation) in operations.iter().enumerate() {
        match *operation {
            Operation::At(offset) => {
                let deadline = model.now + i64::from(offset);
                receivers.push(Some(clock.at(instant(origin, deadline))));
                model.arm(Some(deadline));
            }
            Operation::After(offset) => {
                let duration =
                    offset.map_or(Duration::MAX, |millis| Duration::from_millis(millis.into()));
                receivers.push(Some(clock.after(duration)));
                model.arm(offset.map(|offset| model.now + i64::from(offset)));
            }
            Operation::Advance(by) => {
                test.advance(Duration::from_millis(by.into()));
                model.advance(by);
            }
            Operation::AdvanceTo(by) => {
                test.advance_to(instant(origin, model.now + i64::from(by)));
                model.advance(by);
            }
            Operation::SetSystemTime(millis) => {
                let duration = Duration::from_millis(millis.unsigned_abs().into());
                test.set_system_time(if millis < 0 {
                    UNIX_EPOCH - duration
                } else {
                    UNIX_EPOCH + duration
                });
                model.wall = millis.into();
            }
            Operation::Drop(index) if !receivers.is_empty() => {
                let index = usize::from(index) % receivers.len();
                receivers[index] = None;
                model.timers[index].live = false;
                model.timers[index].message = None;
            }
            Operation::TryRecv(index) if !receivers.is_empty() => {
                let index = usize::from(index) % receivers.len();
                if let Some(receiver) = &receivers[index] {
                    let expected = model.receive(index).map(|millis| instant(origin, millis));
                    prop_assert_eq!(receiver.try_recv(), expected, "{}: {:?}", step, operation);
                }
            }
            Operation::Receive {
                relative,
                buffered,
                connected,
                past,
            } => {
                // Build all four readiness states, none of which makes a receive wait
                let (sender, receiver) = bounded(1);
                if buffered {
                    sender.send(7).unwrap();
                }
                let _sender = connected.then_some(sender);
                let empty = if connected {
                    RecvTimeoutError::Timeout
                } else {
                    RecvTimeoutError::Disconnected
                };

                // The buffered message wins once, then the connection decides the result
                for expected in [if buffered { Ok(7) } else { Err(empty) }, Err(empty)] {
                    let actual = if relative {
                        clock.recv_timeout(&receiver, Duration::ZERO)
                    } else {
                        clock.recv_deadline(&receiver, instant(origin, model.now - i64::from(past)))
                    };
                    prop_assert_eq!(actual, expected, "{}: {:?}", step, operation);
                }
            }
            Operation::Drop(_) | Operation::TryRecv(_) => {}
        }

        // Derive the registry from the history, counting dropped pending receivers
        let pending: Vec<_> = model
            .timers
            .iter()
            .filter_map(|timer| timer.deadline)
            .filter(|deadline| *deadline > model.now)
            .collect();
        let kept = model.timers.iter().filter(|timer| timer.delivered).count();
        let next = pending.iter().min().map(|&millis| instant(origin, millis));
        prop_assert_eq!(test.next_deadline(), next, "{}: {:?}", step, operation);
        prop_assert_eq!(
            clock.paused.as_ref().unwrap().timer_counts(),
            (pending.len(), kept),
            "{}: {:?}",
            step,
            operation
        );
        prop_assert_eq!(
            clock.now(),
            instant(origin, model.now),
            "{}: {:?}",
            step,
            operation
        );

        // Wall jumps affect no monotonic deadline or timer message
        let wall_duration = Duration::from_millis(model.wall.unsigned_abs());
        let wall = if model.wall < 0 {
            UNIX_EPOCH - wall_duration
        } else {
            UNIX_EPOCH + wall_duration
        };
        prop_assert_eq!(clock.system_time(), wall, "{}: {:?}", step, operation);

        // Inspect queued messages without consuming them
        for (index, receiver) in receivers.iter().enumerate() {
            if let Some(receiver) = receiver {
                let timer = &model.timers[index];
                prop_assert_eq!(
                    receiver.len(),
                    usize::from(timer.message.is_some()),
                    "{}: {}",
                    step,
                    index
                );
                prop_assert_eq!(
                    receiver.capacity(),
                    Some(usize::from(timer.deadline.is_some())),
                    "{}: {}",
                    step,
                    index
                );
            }
        }
    }

    // Read every surviving receiver twice, for its payload and its lasting connection
    for (index, receiver) in receivers.iter().enumerate() {
        if let Some(receiver) = receiver {
            let expected = model.receive(index).map(|millis| instant(origin, millis));
            prop_assert_eq!(receiver.try_recv(), expected, "{}", index);
            prop_assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty), "{}", index);
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    // Random timer histories match the model's deliveries, deadlines and kept senders.
    #[test]
    fn test_timer_bookkeeping_matches_model(operations in prop::collection::vec(operation(), 1..=128)) {
        // Check the generated history and let proptest shrink any failing sequence
        check_operations(&operations)?;
    }
}

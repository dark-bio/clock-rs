// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Helpers that the tests of several modules share.

use crate::{Clock, paused};
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
pub(crate) fn pause_before_rewait(clock: &Clock) -> (Receiver<Option<Instant>>, SyncSender<()>) {
    pause(&clock.paused.as_ref().unwrap().before_rewait)
}

/// Installs a one-shot hook that reports a wait's timer and holds the wait
/// until resumed.
fn pause(hook: &Mutex<Option<paused::BeforePark>>) -> (Receiver<Option<Instant>>, SyncSender<()>) {
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
pub(crate) fn last_system_time() -> SystemTime {
    // Add every power of two that still fits, from the largest down
    let mut time = UNIX_EPOCH;
    for bit in (0..64).rev() {
        if let Some(next) = time.checked_add(Duration::from_secs(1 << bit)) {
            time = next;
        }
    }
    time
}

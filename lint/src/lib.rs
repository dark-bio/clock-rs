// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Probes for the recommended clippy configuration against real-time calls.
//!
//! Each probe calls one disallowed path under an expectation of the lint, so a
//! path that stops matching fails clippy with warnings denied. The legal probes
//! expect nothing and must stay clean, including the timed selects that escape
//! the lint.

#![forbid(unsafe_code)]

use crossbeam_channel::{Receiver, Select, Sender};
use std::sync::{Condvar, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime};

/// Checks that reading real monotonic time is disallowed.
#[expect(clippy::disallowed_methods, reason = "verifies the Instant::now path")]
pub fn probe_instant_now() {
    let _ = Instant::now();
}

/// Checks that measuring real monotonic elapsed time is disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the Instant::elapsed path"
)]
pub fn probe_instant_elapsed(instant: Instant) {
    let _ = instant.elapsed();
}

/// Checks that reading real wall time is disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the SystemTime::now path"
)]
pub fn probe_system_time_now() {
    let _ = SystemTime::now();
}

/// Checks that measuring real wall elapsed time is disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the SystemTime::elapsed path"
)]
pub fn probe_system_time_elapsed(time: SystemTime) {
    let _ = time.elapsed();
}

/// Checks that real thread sleeps are disallowed.
#[expect(clippy::disallowed_methods, reason = "verifies the thread::sleep path")]
pub fn probe_sleep() {
    std::thread::sleep(Duration::ZERO);
}

/// Checks that real thread park timeouts are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the thread::park_timeout path"
)]
pub fn probe_park_timeout() {
    std::thread::park_timeout(Duration::ZERO);
}

/// Checks that std condvar timeouts are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the Condvar::wait_timeout path"
)]
pub fn probe_condvar_wait_timeout(condvar: &Condvar, mutex: &Mutex<()>) {
    drop(condvar.wait_timeout(mutex.lock().unwrap(), Duration::ZERO));
}

/// Checks that std predicate condvar timeouts are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the Condvar::wait_timeout_while path"
)]
pub fn probe_condvar_wait_timeout_while(condvar: &Condvar, mutex: &Mutex<()>) {
    drop(condvar.wait_timeout_while(mutex.lock().unwrap(), Duration::ZERO, |_| false));
}

/// Checks that std channel receive timeouts are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the mpsc::Receiver::recv_timeout path"
)]
pub fn probe_std_recv_timeout(receiver: &mpsc::Receiver<()>) {
    let _ = receiver.recv_timeout(Duration::ZERO);
}

/// Checks that crossbeam relative timers are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the crossbeam_channel::after path"
)]
pub fn probe_after() {
    let _ = crossbeam_channel::after(Duration::ZERO);
}

/// Checks that crossbeam absolute timers are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the crossbeam_channel::at path"
)]
pub fn probe_at(deadline: Instant) {
    let _ = crossbeam_channel::at(deadline);
}

/// Checks that crossbeam periodic timers are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the crossbeam_channel::tick path"
)]
pub fn probe_tick() {
    let _ = crossbeam_channel::tick(Duration::from_secs(1));
}

/// Checks that crossbeam receive timeouts are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the crossbeam Receiver::recv_timeout path"
)]
pub fn probe_recv_timeout(receiver: &Receiver<()>) {
    let _ = receiver.recv_timeout(Duration::ZERO);
}

/// Checks that crossbeam receive deadlines are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the crossbeam Receiver::recv_deadline path"
)]
pub fn probe_recv_deadline(receiver: &Receiver<()>, deadline: Instant) {
    let _ = receiver.recv_deadline(deadline);
}

/// Checks that crossbeam send timeouts are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the crossbeam Sender::send_timeout path"
)]
pub fn probe_send_timeout(sender: &Sender<()>) {
    let _ = sender.send_timeout((), Duration::ZERO);
}

/// Checks that crossbeam send deadlines are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the crossbeam Sender::send_deadline path"
)]
pub fn probe_send_deadline(sender: &Sender<()>, deadline: Instant) {
    let _ = sender.send_deadline((), deadline);
}

/// Checks that dynamic select timeouts are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the Select::select_timeout path"
)]
pub fn probe_select_timeout(select: &mut Select<'_>) {
    let _ = select.select_timeout(Duration::ZERO);
}

/// Checks that dynamic select deadlines are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the Select::select_deadline path"
)]
pub fn probe_select_deadline(select: &mut Select<'_>, deadline: Instant) {
    let _ = select.select_deadline(deadline);
}

/// Checks that dynamic readiness timeouts are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the Select::ready_timeout path"
)]
pub fn probe_ready_timeout(select: &mut Select<'_>) {
    let _ = select.ready_timeout(Duration::ZERO);
}

/// Checks that dynamic readiness deadlines are disallowed.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies the Select::ready_deadline path"
)]
pub fn probe_ready_deadline(select: &mut Select<'_>, deadline: Instant) {
    let _ = select.ready_deadline(deadline);
}

/// Checks that an after call in a select argument remains the caller's linted code.
#[expect(
    clippy::disallowed_methods,
    reason = "verifies after inside a select argument"
)]
pub fn probe_after_in_select() {
    crossbeam_channel::select! {
        recv(crossbeam_channel::after(Duration::ZERO)) -> _ => {},
    }
}

/// Keeps untimed std condvar waits legal.
pub fn legal_condvar_wait(condvar: &Condvar, mutex: &Mutex<()>) {
    drop(condvar.wait(mutex.lock().unwrap()));
}

/// Keeps untimed std predicate condvar waits legal.
pub fn legal_condvar_wait_while(condvar: &Condvar, mutex: &Mutex<()>) {
    drop(condvar.wait_while(mutex.lock().unwrap(), |_| false));
}

/// Keeps untimed std channel receives legal.
pub fn legal_std_recv(receiver: &mpsc::Receiver<()>) {
    let _ = receiver.recv();
}

/// Keeps untimed crossbeam receives legal.
pub fn legal_recv(receiver: &Receiver<()>) {
    let _ = receiver.recv();
}

/// Keeps crossbeam's timer-free never receiver legal.
pub fn legal_never() {
    let _ = crossbeam_channel::never::<()>();
}

/// Keeps arithmetic on caller-supplied instants legal.
pub fn legal_instant_arithmetic(instant: Instant, duration: Duration) {
    let _ = instant + duration;
    let _ = instant - duration;
    let _ = instant.checked_add(duration);
    let _ = instant.saturating_duration_since(instant);
}

/// Keeps untimed selects legal.
pub fn legal_select(receiver: &Receiver<()>) {
    crossbeam_channel::select! {
        recv(receiver) -> _ => {},
    }
}

/// Records the gap where a timed select reads real time inside crossbeam's macro.
pub fn unlinted_timed_select(first: &Receiver<()>, second: &Receiver<()>) {
    crossbeam_channel::select! {
        recv(first) -> _ => {},
        recv(second) -> _ => {},
        default(Duration::ZERO) => {},
    }
}

/// Records the same gap for a multi-arm timed biased select.
pub fn unlinted_timed_select_biased(first: &Receiver<()>, second: &Receiver<()>) {
    crossbeam_channel::select_biased! {
        recv(first) -> _ => {},
        recv(second) -> _ => {},
        default(Duration::ZERO) => {},
    }
}

/// Checks that the published recommendation is the configuration these probes verify.
#[cfg(test)]
mod tests {
    // The README contains the entire verified clippy configuration verbatim.
    #[test]
    fn test_readme_contains_verified_lint_configuration() {
        // Read the published instructions and the probe's configuration at compile time
        let readme = include_str!("../../README.md");
        let configuration = include_str!("../clippy.toml");

        // Keep the recommendation identical to what clippy checks
        assert!(readme.contains(configuration));
    }
}

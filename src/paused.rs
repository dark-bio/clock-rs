// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Test clocks, which move only when their owner advances them.

use crate::primitives::{Condvar, Mutex, MutexGuard};
use crate::{Clock, Signal};
#[cfg(feature = "crossbeam")]
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, PoisonError, Weak};
use std::time::{Duration, Instant, SystemTime};

/// Owns a clock that moves only when advanced, for tests.
///
/// [`Self::clock`] hands out handles that read and sleep on its time. Only the
/// owner moves it, through methods that take `&mut self`, so each test clock
/// has one driver.
#[cfg_attr(docsrs, doc(cfg(feature = "test-clock")))]
pub struct TestClock {
    /// State shared with every clock handle.
    paused: Arc<Paused>,
}

impl TestClock {
    /// Creates a stopped clock at the current real monotonic and wall times.
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            paused: Arc::new(Paused {
                start: now,
                state: Mutex::new(PausedState {
                    now,
                    wall: WallAnchor {
                        time: SystemTime::now(),
                        instant: now,
                    },
                    signals: BTreeMap::new(),
                    blocked: 0,
                    deadlines: BTreeMap::new(),
                    #[cfg(feature = "crossbeam")]
                    timers: BTreeMap::new(),
                    #[cfg(feature = "crossbeam")]
                    fired: Vec::new(),
                }),
                changed: Condvar::new(),
                #[cfg(test)]
                before_park: std::sync::Mutex::new(None),
                #[cfg(test)]
                before_rewait: std::sync::Mutex::new(None),
                #[cfg(all(test, feature = "crossbeam"))]
                after_timer_receive: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Returns a handle that reads and sleeps on this clock.
    pub fn clock(&self) -> Clock {
        Clock {
            paused: Some(self.paused.clone()),
        }
    }

    /// Moves both times forward by `by`, waking the sleeps and deadline waits
    /// it reaches and firing its due timers.
    ///
    /// Returns once the reached waits are woken and the due timers hold their
    /// messages, without waiting for any thread to act. Other waits keep
    /// waiting, except that a condvar wait may return spuriously when another
    /// wait on its condvar is reached. A zero advance does nothing.
    ///
    /// # Panics
    ///
    /// Panics if either time would overflow, before changing either one.
    pub fn advance(&mut self, by: Duration) {
        self.advance_with(|now| {
            now.checked_add(by)
                .expect("clock advance overflows Instant")
        });
    }

    /// Moves monotonic time to `target` and wall time by the same amount,
    /// waking the sleeps and deadline waits it reaches and firing its due timers.
    ///
    /// Returns once the reached waits are woken and the due timers hold their
    /// messages, without waiting for any thread to act. Other waits keep
    /// waiting, except that a condvar wait may return spuriously when another
    /// wait on its condvar is reached. Advancing to the current time does nothing.
    ///
    /// # Panics
    ///
    /// Panics if `target` is before now or wall time would overflow.
    /// Neither time changes after a panic.
    pub fn advance_to(&mut self, target: Instant) {
        self.advance_with(|now| {
            assert!(target >= now, "clock cannot go backwards");
            target
        });
    }

    /// Sets wall time forwards or backwards without moving monotonic time.
    ///
    /// This wakes no waits and fires no timers, since deadlines use monotonic time.
    pub fn set_system_time(&mut self, time: SystemTime) {
        let mut state = self.paused.lock();
        state.wall = WallAnchor {
            time,
            instant: state.now,
        };
    }

    /// Blocks until at least `count` threads are parked in this clock's sleeps
    /// and condvar waits.
    ///
    /// Threads blocked in crossbeam receives and selects do not count. A thread
    /// parked earlier counts too, so the count proves no progress on its own.
    /// Nothing bounds the wait, so run tests under a runner with a per-test
    /// timeout, since `cargo test` alone never stops a hung test.
    pub fn wait_blocked(&self, count: usize) {
        let mut state = self.paused.lock();
        while state.blocked < count {
            state = self
                .paused
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Blocks until at least `count` timers are armed on this clock and unfired.
    ///
    /// A waiting receive's timer counts, and so does a timer whose receiver was
    /// dropped. A timer armed earlier counts too, so the count proves no
    /// progress on its own. Nothing bounds the wait, so run tests under a runner
    /// with a per-test timeout, since `cargo test` alone never stops a hung test.
    #[cfg(feature = "crossbeam")]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "test-clock", feature = "crossbeam"))))]
    pub fn wait_timers(&self, count: usize) {
        let mut state = self.paused.lock();
        while state.timers.len() < count {
            state = self
                .paused
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Returns the earliest deadline among this clock's parked sleeps, deadline
    /// waits and unfired timers, or `None` when there is none.
    ///
    /// A timed wait stays listed until it stops waiting, so one an advance
    /// reaches stays listed until its thread runs. Await an advance's effect
    /// before reading the next deadline. A timer leaves the list when it fires.
    pub fn next_deadline(&self) -> Option<Instant> {
        // Compare the earliest parked wait with the earliest unfired timer
        let state = self.paused.lock();
        let deadline = state.deadlines.keys().next().map(|&(deadline, _)| deadline);
        #[cfg(feature = "crossbeam")]
        let deadline = deadline
            .into_iter()
            .chain(state.timers.keys().next().map(|&(deadline, _)| deadline))
            .min();
        deadline
    }

    /// Validates both new times, publishes them together with the due timers'
    /// messages, then wakes the reached waits.
    fn advance_with(&mut self, next: impl FnOnce(Instant) -> Instant) {
        // Check the new times before taking the lock, so a failed check panics with
        // no lock held. Only the owner advances, so nothing changes them in between.
        let (now, wall) = {
            let state = self.paused.lock();
            (state.now, state.wall)
        };
        let next = next(now);
        if next == now {
            return;
        }
        wall.at(next).expect("clock advance overflows SystemTime");

        // Publish the time and collect each reached wait's signal once, in the same
        // lock hold that parks register in
        let mut state = self.paused.lock();
        state.now = next;
        let signals: Vec<_> = state
            .deadlines
            .range(..=(next, usize::MAX))
            .map(|(&(_, key), _)| key)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|key| {
                state
                    .signals
                    .get(&key)
                    .and_then(Weak::upgrade)
                    .expect("parked signal is registered")
            })
            .collect();

        // Deliver every due timer before another thread can read the new time
        #[cfg(feature = "crossbeam")]
        {
            while state
                .timers
                .first_key_value()
                .is_some_and(|(&(deadline, _), _)| deadline <= next)
            {
                let timer = state.timers.pop_first().expect("due timer exists").1;
                state.fire(&timer);
            }
            self.paused.changed.notify_all();
        }
        drop(state);

        // Wake outside the clock lock, since parking takes the signal lock first
        for signal in signals {
            signal.wake();
        }
    }
}

impl Default for TestClock {
    /// Creates a stopped clock at the current real monotonic and wall times.
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TestClock {
    /// Shows the advance, wall time, parked threads and armed timers.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Snapshot the counts before calling the formatter's writer
        let state = self.paused.lock();
        let advanced = state.now - self.paused.start;
        let system_time = state.system_time();
        let blocked = state.blocked;
        #[cfg(feature = "crossbeam")]
        let timers = state.timers.len();
        drop(state);

        // Format without the clock lock, since the writer may read this clock
        let mut debug = f.debug_struct("TestClock");
        debug
            .field("advanced", &advanced)
            .field("system_time", &system_time)
            .field("blocked", &blocked);
        #[cfg(feature = "crossbeam")]
        debug.field("timers", &timers);
        debug.finish()
    }
}

/// A test clock's state, shared by its owner, handles and waiters.
pub(crate) struct Paused {
    /// Time the clock started at, to show how far it has advanced.
    start: Instant,
    /// Current times, live signals, parked thread count, deadlines and timers.
    pub(crate) state: Mutex<PausedState>,
    /// Wakes drivers when parked threads or armed timers change.
    pub(crate) changed: Condvar,
    /// One-shot test hook, run before a park's first wait with no clock or
    /// signal lock held.
    #[cfg(test)]
    pub(crate) before_park: std::sync::Mutex<Option<BeforePark>>,
    /// One-shot test hook, run like `before_park` before a park waits again
    /// after a spurious wakeup.
    #[cfg(test)]
    pub(crate) before_rewait: std::sync::Mutex<Option<BeforePark>>,
    /// One-shot test hook, run when a receive's timer wins, before the receiver
    /// is checked again, with no crate lock held.
    #[cfg(all(test, feature = "crossbeam"))]
    after_timer_receive: std::sync::Mutex<Option<TimerHook>>,
}

/// Mutable part of a test clock.
pub(crate) struct PausedState {
    /// Current monotonic time.
    now: Instant,
    /// Wall time as last set, with the monotonic instant it was set at.
    wall: WallAnchor,
    /// Live waiters and condvars indexed by signal address, removed on drop.
    pub(crate) signals: BTreeMap<usize, Weak<Signal>>,
    /// Threads committed to parking while holding their signal's lock.
    pub(crate) blocked: usize,
    /// Parked timed waits counted by deadline and signal key, each unlisted
    /// when its park ends.
    deadlines: BTreeMap<(Instant, usize), usize>,
    /// Unfired timers by deadline, with each timer's address telling equal
    /// deadlines apart.
    #[cfg(feature = "crossbeam")]
    timers: BTreeMap<(Instant, usize), Arc<Timer>>,
    /// Senders of delivered public timers, kept so their channels stay connected
    /// while the clock lives.
    #[cfg(feature = "crossbeam")]
    fired: Vec<Sender<Instant>>,
}

impl PausedState {
    /// Returns the wall time at the current monotonic time.
    fn system_time(&self) -> SystemTime {
        self.wall
            .at(self.now)
            .expect("wall time fits, since every change checks it first")
    }

    /// Lists a timer until an advance reaches it, or delivers it at once if it
    /// is already due.
    #[cfg(feature = "crossbeam")]
    fn arm_timer(&mut self, deadline: Instant, retain: bool) -> (Arc<Timer>, Receiver<Instant>) {
        // Give the timer a stable address and room for its only message
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let timer = Arc::new(Timer {
            deadline,
            sender,
            retain,
        });

        // List or deliver it in the lock hold that read the time, so no advance slips between
        if deadline > self.now {
            self.timers.insert(timer.key(), timer.clone());
        } else {
            self.fire(&timer);
        }
        (timer, receiver)
    }

    /// Sends a timer's deadline, and keeps the sender of a delivered public timer.
    #[cfg(feature = "crossbeam")]
    fn fire(&mut self, timer: &Timer) {
        // The capacity-1 channel has never been sent to, so this cannot block
        let delivered = timer.sender.send(timer.deadline).is_ok();

        // Keep a delivered public timer's sender, so its channel stays connected like
        // crossbeam's. Nothing reports a dropped receiver without sending, and a second
        // send would refill a consumed timer.
        if delivered && timer.retain {
            self.fired.push(timer.sender.clone());
        }
    }
}

/// A wall time and the monotonic instant it was set at.
#[derive(Clone, Copy)]
struct WallAnchor {
    /// Wall time when the clock was created or last set.
    time: SystemTime,
    /// Monotonic time at that moment.
    instant: Instant,
}

impl WallAnchor {
    /// Returns the wall time at `instant`, or `None` if it does not fit.
    ///
    /// The whole span since the anchor is added at once, so a platform that
    /// rounds wall time, like Windows to 100 ns, rounds once and never per advance.
    fn at(&self, instant: Instant) -> Option<SystemTime> {
        self.time.checked_add(instant - self.instant)
    }
}

impl Paused {
    /// Returns armed timers and retained senders for the bookkeeping model.
    #[cfg(all(test, feature = "crossbeam", not(loom)))]
    pub(crate) fn timer_counts(&self) -> (usize, usize) {
        let state = self.lock();
        (state.timers.len(), state.fired.len())
    }

    /// Arms a public timer, whose channel stays connected after delivery.
    #[cfg(feature = "crossbeam")]
    pub(crate) fn at(&self, deadline: Instant) -> Receiver<Instant> {
        let mut state = self.lock();
        let (_, receiver) = state.arm_timer(deadline, true);
        self.changed.notify_all();
        receiver
    }

    /// Receives until the clock reaches `deadline`, where a message or a
    /// disconnection wins over expiry, as in crossbeam.
    ///
    /// The receiver is never checked under the clock lock, since a rendezvous
    /// receive can wait on a sender that reads the clock. Expiry is decided only
    /// at a time no advance changed since the check, and an advance delivers its
    /// due timers before anyone reads its time, so a timer due by the deadline
    /// holds its message by then.
    #[cfg(feature = "crossbeam")]
    pub(crate) fn recv_deadline<T>(
        &self,
        receiver: &Receiver<T>,
        deadline: Instant,
    ) -> Result<T, RecvTimeoutError> {
        // Check the receiver at a known time, and look again if an advance ran meanwhile
        let (timer, timeout) = loop {
            let seen = self.now();
            match receiver.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => return Err(RecvTimeoutError::Disconnected),
                Err(TryRecvError::Empty) => {}
            }
            let mut state = self.lock();
            if state.now != seen {
                continue;
            }

            // Decide expiry at the checked time, or list the timeout before unlocking so
            // that no advance slips between and wait_timers counts it
            if seen >= deadline {
                return Err(RecvTimeoutError::Timeout);
            }
            let timer = state.arm_timer(deadline, false);
            self.changed.notify_all();
            break timer;
        };

        // Unlist the timeout on every return from the adapter
        let _registration = ReceiveTimer {
            paused: self,
            timer,
        };

        // Wait on the receiver and the clock's timer, with no real timeout
        crossbeam_channel::select! {
            recv(receiver) -> result => result.map_err(RecvTimeoutError::from),
            recv(timeout) -> _ => {
                // Let tests make the receiver ready after the timer has won
                #[cfg(test)]
                {
                    let hook = self.after_timer_receive.lock().unwrap().take();
                    if let Some(hook) = hook {
                        hook();
                    }
                }

                // Wait out an advance still delivering, then recheck without the lock, since
                // select may pick the timeout before a message due at the same time
                drop(self.lock());
                receiver.try_recv().map_err(|err| match err {
                    TryRecvError::Empty => RecvTimeoutError::Timeout,
                    TryRecvError::Disconnected => RecvTimeoutError::Disconnected,
                })
            }
        }
    }

    /// Returns the clock's current monotonic time.
    pub(crate) fn now(&self) -> Instant {
        self.lock().now
    }

    /// Returns the clock's current wall time.
    pub(crate) fn system_time(&self) -> SystemTime {
        self.lock().system_time()
    }

    /// Returns how far the clock has advanced since its creation.
    pub(crate) fn advanced(&self) -> Duration {
        self.lock().now - self.start
    }

    /// Registers a waiter's signal before its first notification check.
    pub(crate) fn register(&self, signal: &Arc<Signal>) {
        self.lock()
            .signals
            .insert(signal.key(), Arc::downgrade(signal));
    }

    /// Removes a waiter's signal without retaining storage for dead waiters.
    pub(crate) fn unregister(&self, signal: &Arc<Signal>) {
        self.lock().signals.remove(&signal.key());
    }

    /// Counts a park and lists its deadline until the guard drops, or returns
    /// `None` if the deadline has already been reached.
    ///
    /// Advances collect the waits to wake under the same lock, so a park either
    /// registers in time to be woken or sees the new time. The caller holds its
    /// signal lock until it waits, so the wake cannot arrive before the wait.
    pub(crate) fn block(&self, deadline: Option<Instant>, signal: &Signal) -> Option<Blocked<'_>> {
        // Refuse a deadline that an advance has already reached
        let mut state = self.lock();
        if deadline.is_some_and(|deadline| deadline <= state.now) {
            return None;
        }

        // Count the park, and list a timed one under its signal's key
        let deadline = deadline.map(|deadline| (deadline, signal.key()));
        state.blocked += 1;
        if let Some(deadline) = deadline {
            *state.deadlines.entry(deadline).or_default() += 1;
        }

        // Wake drivers waiting for the count to grow
        self.changed.notify_all();
        Some(Blocked {
            paused: self,
            deadline,
        })
    }

    /// Takes the one-shot test hook for a park's first wait, or for a wait
    /// `again` after a spurious wakeup, for the caller to run with no lock held.
    #[cfg(test)]
    pub(crate) fn take_hook(&self, again: bool) -> Option<BeforePark> {
        let hook = if again {
            &self.before_rewait
        } else {
            &self.before_park
        };
        hook.lock().unwrap().take()
    }

    /// Locks the state, recovering it from poisoning, since no update under the
    /// lock can stop halfway.
    fn lock(&self) -> MutexGuard<'_, PausedState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A timer's one message, and whether its channel outlives delivery.
#[cfg(feature = "crossbeam")]
struct Timer {
    /// Deadline sent as the message, even when an advance overshoots it.
    deadline: Instant,
    /// Sender of the capacity-1 channel, sent to only once under the clock lock.
    sender: Sender<Instant>,
    /// Whether the clock keeps the sender after delivery, as for public timers.
    retain: bool,
}

#[cfg(feature = "crossbeam")]
impl Timer {
    /// Identifies this allocation among timers at the same deadline.
    fn key(&self) -> (Instant, usize) {
        (self.deadline, self as *const Self as usize)
    }
}

/// Removes a receive adapter's timer when it returns, even before expiry.
#[cfg(feature = "crossbeam")]
struct ReceiveTimer<'a> {
    /// Clock holding the armed timer, if it has not fired yet.
    paused: &'a Paused,
    /// Keeps the registration's address unique until it is removed.
    timer: Arc<Timer>,
}

#[cfg(feature = "crossbeam")]
impl Drop for ReceiveTimer<'_> {
    /// Unlists an unfired timer without retaining its sender.
    fn drop(&mut self) {
        self.paused.lock().timers.remove(&self.timer.key());
        self.paused.changed.notify_all();
    }
}

/// Counts one park, and lists its deadline, from its first wait until it returns.
pub(crate) struct Blocked<'a> {
    /// Clock whose parked count includes this thread.
    paused: &'a Paused,
    /// Deadline and signal key registered for this park, if it is timed.
    deadline: Option<(Instant, usize)>,
}

impl Drop for Blocked<'_> {
    /// Removes this thread and its deadline from the clock's parked state.
    fn drop(&mut self) {
        // Uncount the park and unlist its deadline
        let mut state = self.paused.lock();
        state.blocked -= 1;
        if let Some(deadline) = self.deadline {
            let count = state
                .deadlines
                .get_mut(&deadline)
                .expect("parked deadline exists");
            *count -= 1;
            if *count == 0 {
                state.deadlines.remove(&deadline);
            }
        }

        // Wake watchers of the count, such as tests waiting for it to drop
        self.paused.changed.notify_all();
    }
}

/// Pauses a test wait before parking and exposes its real timer, if any.
#[cfg(test)]
pub(crate) type BeforePark = Box<dyn FnOnce(Option<Instant>) + Send>;

/// Observes a timer race with no crate lock held.
#[cfg(all(test, feature = "crossbeam"))]
type TimerHook = Box<dyn FnOnce() + Send>;

// The timer tests live in src/tests, loaded from here so that they keep this
// module's private items in reach
#[cfg(all(test, feature = "crossbeam", not(loom)))]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "tests/paused.rs"]
mod tests;

// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Test clocks, which move only when their owner advances them.

use crate::{Clock, Signal};
#[cfg(feature = "crossbeam")]
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
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
                    system_time: SystemTime::now(),
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
                before_park: Mutex::new(None),
                #[cfg(test)]
                before_rewait: Mutex::new(None),
                #[cfg(all(test, feature = "crossbeam"))]
                after_timer_send: Mutex::new(None),
                #[cfg(all(test, feature = "crossbeam"))]
                after_timer_receive: Mutex::new(None),
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
    /// Returns once the waits are notified and the timers hold their messages,
    /// without waiting for any thread to act. A zero advance does nothing.
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
    /// Returns once the waits are notified and the timers hold their messages,
    /// without waiting for any thread to act. Advancing to the current time
    /// does nothing.
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
        self.paused.lock().system_time = time;
    }

    /// Blocks until at least `count` threads are parked in this clock's sleeps
    /// and condvar waits.
    ///
    /// Threads blocked in crossbeam receives and selects do not count. A thread
    /// parked earlier counts too, so the count proves no progress on its own.
    /// Nothing bounds the wait, so the test runner ends a hang.
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
    /// progress on its own. Nothing bounds the wait, so the test runner ends a
    /// hang.
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
    /// A wait is listed while parked, and one woken by an advance stays listed
    /// until its thread runs, so await an advance's effect before reading the
    /// next deadline. A timer leaves the list when it fires.
    pub fn next_deadline(&self) -> Option<Instant> {
        // Compare the earliest parked wait with the earliest unfired timer
        let state = self.paused.lock();
        let deadline = state.deadlines.keys().next().copied();
        #[cfg(feature = "crossbeam")]
        let deadline = deadline
            .into_iter()
            .chain(state.timers.keys().next().map(|&(deadline, _)| deadline))
            .min();
        deadline
    }

    /// Validates both new times before updating state and notifying waiters.
    fn advance_with(&mut self, next: impl FnOnce(Instant) -> Instant) {
        // Compute and check the new times outside the lock, so a failed check's
        // panic runs no hook under it. Only the owner advances, so nothing
        // changes the times in between.
        let (now, system_time) = {
            let state = self.paused.lock();
            (state.now, state.system_time)
        };
        let next = next(now);
        if next == now {
            return;
        }
        let system_time = system_time
            .checked_add(next - now)
            .expect("clock advance overflows SystemTime");

        // Publish both times and collect live waiters and due timers under one lock
        let mut state = self.paused.lock();
        let signals: Vec<_> = state.signals.values().filter_map(Weak::upgrade).collect();
        state.now = next;
        state.system_time = system_time;
        #[cfg(feature = "crossbeam")]
        let timers = {
            let mut timers = Vec::new();
            while state
                .timers
                .first_key_value()
                .is_some_and(|(&(deadline, _), _)| deadline <= next)
            {
                timers.push(state.timers.pop_first().expect("due timer exists").1);
            }
            self.paused.changed.notify_all();
            timers
        };
        drop(state);

        // Deliver in deadline order with the new times visible and no lock held
        #[cfg(feature = "crossbeam")]
        for timer in timers {
            self.paused.fire(&timer);
        }

        // Wake outside the clock lock, since parking takes the signal lock first
        for signal in signals {
            signal.advance();
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
        let system_time = state.system_time;
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
    pub(crate) before_park: Mutex<Option<BeforePark>>,
    /// One-shot test hook, run like `before_park` before a park waits again
    /// after a spurious wakeup.
    #[cfg(test)]
    pub(crate) before_rewait: Mutex<Option<BeforePark>>,
    /// One-shot test hook, run after a timer's send with no crate lock held.
    #[cfg(all(test, feature = "crossbeam"))]
    after_timer_send: Mutex<Option<TimerHook>>,
    /// One-shot test hook, run when a receive's timer wins, before the receiver
    /// is checked again.
    #[cfg(all(test, feature = "crossbeam"))]
    after_timer_receive: Mutex<Option<TimerHook>>,
}

/// Mutable part of a test clock.
pub(crate) struct PausedState {
    /// Current monotonic time.
    now: Instant,
    /// Current wall time, independent of monotonic time when set explicitly.
    system_time: SystemTime,
    /// Live waiters and condvars indexed by signal address, removed on drop.
    pub(crate) signals: BTreeMap<usize, Weak<Signal>>,
    /// Threads committed to parking while holding their signal's lock.
    pub(crate) blocked: usize,
    /// Parked timed waits counted by deadline, each unlisted when its park ends.
    deadlines: BTreeMap<Instant, usize>,
    /// Unfired timers by deadline, with each timer's address telling equal
    /// deadlines apart.
    #[cfg(feature = "crossbeam")]
    timers: BTreeMap<(Instant, usize), Arc<Timer>>,
    /// Senders of delivered public timers, kept so their channels stay connected
    /// while the clock lives.
    #[cfg(feature = "crossbeam")]
    fired: Vec<Sender<Instant>>,
}

impl Paused {
    /// Arms a public timer, whose channel stays connected after delivery.
    #[cfg(feature = "crossbeam")]
    pub(crate) fn at(&self, deadline: Instant) -> Receiver<Instant> {
        self.arm_timer(deadline, true).1
    }

    /// Receives until the clock reaches `deadline`, where a message or a
    /// disconnection wins over expiry, as in crossbeam.
    #[cfg(feature = "crossbeam")]
    pub(crate) fn recv_deadline<T>(
        &self,
        receiver: &Receiver<T>,
        deadline: Instant,
    ) -> Result<T, RecvTimeoutError> {
        // Prefer a ready receiver even when the deadline has already passed
        match receiver.try_recv() {
            Ok(value) => return Ok(value),
            Err(TryRecvError::Disconnected) => return Err(RecvTimeoutError::Disconnected),
            Err(TryRecvError::Empty) => {}
        }
        if self.now() >= deadline {
            return Err(RecvTimeoutError::Timeout);
        }

        // Arm before selecting, and disarm on every return from the adapter
        let (timer, timeout) = self.arm_timer(deadline, false);
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

                // Check the receiver again, since select picks randomly among ready arms
                receiver.try_recv().map_err(|err| match err {
                    TryRecvError::Empty => RecvTimeoutError::Timeout,
                    TryRecvError::Disconnected => RecvTimeoutError::Disconnected,
                })
            }
        }
    }

    /// Registers an unfired timer or delivers it immediately if already due.
    #[cfg(feature = "crossbeam")]
    fn arm_timer(&self, deadline: Instant, retain: bool) -> (Arc<Timer>, Receiver<Instant>) {
        // Give the timer a stable address and room for its only message
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let timer = Arc::new(Timer {
            deadline,
            sender,
            retain,
        });

        // Check and register under one lock so an advance cannot miss this timer
        let mut state = self.lock();
        if deadline > state.now {
            state.timers.insert(timer.key(), timer.clone());
            self.changed.notify_all();
            drop(state);
        } else {
            drop(state);
            self.fire(&timer);
        }
        (timer, receiver)
    }

    /// Sends a timer's deadline with no clock lock held, and keeps the sender of
    /// a delivered public timer.
    #[cfg(feature = "crossbeam")]
    fn fire(&self, timer: &Timer) {
        // The capacity-1 channel has never been sent to, so this cannot block
        let delivered = timer.sender.send(timer.deadline).is_ok();

        // Let tests observe publication and delivery order before the next send
        #[cfg(test)]
        {
            let hook = self.after_timer_send.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }

        // Failed deliveries and adapter timers keep no sender in the clock
        if delivered && timer.retain {
            self.lock().fired.push(timer.sender.clone());
        }
    }

    /// Returns the clock's current monotonic time.
    pub(crate) fn now(&self) -> Instant {
        self.lock().now
    }

    /// Returns the clock's current wall time.
    pub(crate) fn system_time(&self) -> SystemTime {
        self.lock().system_time
    }

    /// Returns how far the clock has advanced since its creation.
    pub(crate) fn advanced(&self) -> Duration {
        self.lock().now - self.start
    }

    /// Registers a waiter's signal before its first generation check.
    pub(crate) fn register(&self, signal: &Arc<Signal>) {
        self.lock()
            .signals
            .insert(Arc::as_ptr(signal) as usize, Arc::downgrade(signal));
    }

    /// Removes a waiter's signal without retaining storage for dead waiters.
    pub(crate) fn unregister(&self, signal: &Arc<Signal>) {
        self.lock().signals.remove(&(Arc::as_ptr(signal) as usize));
    }

    /// Counts a park, and lists its deadline, until the guard drops.
    ///
    /// The caller holds its signal lock until it parks, so an advance by a
    /// driver that saw the count still wakes the park.
    pub(crate) fn block(&self, deadline: Option<Instant>) -> Blocked<'_> {
        // Count the park and list its deadline
        let mut state = self.lock();
        state.blocked += 1;
        if let Some(deadline) = deadline {
            *state.deadlines.entry(deadline).or_default() += 1;
        }

        // Wake drivers waiting for the count to grow
        self.changed.notify_all();
        Blocked {
            paused: self,
            deadline,
        }
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
    /// Sender of the timer's capacity-1 channel.
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

/// Counts one parked thread until its signal lock is retaken after waking.
pub(crate) struct Blocked<'a> {
    /// Clock whose parked count includes this thread.
    paused: &'a Paused,
    /// Deadline registered for this park, if it is timed.
    deadline: Option<Instant>,
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

/// Checks timer delivery, retention and receive precedence without real deadlines.
#[cfg(all(test, feature = "crossbeam"))]
#[cfg_attr(coverage_nightly, coverage(off))]
mod timer_tests {
    use super::*;
    use crate::tests::{blocked, pause_before_park};
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
        let last = crate::tests::last_system_time();
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
}

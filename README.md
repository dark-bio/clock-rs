# Virtual Clock for Testing Blocking Code

[![](https://img.shields.io/crates/v/darkbio-clock.svg)](https://crates.io/crates/darkbio-clock)
[![](https://docs.rs/darkbio-clock/badge.svg)](https://docs.rs/darkbio-clock)
[![](https://github.com/dark-bio/clock-rs/workflows/tests/badge.svg)](https://github.com/dark-bio/clock-rs/actions/workflows/ci.yml)
[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](https://github.com/dark-bio/clock-rs/blob/main/LICENSE)

This crate provides a clock that tests can stop and advance, with blocking sleeps, condition variables and crossbeam timers that follow it. Code reads monotonic time, wall time and elapsed durations through a `Clock` handle instead of std's free functions. Production passes `Clock::real()`, and tests pass a clock from a `TestClock`, which moves only when the test advances it.

Sleeps, condvar waits and timers work on ordinary threads, with no async runtime involved. Advancing a test clock wakes and fires them, so a test runs a 5 s timeout without waiting 5 s.

Clocks are explicit handles, never globals or thread-locals. Parallel tests each own their clock, and worker threads follow the one they were handed.

## Quick start

Reading a clock, sleeping on it and waiting on its condvars work in every build. Timers need the `crossbeam` feature, and `TestClock` needs `test-clock`, without which `Clock` is zero-sized and always real. Enable `test-clock` only on a dev-dependency, since Cargo unifies features across a build. Pick the same version as the regular dependency, so the feature reaches the copy the code uses.

```toml
[dependencies]
darkbio-clock = "0.2"

[dev-dependencies]
darkbio-clock = { version = "0.2", features = ["test-clock"] }
```

```rust
# #[cfg(feature = "test-clock")] {
use darkbio_clock::TestClock;
use std::thread;
use std::time::Duration;

let mut tester = TestClock::new();
let clock = tester.clock();
let sleeper = thread::spawn(move || clock.sleep(Duration::from_secs(5)));

tester.wait_blocked(1);
tester.advance(Duration::from_secs(5));
sleeper.join().unwrap();
# }
```

The sleeper parks on the test clock, `wait_blocked` returns once it has, and the advance releases it without any real waiting.

## Replacing std and crossbeam calls

Each call mirrors the std or crossbeam call it replaces. The clock is the receiver of the time calls and an argument when creating a condvar, and a timed condvar wait takes a deadline in place of a timeout.

| std or crossbeam | darkbio-clock |
|---|---|
| `Instant::now()` | `clock.now()` |
| `start.elapsed()` | `clock.elapsed(start)` |
| `SystemTime::now()` | `clock.system_time()` |
| `thread::sleep(duration)` | `clock.sleep(duration)` |
| `thread::sleep_until(deadline)` | `clock.sleep_until(deadline)` |
| `Condvar::new()` | `Condvar::new(&clock)` |
| `condvar.wait_timeout(guard, deadline - now)` | `condvar.wait_deadline(guard, deadline)` |
| `crossbeam_channel::after(d)` | `clock.after(d)` |
| `crossbeam_channel::at(t)` | `clock.at(t)` |
| `receiver.recv_timeout(d)` | `clock.recv_timeout(&receiver, d)` |
| `receiver.recv_deadline(t)` | `clock.recv_deadline(&receiver, t)` |

`Mutex` and `Condvar` come from this crate's `sync` module in place of `std::sync`. Everything else they offer keeps std's names, arguments, results and poisoning.

## Locks

A condvar waits on this crate's `Mutex`, because relocking after a wait needs the mutex, and a std guard does not give it back. The mutex wraps std's and keeps its behavior, with one exception. A wait that starts while its thread unwinds from a panic, on a guard taken before the panic, poisons the mutex. Only a mutex that a clock's condvar waits on needs to change, and every other lock can stay std's.

`wait_deadline` returns once notified or once the condvar's clock reaches the deadline. As with std's, its `timed_out()` reports a timeout only if the deadline ended the wait before it saw a notification, however late the mutex is relocked. A wait may also return spuriously, including when a notification meant for another waiter ends it. So callers recheck their condition after every return, timeout or not, and a deadline worker picks its earliest deadline again.

There is no `wait_timeout`. A relative timeout computed as `deadline - now` overshoots when an advance lands between the subtraction and the wait, and on a test clock that overshoot is a hang. A condvar has no `Default` either, since its clock is always explicit.

## Timers

The `crossbeam` feature adds timers that crossbeam-channel's `select!` can wait on, and receives with a timeout. It re-exports `crossbeam_channel`, so callers can name its types at the version the clock uses. Without the feature, the crate has no dependencies. On the real clock, `after`, `at`, `recv_timeout` and `recv_deadline` are crossbeam's own.

On a test clock, `at(deadline)` returns a capacity-1 channel. The clock sends the deadline into it once an advance reaches it, or at once when it is already due. The message is the deadline itself, even after an advance that overshoots it, as crossbeam's is. `after(duration)` takes its deadline from the clock when called, and a duration past the end of `Instant`'s range returns a receiver that never fires.

crossbeam's timers never disconnect, but a crossbeam sender cannot tell that its receiver dropped. So a test clock keeps the sender of every delivered timer while the clock lives, and a consumed timer stays connected and empty, as crossbeam's does. The cost is test-only. Memory grows with the timers a test clock delivers, and a timer whose receiver was dropped still counts in `wait_timers` and `next_deadline` until it fires.

`recv_timeout` and `recv_deadline` keep crossbeam's precedence, where a buffered message or a disconnection wins over the timeout. They are clock methods, because a receiver's own methods of the same names would shadow an extension trait's. On a test clock, a waiting receive arms a timer that `wait_timers` counts until it fires or the receive returns.

## Testing on a test clock

### Moving time

A test clock starts at the real monotonic and wall times. `advance` and `advance_to` move both together, and `set_system_time` jumps wall time alone, forwards or backwards, the way an NTP correction does. Only the `TestClock` moves time, and its controls take `&mut self`, so each clock has one driver. A `Clock` handle reads, sleeps and creates condvars and timers, but cannot advance.

An advance jumps straight to its target. Where every period matters, advance one period and wait for its effect before the next. A test clock never moves by itself, so a deadline nobody advances to never passes, and such tests belong under a runner that stops hung tests.

Dropping the `TestClock` stops all advances. Its parked sleeps then never end, its condvar waits end only when notified, and its unfired timers never fire, so advance past their deadlines before dropping it.

### Waiting for threads

An advance returns once the sleeps and deadline waits it reaches are woken and its due timers hold their messages, not once any thread has acted. Wait for the effect itself, such as a result or a message, before checking it.

An advance wakes only the waits whose deadlines it reaches, and never counts as a notification. Reaching a wait on a condvar wakes the condvar's other waits too, so one that saw an earlier notification returns then, as a spurious wakeup.

`wait_blocked(n)` returns once at least `n` threads are parked in the clock's sleeps and condvar waits, timed or not. Threads blocked in a crossbeam receive or `select!` are invisible to it, so with `crossbeam`, `wait_timers(n)` waits for at least `n` armed timers instead, counting those of waiting receives. An earlier park or timer counts too, so neither count proves progress on its own.

`next_deadline()` returns the earliest deadline among the clock's parked sleeps, deadline waits and unfired timers, and advancing to it runs a test to its next timeout. A wait stays listed until it stops waiting, so await an advance's effect before reading the next deadline.

### Deadlines

A sleep takes its deadline when it is called, so a sleep that starts after an advance waits for a later time. Wait with `wait_blocked` before advancing, or take the deadline first and use `sleep_until`.

Take deadlines from the handle's own clock. An `Instant` does not record its clock, so a deadline from `Instant::now()` is silently read as test time. Only calls through the clock follow it, and std's and crossbeam's own time reads, sleeps, timeouts and timers stay on real time, which the lint below flags.

## Linting real-time calls

clippy's `disallowed-methods` can flag every real-time call that has a clock replacement. Put this list in a crate's `clippy.toml` and run clippy with warnings denied, and each stray call fails the build with a pointer to its replacement. This repository's `make lint` checks every entry against a real call.

```toml
disallowed-methods = [
    { path = "std::time::Instant::now", reason = "use Clock::now" },
    { path = "std::time::Instant::elapsed", reason = "use Clock::elapsed" },
    { path = "std::time::SystemTime::now", reason = "use Clock::system_time" },
    { path = "std::time::SystemTime::elapsed", reason = "use Clock::system_time" },
    { path = "std::thread::sleep", reason = "use Clock::sleep" },
    { path = "std::thread::park_timeout", reason = "use a clock condvar" },
    { path = "std::sync::Condvar::wait_timeout", reason = "use darkbio_clock::sync::Condvar" },
    { path = "std::sync::Condvar::wait_timeout_while", reason = "use darkbio_clock::sync::Condvar" },
    { path = "std::sync::mpsc::Receiver::recv_timeout", reason = "use a crossbeam receiver with Clock::recv_timeout" },
    { path = "crossbeam_channel::after", reason = "use Clock::after" },
    { path = "crossbeam_channel::at", reason = "use Clock::at" },
    { path = "crossbeam_channel::tick", reason = "arm Clock::at per period" },
    { path = "crossbeam_channel::Receiver::recv_timeout", reason = "use Clock::recv_timeout" },
    { path = "crossbeam_channel::Receiver::recv_deadline", reason = "use Clock::recv_deadline" },
    { path = "crossbeam_channel::Sender::send_timeout", reason = "select against a clock timer" },
    { path = "crossbeam_channel::Sender::send_deadline", reason = "select against a clock timer" },
    { path = "crossbeam_channel::Select::select_timeout", reason = "select against a clock timer" },
    { path = "crossbeam_channel::Select::select_deadline", reason = "select against a clock timer" },
    { path = "crossbeam_channel::Select::ready_timeout", reason = "select against a clock timer" },
    { path = "crossbeam_channel::Select::ready_deadline", reason = "select against a clock timer" },
]
```

A site that needs real time opts out with `#[expect(clippy::disallowed_methods, reason = "...")]` on its function. Untimed std condvar waits and receives, `crossbeam_channel::never`, `Instant` arithmetic and untimed `select!` stay legal. A crate that does not depend on crossbeam ignores its entries.

clippy does not lint inside another crate's macros, so a timed arm, `default(timeout)`, in a `select!` or `select_biased!` can escape the lint. Write the timeout as a `recv(clock.after(timeout))` arm instead, which clippy checks as the caller's own code. Nor can clippy see real-time calls inside dependencies.

## License

This library is licensed under the [BSD 3-Clause License](https://github.com/dark-bio/clock-rs/blob/main/LICENSE).

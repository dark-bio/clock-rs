# Virtual Clock for Testing Blocking Code

[![](https://img.shields.io/crates/v/darkbio-clock.svg)](https://crates.io/crates/darkbio-clock)
[![](https://docs.rs/darkbio-clock/badge.svg)](https://docs.rs/darkbio-clock)
[![](https://github.com/dark-bio/clock-rs/workflows/tests/badge.svg)](https://github.com/dark-bio/clock-rs/actions/workflows/ci.yml)
[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](https://github.com/dark-bio/clock-rs/blob/main/LICENSE)

This crate provides a clock that tests can stop and advance, with blocking sleeps that follow it. Code reads monotonic time, wall time and elapsed durations through a `Clock` handle instead of std's free functions. Production passes `Clock::real()`, and tests pass a clock from a `TestClock`, which moves only when the test advances it.

The sleeps block ordinary threads, with no async runtime involved. Advancing a test clock wakes them, so a test runs a 5 s timeout without waiting 5 s.

Clocks are explicit handles, never globals or thread-locals. Parallel tests each own their clock, and worker threads follow the one they were handed.

## Quick start

Reading a clock and sleeping on it work in every build. `TestClock` needs the `test-clock` feature, and without it `Clock` is zero-sized and always real. Enable the feature only on a dev-dependency, since Cargo unifies features across a build. Pick the same version as the regular dependency, so the feature reaches the copy the code uses.

```toml
[dependencies]
darkbio-clock = "0.1"

[dev-dependencies]
darkbio-clock = { version = "0.1", features = ["test-clock"] }
```

```rust
# #[cfg(feature = "test-clock")] {
use darkbio_clock::TestClock;
use std::thread;
use std::time::Duration;

let mut test = TestClock::new();
let clock = test.clock();
let sleeper = thread::spawn(move || clock.sleep(Duration::from_secs(5)));

test.wait_blocked(1);
test.advance(Duration::from_secs(5));
sleeper.join().unwrap();
# }
```

Each call mirrors the std call it replaces, with the same arguments and results.

| std | darkbio-clock |
|---|---|
| `Instant::now()` | `clock.now()` |
| `start.elapsed()` | `clock.elapsed(start)` |
| `SystemTime::now()` | `clock.system_time()` |
| `thread::sleep(duration)` | `clock.sleep(duration)` |
| `thread::sleep_until(deadline)` | `clock.sleep_until(deadline)` |

A test clock starts at the real monotonic and wall times and moves both on every advance. `set_system_time` jumps wall time alone, forwards or backwards, the way an NTP correction does.

## Testing on a test clock

- A test clock never moves by itself, so a deadline nobody advances to never passes. Run such tests under a runner that stops hung tests.
- Only the `TestClock` moves time, and its controls take `&mut self`, so each clock has one driver. A `Clock` handle reads and sleeps, but cannot advance.
- An advance returns once sleepers are notified, not once they have acted. Wait for the effect itself, such as a result or a message, before checking it.
- `wait_blocked(n)` returns once at least `n` threads are parked on the clock. A thread parked earlier counts too, so it proves no progress on its own.
- A sleep takes its deadline when it is called, so a sleep that starts after an advance waits for a later time. Wait with `wait_blocked` before advancing, or take the deadline first and use `sleep_until`.
- Take deadlines from the handle's own clock. An `Instant` does not record its clock, so a deadline from `Instant::now()` is silently read as test time.
- An advance jumps straight to its target. Where every period matters, advance one period and wait for its effect before the next.
- Dropping the `TestClock` leaves its sleepers parked for good. Advance past their deadlines before dropping it.
- Only calls through the clock follow it. std's own time reads, sleeps, condvar timeouts and timed channel receives stay on real time.

## License

This library is licensed under the [BSD 3-Clause License](https://github.com/dark-bio/clock-rs/blob/main/LICENSE).

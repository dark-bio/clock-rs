# Virtual Clock for Testing Blocking Code

[![](https://img.shields.io/crates/v/darkbio-clock.svg)](https://crates.io/crates/darkbio-clock)
[![](https://docs.rs/darkbio-clock/badge.svg)](https://docs.rs/darkbio-clock)
[![](https://github.com/dark-bio/clock-rs/workflows/tests/badge.svg)](https://github.com/dark-bio/clock-rs/actions/workflows/ci.yml)
[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](https://github.com/dark-bio/clock-rs/blob/main/LICENSE)

This crate provides a clock that tests can pause and advance, and blocking waits that follow it. Code under test reads time and measures deadlines through a `Clock` handle instead of `Instant::now`. Production hands it the real monotonic clock, while tests hand it a paused one that moves only when advanced.

A `Waiter` blocks a thread until a condition holds or a deadline passes on its clock. Advancing a paused clock wakes its waiters, so a test runs a 5 s timeout without waiting 5 s. The waits block ordinary threads, with no async runtime involved.

Clocks are explicit handles, never globals or thread-locals. Parallel tests each own their clock, and worker threads follow the one they were handed.

## Quick start

Reading a clock and waiting on it work in every build. Pausing and advancing need the `test-clock` feature, and a build without it cannot pause its clock. Enable it only on a dev-dependency, since Cargo unifies features across a build. Pick the same version as the regular dependency, so the feature reaches the copy the code uses.

```toml
[dependencies]
darkbio-clock = "0.1"

[dev-dependencies]
darkbio-clock = { version = "0.1", features = ["test-clock"] }
```

```rust
# #[cfg(feature = "test-clock")] {
use darkbio_clock::Clock;
use std::thread;
use std::time::Duration;

let clock = Clock::paused();
let deadline = clock.now() + Duration::from_secs(5);
let waiter = clock.waiter();
let waiting = thread::spawn(move || waiter.wait_until(Some(deadline), || None::<()>));

clock.advance(Duration::from_secs(5));
assert_eq!(waiting.join().unwrap(), None);
# }
```

## Testing on a paused clock

- A paused clock never moves by itself, so a deadline nobody advances to never passes. Run such tests under a runner that stops hung tests.
- An advance returns once the waiters are notified, not once they have acted. Wait for the effect itself, such as a result or a message, before checking it.
- Take deadlines from the waiter's own clock. An `Instant` does not record its clock, so a deadline from `Instant::now()` is silently read as paused time.
- A thread that takes its deadline after an advance waits for a later time. Take deadlines before advancing, not in a thread that may run after it.
- An advance jumps straight to its target, and changes that land together wake a waiter once. Where every period matters, advance one period and wait for its effect before the next.
- Waiters recheck their condition only when notified or when the clock moves. Notify after every change a condition reads, including closing.
- Dropping a clock does not end the waits on it. Before joining a waiting thread, make its condition ready and notify, or advance past its deadline.
- Only code that reads the clock follows it. `Instant::now`, `thread::sleep`, timed channel receives and `SystemTime` all stay on real time.
- Pausing time does not order threads, and advances from several threads race each other. Drive a paused clock from one thread.

## License

This library is licensed under the [BSD 3-Clause License](https://github.com/dark-bio/clock-rs/blob/main/LICENSE).

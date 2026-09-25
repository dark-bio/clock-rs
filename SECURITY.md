# Security policy

This crate provides the clock that deadline code reads and the blocking waits
that follow it. A build without the `test-clock` feature only reads real time,
so it cannot be paused.

## Reporting a vulnerability

Please do not open a public issue for anything that looks like a security
problem. Send a private email to peter@dark.bio instead, with a description of
the issue, the affected version and, if you have one, a way to reproduce it.
You will get an acknowledgement within a few days, and updates as the fix
progresses.

## Supported versions

Only the latest release on crates.io receives fixes. Versions are 0.x and every
minor bump may change the API, so fixes ship as new versions rather than
backports. Consumers should track the latest release.

## Disclosure

Fixes are released first and disclosed afterwards. Once a fixed version is on
crates.io, an advisory is filed with the RustSec database so that `cargo audit`
users learn about it. The report is credited unless you prefer otherwise.

// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! The crate's tests, kept apart from the code they check.
//!
//! Each module loads its own tests from this folder through a `#[path]`
//! attribute, so they keep that module's private items in reach. Declared here
//! are the tests of the clock core in lib.rs, the helpers that several modules'
//! tests share, and the model checks.

// The clock core's tests, and the helpers that other modules' tests share
#[cfg(not(loom))]
mod clock;
#[cfg(not(loom))]
pub(crate) mod helpers;

// Loom checks the wait core under every schedule, and proptest checks the timer
// bookkeeping against a reference model
#[cfg(loom)]
mod loom_models;
#[cfg(all(feature = "crossbeam", not(loom)))]
mod timer_model;

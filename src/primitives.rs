// clock-rs: virtual clock for testing blocking code
// Copyright 2026 Dark Bio AG. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//! Locks and condvars, from loom when tests run under its model checker and
//! from std otherwise.

#[cfg(all(test, loom))]
pub(crate) use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(all(test, loom)))]
pub(crate) use std::sync::{Condvar, Mutex, MutexGuard};

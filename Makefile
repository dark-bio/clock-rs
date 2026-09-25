.PHONY: check lint loom coverage
.DEFAULT_GOAL := check

# check runs the gates CI holds a push to, the formatting, clippy, the docs and
# the tests of every feature combination.
# Loom runs separately because exhaustive scheduling is slower than these gates.
check:
	cargo fmt --all -- --check
	cargo hack clippy --feature-powerset --all-targets --locked -- -D warnings
	RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --locked
	cargo hack test --feature-powerset --locked
	$(MAKE) lint

# loom explores the wait core with at most four preemptions per execution, using
# a separate release cache so it leaves ordinary builds alone.
loom:
	RUSTFLAGS="--cfg loom" LOOM_MAX_PREEMPTIONS=4 CARGO_TARGET_DIR=target/loom cargo test --release --lib --locked loom_models

# lint checks the recommended clippy configuration in lint/clippy.toml against
# real calls, and that the README carries it verbatim.
lint:
	cargo fmt --manifest-path lint/Cargo.toml -- --check
	cd lint && cargo clippy --all-targets --locked -- -D warnings
	cd lint && cargo test --locked

# coverage measures the test coverage of the library code and opens the HTML
# report. It needs nightly to leave the test modules out of the numbers. The
# previous instrumented build is dropped first, as a test binary left behind
# by another toolchain would get merged into the report as uncovered code.
coverage:
	rm -rf target/llvm-cov-target
	cargo +nightly llvm-cov --html --open

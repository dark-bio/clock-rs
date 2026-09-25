.PHONY: check coverage
.DEFAULT_GOAL := check

# check runs the gates CI holds a push to, the formatting, clippy, the docs and
# the tests of every feature combination.
check:
	cargo fmt --all -- --check
	cargo hack clippy --feature-powerset --all-targets --locked -- -D warnings
	RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --locked
	cargo hack test --feature-powerset --locked

# coverage measures the test coverage of the library code and opens the HTML
# report. It needs nightly to leave the test modules out of the numbers. The
# previous instrumented build is dropped first, as a test binary left behind
# by another toolchain would get merged into the report as uncovered code.
coverage:
	rm -rf target/llvm-cov-target
	cargo +nightly llvm-cov --html --open

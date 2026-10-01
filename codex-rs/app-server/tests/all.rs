#![allow(clippy::expect_used)]
// Existing integration fixtures exercise the compatibility entrypoint directly.
#![allow(deprecated)]

// Single integration test binary that aggregates all test modules.
// The submodules live in `tests/suite/`.
mod suite;

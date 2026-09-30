//! Listing tasks: the list-mode reactor and the diff sides, both built on
//! one ListObjectsV2 continuation chain per key-range segment.
//!
//! - `reactor`: list mode's concurrency controller (segments, runtime
//!   splits, resume ranges).
//! - `split`: adaptive long-tail splitting (segment control, split probes,
//!   the fan-out governor).
//! - `chain`: one segment run to completion, with retry.
//! - `sdk_error`: SDK error classification and trace events.
//! - `diff`: parallel per-side listing for diff.

mod chain;
mod diff;
mod reactor;
mod sdk_error;
mod split;

pub use diff::{DIFF_SEGMENT_CHANNEL_CAP, diff_list_side_task};
pub use reactor::flat_list_main_task;

/// What a segment task's `JoinError` means for the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinFailure {
    /// The run is already shutting down and aborted this task on purpose.
    ShutdownCancel,
    /// The task died on its own — its key range is missing from the output.
    LostSegment,
}

/// Classify a segment task's join failure.
///
/// Only a cancellation raised *while the run is already quitting* is benign:
/// that is `abort_all` reaping siblings after some other failure. Anything
/// else means a segment stopped early, and the keys it owned are simply absent
/// from the output — indistinguishable downstream from a range that was empty.
/// Treating that as non-fatal is what let a panicking segment produce a short
/// Parquet under `status: success` with `fatal_errors: 0`.
pub(crate) fn classify_join_failure(is_cancelled: bool, run_is_quitting: bool) -> JoinFailure {
    if is_cancelled && run_is_quitting {
        JoinFailure::ShutdownCancel
    } else {
        JoinFailure::LostSegment
    }
}

#[cfg(test)]
mod join_failure_tests {
    use super::*;

    #[test]
    fn test_panicked_segment_is_a_lost_segment() {
        // A panic is never benign: the segment stopped mid-range and the keys
        // it owned are absent from the output, which nothing downstream can
        // distinguish from an empty range.
        assert_eq!(
            classify_join_failure(false, false),
            JoinFailure::LostSegment
        );
    }

    #[test]
    fn test_panicked_segment_stays_fatal_even_while_quitting() {
        // `is_quit()` alone must not excuse a join failure — the run may be
        // quitting *because* of this very panic, and the segment's keys are
        // missing either way.
        assert_eq!(classify_join_failure(false, true), JoinFailure::LostSegment);
    }

    #[test]
    fn test_cancellation_during_shutdown_is_benign() {
        // `abort_all` reaping siblings after some other failure already quit
        // the run: counting these would inflate fatal_errors on a clean stop.
        assert_eq!(
            classify_join_failure(true, true),
            JoinFailure::ShutdownCancel
        );
    }

    #[test]
    fn test_cancellation_without_shutdown_is_a_lost_segment() {
        // Nothing should cancel a segment while the run is healthy; if it
        // happens, the range is still missing and the run must not claim
        // success.
        assert_eq!(classify_join_failure(true, false), JoinFailure::LostSegment);
    }
}

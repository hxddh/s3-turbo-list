//! Diff mode is handled by dispatching the same KeySpace segments to both
//! source and target buckets in main.rs.  Both S3TaskContext instances share
//! the same bounded channel to the data_map task, which performs the matching
//! via ObjectProps::r#match().
//!
//! Diff is intentionally authoritative single-segment behavior. Left and right
//! list tasks run independently, and the data_map task handles matching as
//! objects arrive from either side. Hinted multi-segment diff and diff resume
//! are not part of the current architecture because incomplete segment pairing
//! can hide left-only or right-only objects.

use log::info;

// Called after both list tasks complete to signal the data_map task
// that all data for comparison has been sent.
pub fn diff_complete_notice() -> &'static str {
    "diff mode comparison complete — data_map will finalize output"
}

/// Initialize diff-specific state if needed.
pub fn init_diff_state() {
    info!("Diff mode initialized — objects from both sides will be compared by data_map");
}

//! Diff: each side runs the reactor with an ordered output
//! (`reactor::diff_side_task`); see `segment_order` for how the merge reads
//! a side in key order while its segments list, and split, in parallel.

/// Per-segment channel capacity (batches): how far one segment may list
/// ahead of the merge. A segment whose buffer is full waits, and frees its
/// concurrency slot for the segments the merge needs (see the reactor); a
/// side starts segments at most 128 ahead of the merge, so it buffers at
/// most 1,024 pages.
pub const DIFF_SEGMENT_CHANNEL_CAP: usize = 8;

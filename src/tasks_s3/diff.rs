//! Diff: each side runs the reactor with an ordered output
//! (`reactor::diff_side_task`); see `segment_order` for how the merge reads
//! a side in key order while its segments list, and split, in parallel.

/// Per-segment channel capacity (batches): how far one segment may list
/// ahead of the merge. At 4, a segment behind the merge head stalled after
/// four pages, so a side with a few large segments listed nearly serially
/// (one page per round trip).
pub const DIFF_SEGMENT_CHANNEL_CAP: usize = 32;

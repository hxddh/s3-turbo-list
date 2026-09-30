//! Diff: parallel per-side listing.

use super::chain::{flat_list_run_to_complete, quit_signalled};
use super::split::SegmentControl;
use super::{JoinFailure, classify_join_failure};
use crate::core::{KeySpaceHints, ObjectKey, ObjectProps, S3TaskContext};
use log::{error, info};
use std::sync::Arc;

//
// Diff sides list their static key-space segments in parallel; the merge
// consumes each side's segment channels in index order, which yields one
// globally ordered stream per side (segment k's keys all precede segment
// k+1's by boundary construction). Each segment's small channel acts as
// the prefetch window, bounding memory. Runtime splitting stays disabled
// for diff: the segment set must remain static for ordered consumption.

/// Per-segment channel capacity (batches): how far one segment may list
/// ahead of the merge.  At 4, a segment behind the merge head stalled after
/// four pages, so a side with a few large segments listed nearly serially
/// (one page per round trip).
pub const DIFF_SEGMENT_CHANNEL_CAP: usize = 32;
/// Upper bound on concurrently listing segments per diff side.
const DIFF_SIDE_MAX_CONCURRENCY: usize = 32;
/// Segments a side may start ahead of the one the merge is reading.  With
/// the channel capacity this bounds a side's buffered batches to
/// `DIFF_SIDE_LOOKAHEAD_SEGMENTS * DIFF_SEGMENT_CHANNEL_CAP` whatever the
/// segment count: finished segments used to keep their batches queued, so a
/// many-segment side could buffer most of the listing (RSS grew with bucket
/// size).
const DIFF_SIDE_LOOKAHEAD_SEGMENTS: usize = 16;

/// List one diff side across its static segments, writing each segment's
/// batches to the index-aligned sender. Any segment failure marks the run
/// fatal (non-zero exit), exactly like list mode.
pub async fn diff_list_side_task(
    ctx: &S3TaskContext,
    start_prefix: &str,
    concurrency: usize,
    boundaries: &[String],
    senders: Vec<tokio::sync::mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>>,
    // Index of the segment the merge is reading (see `DiffSideStream`).
    mut merge_head: Option<tokio::sync::watch::Receiver<usize>>,
) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    let mut hints = KeySpaceHints::new_from(boundaries);
    assert_eq!(
        hints.total_count(),
        senders.len(),
        "diff side senders must align with segments"
    );
    info!(
        "Diff List S3 Task — {} — started, {} segments",
        ctx.s3_bucket_name,
        senders.len()
    );

    let concurrency = concurrency.clamp(1, DIFF_SIDE_MAX_CONCURRENCY);
    let mut senders = senders.into_iter();
    let mut set = tokio::task::JoinSet::new();

    let mut next_pair = hints.next();
    let mut aborted = false;
    loop {
        while set.len() < concurrency {
            // After Ctrl-C (possibly during startup discovery) start nothing
            // new: the merge aborts, and a fresh request would only delay
            // the exit by up to its timeout.
            if ctx.is_quit() {
                next_pair = None;
                break;
            }
            let Some(pair) = next_pair.take() else { break };
            if let Some(head) = &merge_head {
                if pair.index >= *head.borrow() + DIFF_SIDE_LOOKAHEAD_SEGMENTS {
                    next_pair = Some(pair);
                    break;
                }
            }
            next_pair = hints.next();
            let sender = senders.next().expect("sender per segment");
            let mut task_ctx = ctx.clone();
            task_ctx.data_map_channel = sender;
            let start_prefix = start_prefix.to_string();
            let control = Arc::new(SegmentControl::new(pair.end));
            let index = pair.index;
            let start = pair.start;
            set.spawn(async move {
                flat_list_run_to_complete(&task_ctx, index, &start_prefix, &start, &control, None)
                    .await
            });
        }

        if set.is_empty() && next_pair.is_none() {
            break;
        }
        // Wait for a segment to finish or, when the next segment is held
        // back by the lookahead window, for the merge to move on.
        let joined = tokio::select! {
            joined = set.join_next(), if !set.is_empty() => joined,
            // Ctrl-C: stop the in-flight segments now (list mode's reactor
            // does the same); a request or a retry backoff would otherwise
            // hold the exit for up to its timeout. The merge aborts.
            _ = quit_signalled(ctx), if !set.is_empty() && !aborted => {
                set.abort_all();
                aborted = true;
                continue;
            }
            changed = async {
                match merge_head.as_mut() {
                    Some(head) => head.changed().await,
                    None => std::future::pending().await,
                }
            }, if next_pair.is_some() => {
                if changed.is_err() {
                    // The merge is gone; stop gating (the sends will fail).
                    merge_head = None;
                }
                continue;
            }
        };
        match joined {
            Some(Ok(_completed)) => {}
            Some(Err(e)) => {
                // Losing a segment is worse here than in list mode: the merge
                // sees the missing keys as absent from this side and reports
                // every one of them as a one-sided difference. Two identical
                // buckets came out as a full-bucket delta, under
                // `status: success`.
                match classify_join_failure(e.is_cancelled(), ctx.is_quit()) {
                    JoinFailure::ShutdownCancel => {
                        info!("Diff segment task cancelled during shutdown");
                    }
                    JoinFailure::LostSegment => {
                        error!(
                            "Diff List S3 Task — {} — segment task failed to join, its key range is missing from this side of the comparison: {:?}",
                            ctx.s3_bucket_name, e
                        );
                        ctx.g_state.inc_fatal_error();
                        ctx.g_state.quit();
                    }
                }
            }
            None => break,
        }
        if ctx.is_quit() {
            set.abort_all();
            info!("Diff List S3 Task — {} — aborted", ctx.s3_bucket_name);
            break;
        }
    }

    ctx.complete();
    info!("Diff List S3 Task — {} — completed", ctx.s3_bucket_name);
}

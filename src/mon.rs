use crate::core::{MonContext, DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS};
use log::info;
use std::time::{Duration, Instant};

/// How often the monitor re-checks the run's exit conditions.  The heartbeat
/// itself still prints every `DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS`, but
/// sleeping that long between checks held the process open for up to a full
/// heartbeat after the listing had finished — a fixed tail that dwarfed the
/// run itself on anything small.
const MON_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub async fn mon_task(ctx: MonContext) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Mon Task — started");
    let mut last_heartbeat: Option<Instant> = None;

    loop {
        if ctx.is_quit() {
            ctx.complete();
            info!("Mon Task — quit");
            return;
        }

        // Exit when all worker tasks (list + data_map) have finished.
        if !ctx.g_state.all_list_tasks_is_running() {
            ctx.complete();
            info!("Mon Task — all list tasks completed, exiting");
            return;
        }

        // Poll fast, report on the heartbeat interval.
        if last_heartbeat.is_some_and(|at| {
            at.elapsed() < Duration::from_secs(DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS)
        }) {
            tokio::time::sleep(MON_POLL_INTERVAL).await;
            continue;
        }
        last_heartbeat = Some(Instant::now());

        let tracker_stats = format!("{}", *ctx.get_tracker());
        if !tracker_stats.is_empty() {
            info!("Mon Task — http status: {}", tracker_stats);
        } else {
            info!("Mon Task — heartbeat (no HTTP errors)");
        }

        let stream_timeout = ctx.g_state.read_task_next_stream_timeout();
        let client_timeout = ctx.g_state.read_s3_client_timeout();
        let generic_error = ctx.g_state.read_s3_client_generic_error();
        if stream_timeout > 0 || client_timeout > 0 || generic_error > 0 {
            info!(
                "Mon Task — stream timeout: {}, client timeout: {}, generic error: {}",
                stream_timeout, client_timeout, generic_error
            );
        }

        tokio::time::sleep(MON_POLL_INTERVAL).await;
    }
}

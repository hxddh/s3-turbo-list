//! The request window: how many requests a run keeps in flight against the
//! endpoint, adapted to how the endpoint answers.
//!
//! One window, shared by every listing page and every partitioning probe,
//! is the run's only concurrency control. It starts at `-c` and follows
//! additive-increase / multiplicative-decrease:
//!
//! - a throttle (`SlowDown`, 429, ...) or a timed-out request shrinks it to
//!   70%, at most once per round trip, so one burst of rejections counts once;
//! - every `limit` requests answered without either grow it by one, up to
//!   `-c` again.
//!
//! On an endpoint that never pushes back, the window stays at `-c`: nothing
//! changes for it. On one with a request-rate ceiling it settles just under
//! that ceiling instead of queueing and retrying above it. Segments beyond
//! the window are cheap: they wait for a permit. Runtime splitting fills the
//! window, not `-c`.
//!
//! The window also times requests: a page may take ten times the run's
//! typical latency (never less than the configured floor, never more than a
//! minute) before it counts as timed out, so a slow but healthy endpoint is
//! not retried into the ground.

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Multiplicative decrease on a throttle or timeout.
const DECREASE: f64 = 0.7;
/// Longest a request may take before it counts as timed out.
const MAX_TIMEOUT: Duration = Duration::from_secs(60);
/// Request timeout in multiples of the typical latency.
const TIMEOUT_LATENCIES: f64 = 10.0;
/// A probe (optional work) gives up after this many typical latencies.
const PROBE_LATENCIES: f64 = 4.0;
/// Shortest probe deadline.
const MIN_PROBE_DEADLINE: Duration = Duration::from_secs(1);
/// How often a waiter re-checks without a wake-up (belt and braces).
const RECHECK: Duration = Duration::from_millis(50);

#[derive(Debug)]
struct State {
    max: usize,
    limit: f64,
    in_flight: usize,
    /// Answers since the last change of `limit`.
    answered: usize,
    last_decrease: Option<Instant>,
    /// Smoothed latency of answered requests, ms (`None`: none yet).
    latency_ms: Option<f64>,
    // Statistics.
    peak: usize,
    started: Instant,
    last_change: Instant,
    in_flight_ms: f64,
    list_requests: u64,
    probes: u64,
}

impl State {
    fn new(max: usize) -> Self {
        let now = Instant::now();
        Self {
            max: max.max(1),
            limit: max.max(1) as f64,
            in_flight: 0,
            answered: 0,
            last_decrease: None,
            latency_ms: None,
            peak: 0,
            started: now,
            last_change: now,
            in_flight_ms: 0.0,
            list_requests: 0,
            probes: 0,
        }
    }

    /// Account the time spent at the current in-flight count.
    fn tick(&mut self) {
        let now = Instant::now();
        self.in_flight_ms +=
            self.in_flight as f64 * now.duration_since(self.last_change).as_secs_f64() * 1000.0;
        self.last_change = now;
    }
}

/// The window. One per process (`global`).
#[derive(Debug)]
pub struct Window {
    state: Mutex<State>,
    freed: Notify,
}

/// What a request's answer tells the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Answered (successfully or with an error that is not about load).
    Answered(Duration),
    /// Throttled by the endpoint.
    Throttled,
    /// No answer before the request's timeout.
    TimedOut,
    /// Given up by the caller (a probe past its deadline): says nothing.
    Abandoned,
}

/// Which budget a request counts against in the statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    List,
    Probe,
}

/// A slot in the window, held for the duration of one request.
#[must_use]
pub struct Permit<'a> {
    window: &'a Window,
    done: bool,
}

impl Permit<'_> {
    /// Release the slot and report how the request went.
    pub fn finish(mut self, outcome: Outcome) {
        self.done = true;
        self.window.release(Some(outcome));
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.window.release(None);
        }
    }
}

/// Snapshot of the window's statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct WindowStats {
    pub list_requests: u64,
    pub probes: u64,
    pub peak_concurrency: usize,
    /// Time-weighted mean of requests in flight.
    pub avg_concurrency: f64,
    /// The window at the end of the run.
    pub final_limit: usize,
}

impl Window {
    pub fn new(max: usize) -> Self {
        Self {
            state: Mutex::new(State::new(max)),
            freed: Notify::new(),
        }
    }

    /// Wait for a slot.
    pub async fn acquire(&self, kind: Kind) -> Permit<'_> {
        loop {
            {
                let mut s = self.state.lock().unwrap();
                if s.in_flight < (s.limit.floor() as usize).max(1) {
                    s.tick();
                    s.in_flight += 1;
                    s.peak = s.peak.max(s.in_flight);
                    match kind {
                        Kind::List => s.list_requests += 1,
                        Kind::Probe => s.probes += 1,
                    }
                    return Permit {
                        window: self,
                        done: false,
                    };
                }
            }
            tokio::select! {
                _ = self.freed.notified() => {}
                _ = tokio::time::sleep(RECHECK) => {}
            }
        }
    }

    fn release(&self, outcome: Option<Outcome>) {
        {
            let mut s = self.state.lock().unwrap();
            s.tick();
            s.in_flight = s.in_flight.saturating_sub(1);
            match outcome {
                Some(Outcome::Answered(latency)) => {
                    let ms = latency.as_secs_f64() * 1000.0;
                    s.latency_ms = Some(match s.latency_ms {
                        Some(avg) => avg * 0.9 + ms * 0.1,
                        None => ms,
                    });
                    s.answered += 1;
                    if s.answered >= (s.limit as usize).max(1) && s.limit < s.max as f64 {
                        s.limit = (s.limit + 1.0).min(s.max as f64);
                        s.answered = 0;
                    }
                }
                Some(Outcome::Throttled | Outcome::TimedOut) => {
                    // Once per round trip: the requests already in flight
                    // were sent under the old window, and their rejections
                    // are the same signal.
                    let round = Duration::from_secs_f64(
                        (s.latency_ms.unwrap_or(100.0) * 2.0 / 1000.0).max(0.1),
                    );
                    if s.last_decrease.is_none_or(|t| t.elapsed() >= round) {
                        s.limit = (s.limit * DECREASE).max(1.0);
                        s.last_decrease = Some(Instant::now());
                        s.answered = 0;
                        log::info!(
                            "Request window: endpoint pushed back, window now {}",
                            s.limit as usize
                        );
                    }
                }
                Some(Outcome::Abandoned) | None => {}
            }
        }
        self.freed.notify_one();
    }

    /// The window (requests the run may have in flight now).
    pub fn limit(&self) -> usize {
        (self.state.lock().unwrap().limit.floor() as usize).max(1)
    }

    /// How long one request may take: ten typical latencies, at least
    /// `floor`, at most a minute.
    pub fn timeout(&self, floor: Duration) -> Duration {
        let latency = self.state.lock().unwrap().latency_ms;
        let adaptive = latency.map_or(Duration::ZERO, |ms| {
            Duration::from_secs_f64(ms * TIMEOUT_LATENCIES / 1000.0)
        });
        adaptive.max(floor).min(MAX_TIMEOUT.max(floor))
    }

    /// How long a probe waits for its answer before the caller goes on
    /// without it: four typical latencies (at least a second), never past the
    /// request timeout. Before any latency is known, the request timeout.
    pub fn probe_deadline(&self, floor: Duration) -> Duration {
        let timeout = self.timeout(floor);
        match self.state.lock().unwrap().latency_ms {
            Some(ms) => Duration::from_secs_f64(ms * PROBE_LATENCIES / 1000.0)
                .max(MIN_PROBE_DEADLINE)
                .min(timeout),
            None => timeout,
        }
    }

    pub fn stats(&self) -> WindowStats {
        let mut s = self.state.lock().unwrap();
        s.tick();
        let elapsed_ms = s.started.elapsed().as_secs_f64() * 1000.0;
        WindowStats {
            list_requests: s.list_requests,
            probes: s.probes,
            peak_concurrency: s.peak,
            avg_concurrency: if elapsed_ms > 0.0 {
                s.in_flight_ms / elapsed_ms
            } else {
                0.0
            },
            final_limit: (s.limit.floor() as usize).max(1),
        }
    }
}

static GLOBAL: OnceLock<Window> = OnceLock::new();

/// Set the run's window to `max` (`-c`). The first call wins; a library
/// user that never calls it gets a window of 512.
pub fn configure(max: usize) {
    let _ = GLOBAL.set(Window::new(max));
}

/// The run's window.
pub fn global() -> &'static Window {
    GLOBAL.get_or_init(|| Window::new(512))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn throttles_shrink_the_window_once_per_round_trip() {
        let w = Window::new(100);
        assert_eq!(w.limit(), 100);
        w.acquire(Kind::List)
            .await
            .finish(Outcome::Answered(Duration::from_millis(30)));
        for _ in 0..5 {
            w.acquire(Kind::List).await.finish(Outcome::Throttled);
        }
        assert_eq!(w.limit(), 70, "a burst of rejections counts once");
        tokio::time::sleep(Duration::from_millis(120)).await;
        w.acquire(Kind::List).await.finish(Outcome::TimedOut);
        assert_eq!(w.limit(), 49);
    }

    #[tokio::test]
    async fn answers_grow_the_window_back_to_its_max() {
        let w = Window::new(4);
        w.acquire(Kind::List).await.finish(Outcome::Throttled);
        assert_eq!(w.limit(), 2);
        for _ in 0..40 {
            w.acquire(Kind::List)
                .await
                .finish(Outcome::Answered(Duration::from_millis(10)));
        }
        assert_eq!(w.limit(), 4, "never past -c");
    }

    #[tokio::test]
    async fn requests_past_the_window_wait() {
        let w = std::sync::Arc::new(Window::new(2));
        let a = w.acquire(Kind::List).await;
        let _b = w.acquire(Kind::Probe).await;
        let waiting = {
            let w = w.clone();
            tokio::spawn(async move {
                let p = w.acquire(Kind::List).await;
                p.finish(Outcome::Abandoned);
            })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!waiting.is_finished());
        a.finish(Outcome::Answered(Duration::from_millis(5)));
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        let stats = w.stats();
        assert_eq!(
            (stats.list_requests, stats.probes, stats.peak_concurrency),
            (2, 1, 2)
        );
    }

    #[test]
    fn timeouts_follow_latency_within_bounds() {
        let w = Window::new(8);
        let floor = Duration::from_secs(10);
        assert_eq!(w.timeout(floor), floor);
        w.state.lock().unwrap().latency_ms = Some(3000.0);
        assert_eq!(w.timeout(floor), Duration::from_secs(30));
        w.state.lock().unwrap().latency_ms = Some(30_000.0);
        assert_eq!(w.timeout(floor), MAX_TIMEOUT);
        w.state.lock().unwrap().latency_ms = Some(30.0);
        assert_eq!(w.probe_deadline(floor), MIN_PROBE_DEADLINE);
    }
}

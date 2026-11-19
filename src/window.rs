//! The request window: how many requests a side of a run keeps in flight
//! against its endpoint, adapted to how the endpoint answers.
//!
//! One window per listing side (a list run has one, a diff one per side, so
//! an endpoint that throttles never slows the other side), shared by every
//! listing page and every partitioning probe of that side. It is capped at
//! `-c` and follows additive-increase / multiplicative-decrease from `-c`:
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
//! window, not `-c`. A listing page takes a whole slot and a probe a third:
//! probes are single-key or single-page requests sent in bursts at startup,
//! and the window holds three of them for every page.
//!
//! Latency alone does not move the window. An endpoint that queues instead
//! of throttling serves the same throughput whatever the queue (Little's
//! law); what such a queue cost was the probes partitioning sized to `-c`,
//! which partitioning no longer does. Reading latency as a queue was tried
//! and measured: latency also rises with jitter, with the client's own CPU
//! and at the end of a run, and shrinking for those made runs slower
//! (`docs/validation-results/capacity-sizing-20261118.md`).
//!
//! The window also times requests: a page may take ten times the side's
//! typical latency (never less than the configured floor, never more than a
//! minute) before it counts as timed out, so a slow but healthy endpoint is
//! not retried into the ground.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Share of a slot a probe takes, in thirds: a probe is a single-key or
/// single-page request, and startup partitioning sends many at once. A
/// listing page takes a whole slot.
const PROBE_UNITS: usize = 1;
const PAGE_UNITS: usize = 3;
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
/// Without `-c`: the window starts here and its cap rises to keep the
/// client busy at the endpoint's latency (see `open_auto`), up to
/// `AUTO_CEILING`.
pub const AUTO_START: usize = 100;
pub const AUTO_CEILING: usize = 1024;
/// Listing pages a core decodes and writes per second, with room to spare
/// (measured: about 1,500 of 1,000 keys).
const PAGES_PER_CORE_SECOND: f64 = 1000.0;
/// A listing page sent with at most this many pages in flight is quiet:
/// its latency is the endpoint's, not a queue's (the listing starts as one
/// segment while discovery runs, so its first pages are quiet).
const QUIET_PAGES: usize = 4;
/// Quiet pages the automatic cap is read off (their median latency).
const AUTO_SAMPLES: usize = 3;
/// Pages after which the cap is set from whatever pages there were.
const AUTO_FALLBACK_PAGES: usize = 32;

#[derive(Debug)]
struct State {
    max: usize,
    limit: f64,
    /// Slot units in flight (a page 3, a probe 1); the window holds
    /// `3 x limit`.
    units: usize,
    /// Requests in flight.
    in_flight: usize,
    /// Answers since the last change of `limit`.
    answered: usize,
    last_decrease: Option<Instant>,
    /// Smoothed latency of answered requests, ms (`None`: none yet).
    latency_ms: Option<f64>,
    /// Listing pages in flight.
    pages_in_flight: usize,
    /// Automatic cap still to set (no `-c`): up to this ceiling.
    auto_ceiling: Option<usize>,
    /// Latencies of quiet pages, and of all pages, until it is set, ms.
    quiet_ms: Vec<f64>,
    pages_ms: Vec<f64>,
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
            units: 0,
            in_flight: 0,
            answered: 0,
            last_decrease: None,
            latency_ms: None,
            pages_in_flight: 0,
            auto_ceiling: None,
            quiet_ms: Vec::new(),
            pages_ms: Vec::new(),
            peak: 0,
            started: now,
            last_change: now,
            in_flight_ms: 0.0,
            list_requests: 0,
            probes: 0,
        }
    }

    /// A listing page's latency, while the automatic cap is still to set.
    fn record_page(&mut self, ms: f64, quiet: bool) {
        let Some(ceiling) = self.auto_ceiling else {
            return;
        };
        if quiet {
            self.quiet_ms.push(ms);
        }
        self.pages_ms.push(ms);
        let samples = if self.quiet_ms.len() >= AUTO_SAMPLES {
            std::mem::take(&mut self.quiet_ms)
        } else if self.pages_ms.len() >= AUTO_FALLBACK_PAGES {
            std::mem::take(&mut self.pages_ms)
        } else {
            return;
        };
        self.auto_ceiling = None;
        self.quiet_ms = Vec::new();
        self.pages_ms = Vec::new();
        let mut samples = samples;
        samples.sort_by(f64::total_cmp);
        let latency_ms = samples[samples.len() / 2];
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get()) as f64;
        // Enough pages in flight to keep the client's cores busy at this
        // latency (Little's law), never below the start.
        let target = (cores * PAGES_PER_CORE_SECOND * latency_ms / 1000.0).round() as usize;
        let target = target.clamp(self.max, ceiling);
        if target > self.max {
            self.max = target;
            // Unless the endpoint already pushed back, open to it at once.
            if self.last_decrease.is_none() {
                self.limit = target as f64;
            }
            log::info!(
                "Request window: {:.0} ms a page, concurrency up to {}",
                latency_ms,
                target
            );
        }
    }

    /// Account the time spent at the current occupancy (in slot units).
    fn tick(&mut self) {
        let now = Instant::now();
        self.in_flight_ms +=
            self.units as f64 * now.duration_since(self.last_change).as_secs_f64() * 1000.0;
        self.last_change = now;
    }
}

/// The window of one listing side (see `open`).
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
    cost: usize,
    /// A listing page sent with few pages in flight (see `QUIET_PAGES`).
    quiet: bool,
    done: bool,
}

impl Permit<'_> {
    /// Release the slot and report how the request went.
    pub fn finish(mut self, outcome: Outcome) {
        self.done = true;
        self.window.release(self.cost, self.quiet, Some(outcome));
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.window.release(self.cost, self.quiet, None);
        }
    }
}

/// Snapshot of the window's statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct WindowStats {
    pub list_requests: u64,
    pub probes: u64,
    /// Most of the window in use at once, in slots (a page one, a probe a
    /// third): at most `-c` per side (a diff adds its sides' peaks).
    pub peak_concurrency: usize,
    /// Time-weighted mean of the window in use, in slots.
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
                let cost = match kind {
                    Kind::List => PAGE_UNITS,
                    Kind::Probe => PROBE_UNITS,
                };
                let capacity = (s.limit.floor() as usize).max(1) * PAGE_UNITS;
                if s.units + cost <= capacity {
                    s.tick();
                    let quiet = s.pages_in_flight <= QUIET_PAGES;
                    s.units += cost;
                    s.in_flight += 1;
                    if kind == Kind::List {
                        s.pages_in_flight += 1;
                    }
                    s.peak = s.peak.max(s.units);
                    match kind {
                        Kind::List => s.list_requests += 1,
                        Kind::Probe => s.probes += 1,
                    }
                    return Permit {
                        window: self,
                        cost,
                        quiet,
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

    fn release(&self, cost: usize, quiet: bool, outcome: Option<Outcome>) {
        {
            let mut s = self.state.lock().unwrap();
            s.tick();
            s.units = s.units.saturating_sub(cost);
            s.in_flight = s.in_flight.saturating_sub(1);
            if cost == PAGE_UNITS {
                s.pages_in_flight = s.pages_in_flight.saturating_sub(1);
            }
            match outcome {
                Some(Outcome::Answered(latency)) => {
                    let ms = latency.as_secs_f64() * 1000.0;
                    if cost == PAGE_UNITS {
                        s.record_page(ms, quiet);
                    }
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
            peak_concurrency: s.peak.div_ceil(PAGE_UNITS),
            avg_concurrency: if elapsed_ms > 0.0 {
                s.in_flight_ms / elapsed_ms / PAGE_UNITS as f64
            } else {
                0.0
            },
            final_limit: (s.limit.floor() as usize).max(1),
        }
    }
}

static OPENED: Mutex<Vec<Arc<Window>>> = Mutex::new(Vec::new());

/// A new window capped at `max` (`-c`), for one listing side. Every window
/// opened in the process counts in `run_stats`.
pub fn open(max: usize) -> Arc<Window> {
    let window = Arc::new(Window::new(max));
    OPENED.lock().unwrap().push(Arc::clone(&window));
    window
}

/// A window for a run without `-c`: it opens at `AUTO_START`, and once the
/// side's first quiet pages give the endpoint's latency, its cap rises to
/// what keeps the client's cores busy at that latency, up to `ceiling`. A
/// 30 ms endpoint stays near the start; at 200 ms, 100 requests in flight
/// listed half as fast as 256.
pub fn open_auto(ceiling: usize) -> Arc<Window> {
    let window = Window::new(AUTO_START.min(ceiling));
    window.state.lock().unwrap().auto_ceiling = Some(ceiling.max(AUTO_START));
    let window = Arc::new(window);
    OPENED.lock().unwrap().push(Arc::clone(&window));
    window
}

/// The run's statistics: every window's, added (a diff's two sides).
pub fn run_stats() -> WindowStats {
    let opened = OPENED.lock().unwrap();
    let mut total = WindowStats::default();
    for w in opened.iter() {
        let s = w.stats();
        total.list_requests += s.list_requests;
        total.probes += s.probes;
        total.peak_concurrency += s.peak_concurrency;
        total.avg_concurrency += s.avg_concurrency;
        total.final_limit += s.final_limit;
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn throttles_shrink_the_window_once_per_round_trip() {
        let w = Window::new(100);
        assert_eq!(w.limit(), 100, "opens at -c");
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
        // Two slots: one page and three probes fill them.
        let w = std::sync::Arc::new(Window::new(2));
        let a = w.acquire(Kind::List).await;
        let _probes = [
            w.acquire(Kind::Probe).await,
            w.acquire(Kind::Probe).await,
            w.acquire(Kind::Probe).await,
        ];
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
            (2, 3, 2)
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

    #[tokio::test]
    async fn each_side_has_its_own_window() {
        let (left, right) = (open(10), open(10));
        left.acquire(Kind::List).await.finish(Outcome::Throttled);
        assert_eq!(left.limit(), 7);
        assert_eq!(
            right.limit(),
            10,
            "a throttled side does not slow the other"
        );
        right
            .acquire(Kind::List)
            .await
            .finish(Outcome::Answered(Duration::from_millis(5)));
        let total = run_stats();
        assert!(total.list_requests >= 2 && total.final_limit >= 17);
    }

    #[tokio::test]
    async fn without_c_the_cap_rises_to_the_endpoint_latency() {
        let w = open_auto(AUTO_CEILING);
        assert_eq!(w.limit(), AUTO_START);
        // Three quiet pages at 200 ms: enough in flight to keep every core
        // busy (1,000 pages a second each), within the ceiling.
        for _ in 0..AUTO_SAMPLES {
            w.acquire(Kind::List)
                .await
                .finish(Outcome::Answered(Duration::from_millis(200)));
        }
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
        let expected = (cores * 200).clamp(AUTO_START, AUTO_CEILING);
        assert_eq!(w.limit(), expected);
        // A fast endpoint keeps the start.
        let fast = open_auto(AUTO_CEILING);
        for _ in 0..AUTO_SAMPLES {
            fast.acquire(Kind::List)
                .await
                .finish(Outcome::Answered(Duration::from_millis(5)));
        }
        assert_eq!(fast.limit(), AUTO_START);
    }
}

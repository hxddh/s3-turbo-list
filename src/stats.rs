use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct HttpStatusCodeTracker {
    map: DashMap<u16, Arc<AtomicUsize>>,
}

impl HttpStatusCodeTracker {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    /// Increment the counter for a status code. Uses entry API to
    /// prevent lost updates when two callers race on a first-seen code.
    #[allow(dead_code)] // Phase 5: async variant retained for future async callers
    pub async fn inc(&self, code: u16) {
        self.inc_sync(code);
    }

    /// Synchronous increment — safe because DashMap entry API is lock-free.
    pub fn inc_sync(&self, code: u16) {
        self.map
            .entry(code)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> Vec<(u16, usize)> {
        let mut v: Vec<_> = self
            .map
            .iter()
            .map(|entry| (*entry.key(), entry.value().load(Ordering::SeqCst)))
            .collect();
        v.sort_by_key(|(code, _)| *code);
        v
    }
}

impl std::fmt::Display for HttpStatusCodeTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snap = self.snapshot();
        let stats: Vec<String> = snap
            .iter()
            .map(|(code, count)| format!("[{} => {}]", code, count))
            .collect();
        write!(f, "{}", stats.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[tokio::test]
    async fn test_single_code_inc_and_snapshot() {
        let tracker = HttpStatusCodeTracker::new();
        tracker.inc(200).await;
        tracker.inc(200).await;
        tracker.inc(404).await;

        let snap = tracker.snapshot();
        assert_eq!(snap, vec![(200, 2), (404, 1)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_inc_same_code_no_lost_updates() {
        let tracker = Arc::new(HttpStatusCodeTracker::new());
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let t = Arc::clone(&tracker);
            let b = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                b.wait(); // all tasks start together
                for _ in 0..250 {
                    t.inc(500).await;
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let snap = tracker.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0], (500, 1000), "4 tasks x 250 increments = 1000");
    }
}

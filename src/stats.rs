use std::collections::BTreeMap;
use std::sync::Mutex;

/// Response counts per HTTP status code. Updated once per request, so an
/// uncontended mutex costs nothing measurable, and the map keeps the codes
/// sorted for the snapshot.
pub struct HttpStatusCodeTracker {
    map: Mutex<BTreeMap<u16, usize>>,
}

impl HttpStatusCodeTracker {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn inc_sync(&self, code: u16) {
        *self.map.lock().unwrap().entry(code).or_insert(0) += 1;
    }

    pub fn snapshot(&self) -> Vec<(u16, usize)> {
        self.map
            .lock()
            .unwrap()
            .iter()
            .map(|(code, count)| (*code, *count))
            .collect()
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
    use std::sync::{Arc, Barrier};

    #[tokio::test]
    async fn test_single_code_inc_and_snapshot() {
        let tracker = HttpStatusCodeTracker::new();
        tracker.inc_sync(200);
        tracker.inc_sync(200);
        tracker.inc_sync(404);

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
                    t.inc_sync(500);
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

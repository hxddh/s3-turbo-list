//! The key order of one diff side's segments, as they split at runtime.
//!
//! A diff merges each side as one ascending stream, reading the side's
//! segments one after another. Segments are listed concurrently, each into
//! its own bounded channel. A segment that splits at runtime keeps
//! `(start, cut]` and hands `(cut, end]` to a new child, so the child's keys
//! come right after the parent's and before anything the parent was followed
//! by. The order is therefore a linked list: a new child is inserted directly
//! after its parent (a parent that splits twice puts the second, lower child
//! before the first).
//!
//! The merge may only move past a segment once no more children can be
//! linked after it. The listing seals a segment when its task has joined and
//! every split it sent has been linked; `successor` waits for that.

use crate::core::{ObjectKey, ObjectProps};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc::{Receiver, Sender, channel};

pub type Batch = Vec<(ObjectKey, ObjectProps)>;

#[derive(Default)]
struct Inner {
    first: Option<usize>,
    next: HashMap<usize, usize>,
    senders: HashMap<usize, Sender<Batch>>,
    receivers: HashMap<usize, Receiver<Batch>>,
    sealed: HashSet<usize>,
    /// The static segment each segment descends from (itself for static
    /// ones): the merge publishes it to gate how far the listing runs ahead.
    root: HashMap<usize, usize>,
    /// Everything is sealed (the listing ended or was aborted).
    all_sealed: bool,
    /// The segment the merge reached last (took the receiver of).
    reached: Option<usize>,
}

/// One side's segment order and channels.
pub struct SegmentOrder {
    inner: Mutex<Inner>,
    changed: Notify,
    capacity: usize,
}

impl SegmentOrder {
    /// `segments` static segments `0..segments`, in order, each with a
    /// channel of `capacity` batches.
    pub fn new(segments: usize, capacity: usize) -> Self {
        let order = Self {
            inner: Mutex::new(Inner::default()),
            changed: Notify::new(),
            capacity: capacity.max(1),
        };
        {
            let mut inner = order.inner.lock().unwrap();
            inner.first = (segments > 0).then_some(0);
            for id in 0..segments {
                let (tx, rx) = channel(order.capacity);
                inner.senders.insert(id, tx);
                inner.receivers.insert(id, rx);
                inner.root.insert(id, id);
                if id + 1 < segments {
                    inner.next.insert(id, id + 1);
                }
            }
        }
        order
    }

    /// A fixed order over existing receivers, all sealed (no splitting).
    pub fn from_receivers(receivers: Vec<Receiver<Batch>>) -> Self {
        let order = Self::new(0, 1);
        {
            let mut inner = order.inner.lock().unwrap();
            let n = receivers.len();
            inner.first = (n > 0).then_some(0);
            for (id, rx) in receivers.into_iter().enumerate() {
                inner.receivers.insert(id, rx);
                inner.root.insert(id, id);
                inner.sealed.insert(id);
                if id + 1 < n {
                    inner.next.insert(id, id + 1);
                }
            }
        }
        order
    }

    /// The sender a segment task writes to (once).
    pub fn take_sender(&self, id: usize) -> Option<Sender<Batch>> {
        self.inner.lock().unwrap().senders.remove(&id)
    }

    /// Link `child` (a range split off `parent`) right after `parent`.
    pub fn insert_after(&self, parent: usize, child: usize) {
        let (tx, rx) = channel(self.capacity);
        let mut inner = self.inner.lock().unwrap();
        debug_assert!(!inner.sealed.contains(&parent), "parent already sealed");
        if let Some(after) = inner.next.insert(parent, child) {
            inner.next.insert(child, after);
        }
        let root = inner.root.get(&parent).copied().unwrap_or(parent);
        inner.root.insert(child, root);
        inner.senders.insert(child, tx);
        inner.receivers.insert(child, rx);
    }

    /// No more children will be linked after `id`.
    pub fn seal(&self, id: usize) {
        self.inner.lock().unwrap().sealed.insert(id);
        self.changed.notify_waiters();
    }

    /// The listing is over (finished, failed or aborted): release a merge
    /// waiting for a seal. Unstarted segments' senders are dropped, so their
    /// channels read as closed.
    pub fn seal_all(&self) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.all_sealed = true;
            inner.senders.clear();
        }
        self.changed.notify_waiters();
    }

    pub fn first(&self) -> Option<usize> {
        self.inner.lock().unwrap().first
    }

    pub fn root(&self, id: usize) -> usize {
        self.inner
            .lock()
            .unwrap()
            .root
            .get(&id)
            .copied()
            .unwrap_or(id)
    }

    /// Whether `id` is among the first `n` segments from where the merge
    /// reads (the segment it reached last, or the first). A diff side
    /// starts only those: the segments the merge reads next, whatever was
    /// cut or started before them, and never more than `n` ahead of it.
    pub fn within_reach(&self, id: usize, n: usize) -> bool {
        let inner = self.inner.lock().unwrap();
        let mut at = inner.reached.or(inner.first);
        for _ in 0..n {
            match at {
                Some(seg) if seg == id => return true,
                Some(seg) => at = inner.next.get(&seg).copied(),
                None => return false,
            }
        }
        false
    }

    /// The receiver of `id` (once).
    pub fn take_receiver(&self, id: usize) -> Option<Receiver<Batch>> {
        let mut inner = self.inner.lock().unwrap();
        inner.reached = Some(id);
        inner.receivers.remove(&id)
    }

    /// The segment after `id`, once `id` is sealed.
    pub async fn successor(&self, id: usize) -> Option<usize> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let inner = self.inner.lock().unwrap();
                if inner.all_sealed || inner.sealed.contains(&id) {
                    return inner.next.get(&id).copied();
                }
            }
            notified.await;
        }
    }
}

/// Seals everything when dropped, so a listing that ends any way (including
/// a panic) never leaves the merge waiting.
pub struct SealOnDrop<'a>(pub &'a SegmentOrder);

impl Drop for SealOnDrop<'_> {
    fn drop(&mut self) {
        self.0.seal_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn walk(order: &SegmentOrder) -> Vec<usize> {
        let mut out = Vec::new();
        let mut cur = order.first();
        while let Some(id) = cur {
            out.push(id);
            cur = order.successor(id).await;
        }
        out
    }

    #[tokio::test]
    async fn children_go_right_after_their_parent() {
        let order = SegmentOrder::new(3, 4);
        order.insert_after(0, 10); // 0 splits: (cut, end0] -> 10
        order.insert_after(0, 11); // 0 splits lower: 11 before 10
        order.insert_after(10, 12); // 10 splits: 12 after 10
        for id in [0, 1, 2, 10, 11, 12] {
            order.seal(id);
        }
        assert_eq!(walk(&order).await, vec![0, 11, 10, 12, 1, 2]);
        assert_eq!(order.root(12), 0);
        assert_eq!(order.root(2), 2);
    }

    #[tokio::test]
    async fn successor_waits_for_the_seal() {
        let order = std::sync::Arc::new(SegmentOrder::new(2, 4));
        let waiter = {
            let order = order.clone();
            tokio::spawn(async move { order.successor(0).await })
        };
        tokio::task::yield_now().await;
        order.insert_after(0, 5);
        order.seal(0);
        assert_eq!(waiter.await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn seal_all_releases_waiters_and_closes_unstarted_channels() {
        let order = std::sync::Arc::new(SegmentOrder::new(2, 4));
        let mut rx1 = order.take_receiver(1).unwrap();
        {
            let _guard = SealOnDrop(&order);
        }
        assert_eq!(order.successor(0).await, Some(1));
        assert!(
            rx1.recv().await.is_none(),
            "unstarted segment reads as closed"
        );
    }

    #[tokio::test]
    async fn reach_counts_from_where_the_merge_reads() {
        let order = SegmentOrder::new(3, 4);
        // 0 -> 5 -> 1 -> 2: a child cut from 0 comes right after it.
        order.insert_after(0, 5);
        assert!(order.within_reach(0, 1));
        assert!(order.within_reach(5, 2) && !order.within_reach(1, 2));
        // The merge moves on to 5: reach now counts from there.
        let _ = order.take_receiver(0);
        let _ = order.take_receiver(5);
        assert!(order.within_reach(2, 3));
        assert!(!order.within_reach(0, 3), "behind the merge");
    }
}

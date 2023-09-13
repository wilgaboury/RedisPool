use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam_queue::SegQueue;

pub struct TtlQueueItem<T> {
    item: T,
    time: Instant,
}
pub struct TtlQueue<T> {
    pool: Arc<SegQueue<TtlQueueItem<T>>>,
    ttl: Duration,
}

impl<T> TtlQueue<T> {
    pub fn new(ttl: Duration) -> Self {
        TtlQueue {
            pool: Arc::new(SegQueue::new()),
            ttl,
        }
    }

    pub fn pop(&self) -> Option<T> {
        let now = Instant::now();
        while let Some(item) = self.pool.pop() {
            if now - item.time < self.ttl {
                return Some(item.item);
            }
        }
        None
    }

    pub fn push(&self, item: T) {
        self.pool.push(TtlQueueItem {
            item,
            time: Instant::now(),
        })
    }
}

impl<T> Clone for TtlQueue<T> {
    fn clone(&self) -> Self {
        TtlQueue {
            pool: self.pool.clone(),
            ttl: self.ttl.clone(),
        }
    }
}

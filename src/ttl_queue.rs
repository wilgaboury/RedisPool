use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam_queue::SegQueue;

pub struct TtlPool<T> {
    pub(crate) item: T,
    pub(crate) time: Instant,
}
pub struct TtlQueue<T> {
    pool: Arc<SegQueue<TtlPool<T>>>,
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
            if self.pool.is_empty() || now - item.time < self.ttl {
                return Some(item.item);
            }
        }
        None
    }

    pub fn push(&self, item: T) {
        self.pool.push(TtlPool {
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

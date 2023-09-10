use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam_queue::SegQueue;

pub struct DynPoolItem<T> {
    pub(crate) item: T,
    pub(crate) time: Instant,
}
pub struct DynPool<T> {
    pool: Arc<SegQueue<DynPoolItem<T>>>,
    ttl: Duration,
}

impl<T> DynPool<T> {
    pub fn new(ttl: Duration) -> Self {
        DynPool {
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
        self.pool.push(DynPoolItem {
            item,
            time: Instant::now(),
        })
    }
}

impl<T> Clone for DynPool<T> {
    fn clone(&self) -> Self {
        DynPool {
            pool: self.pool.clone(),
            ttl: self.ttl.clone(),
        }
    }
}

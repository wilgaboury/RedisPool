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
    ttl: Option<Duration>,
}

impl<T> TtlQueue<T> {
    pub fn new(ttl: Option<Duration>) -> Self {
        TtlQueue {
            pool: Arc::new(SegQueue::new()),
            ttl,
        }
    }

    pub fn pop(&self) -> Option<T> {
        while let Some(item) = self.pool.pop() {
            match self.ttl {
                Some(ttl) => {
                    let now = Instant::now();
                    if now - item.time < ttl {
                        return Some(item.item);
                    }
                }
                None => return Some(item.item),
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

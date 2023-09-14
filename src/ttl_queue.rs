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

#[test]
fn test_eviction() {
    let queue: TtlQueue<usize> = TtlQueue::new(Some(Duration::from_secs(1)));

    queue.push(1);
    queue.push(2);
    queue.push(3);

    std::thread::sleep(Duration::from_secs(2));

    assert_eq!(None, queue.pop());

    queue.push(1);
    queue.push(2);

    assert_eq!(Some(1), queue.pop());
    assert_eq!(Some(2), queue.pop());
    assert_eq!(None, queue.pop());
}

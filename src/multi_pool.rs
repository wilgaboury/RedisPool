use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use redis::{aio::ConnectionLike, Cmd, RedisFuture, Value};

/// A pool with a fixed number of multiplexed and managed connections
///
/// This pool is very dumb and meant for requests that will complete
/// quickly. Internally it will simply round robin redis requests to
/// each connection. Long running operations will block other requests
/// and likely cause performance issues.
#[derive(Clone)]
pub struct MultiPool<M>
where
    M: ConnectionLike + Send + Sync + Clone,
{
    pool: Vec<M>,
    rr: Arc<AtomicUsize>,
}

impl<M> MultiPool<M>
where
    M: ConnectionLike + Send + Sync + Clone,
{
    pub fn new(pool: Vec<M>) -> Self {
        MultiPool {
            pool,
            rr: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<M> ConnectionLike for MultiPool<M>
where
    M: ConnectionLike + Send + Sync + Clone,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let idx = self.rr.fetch_add(1, Ordering::Relaxed);
        let len = self.pool.len();
        (&mut self.pool[idx % len]).req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        let idx = self.rr.fetch_add(1, Ordering::Relaxed);
        let len = self.pool.len();
        (&mut self.pool[idx % len]).req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        let idx = self.rr.fetch_add(1, Ordering::Relaxed);
        (&self.pool[idx % self.pool.len()]).get_db()
    }
}

const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    fn assert_all<F, M, C>()
    where
        M: redis::aio::ConnectionLike + Send + Sync + Clone,
    {
        assert_send::<MultiPool<M>>();
        assert_sync::<MultiPool<M>>();
    }
};

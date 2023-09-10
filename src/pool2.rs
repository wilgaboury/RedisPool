use crate::{
    connection2::RedisPoolConnection2, dyn_pool::DynPool, errors::RedisPoolError,
    factory::ConnectionFactory,
};
use futures::future::join_all;
use itertools::Itertools;
use redis::{
    aio::{Connection, ConnectionLike, MultiplexedConnection},
    Client, Cmd, RedisFuture, RedisResult, Value,
};
use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    select,
    sync::{oneshot, Semaphore},
};

pub const DEFAULT_POOL_SIZE: usize = 4;
pub const DEFAULT_CON_LIMIT: usize = 512;
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);

struct DropSignal {
    sender: Option<oneshot::Sender<()>>,
}

impl DropSignal {
    pub fn new(sender: oneshot::Sender<()>) -> Self {
        DropSignal {
            sender: Some(sender),
        }
    }
}

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}

pub struct RedisPool2<F, M, C>
where
    F: ConnectionFactory<C> + Send + Sync + Clone,
    M: redis::aio::ConnectionLike + Clone,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    factory: F,
    fast_pool: Vec<M>,
    fast_rr_idx: Arc<AtomicUsize>,
    dyn_pool: DynPool<C>,
    dyn_sem: Option<Arc<Semaphore>>, // cannot currently handle con_limit of zero
    dyn_shutdown_cleaner: Option<Arc<DropSignal>>,
}

impl<F, M, C> RedisPool2<F, M, C>
where
    F: ConnectionFactory<C> + Send + Sync + Clone,
    M: redis::aio::ConnectionLike + Clone,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    pub fn new(factory: F, fast_pool: Vec<M>, con_limit: Option<usize>, ttl: Duration) -> Self {
        let dyn_pool = DynPool::new(ttl);
        let (dyn_sem, dyn_shutdown_cleaner) =
            Self::setup_dyn_pool(dyn_pool.clone(), con_limit, ttl);

        return RedisPool2 {
            factory,
            fast_pool: fast_pool,
            fast_rr_idx: Arc::new(AtomicUsize::new(0)),
            dyn_pool,
            dyn_sem,
            dyn_shutdown_cleaner,
        };
    }

    fn setup_dyn_pool(
        dyn_pool: DynPool<C>,
        con_limit: Option<usize>,
        ttl: Duration,
    ) -> (Option<Arc<Semaphore>>, Option<Arc<DropSignal>>) {
        match con_limit {
            Some(0) => (None, None),
            Some(lim) => {
                let (tx, rx) = oneshot::channel();
                Self::launch_dyn_cleaner(dyn_pool.clone(), rx, ttl);
                (
                    Some(Arc::new(Semaphore::new(lim))),
                    Some(Arc::new(DropSignal::new(tx))),
                )
            }
            None => {
                let (tx, rx) = oneshot::channel();
                Self::launch_dyn_cleaner(dyn_pool.clone(), rx, ttl);
                (None, Some(Arc::new(DropSignal::new(tx))))
            }
        }
    }

    fn launch_dyn_cleaner(dyn_pool: DynPool<C>, mut rx: oneshot::Receiver<()>, ttl: Duration) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(ttl);
            loop {
                select! {
                    _ = interval.tick() => {
                        if let Some(item) = dyn_pool.pop() {
                            dyn_pool.push(item)
                        }
                    }
                    _ = &mut rx => return
                }
            }
        });
    }

    pub async fn aquire(&self) -> Result<RedisPoolConnection2<C>, RedisPoolError> {
        let permit = match &self.dyn_sem {
            Some(sem) => Some(sem.clone().acquire_owned().await?),
            None => None,
        };
        let con = self.aquire_connection().await?;
        Ok(RedisPoolConnection2::new(
            con,
            permit,
            self.dyn_pool.clone(),
        ))
    }

    async fn aquire_connection(&self) -> RedisResult<C> {
        while let Some(mut con) = self.dyn_pool.pop() {
            let res = redis::Pipeline::with_capacity(2)
                .cmd("UNWATCH")
                .ignore()
                .cmd("PING")
                .arg(1)
                .query_async::<_, (usize,)>(&mut con)
                .await;

            match res {
                Ok((1,)) => {
                    return Ok(con);
                }
                Ok(_) => {
                    tracing::trace!("connection ping returned wrong value");
                }
                Err(e) => {
                    tracing::trace!("bad redis connection: {}", e);
                }
            }
        }

        self.factory.create().await
    }
}

impl<F, M, C> Clone for RedisPool2<F, M, C>
where
    F: ConnectionFactory<C> + Send + Sync + Clone,
    M: redis::aio::ConnectionLike + Clone,
    C: redis::aio::ConnectionLike + Send,
{
    fn clone(&self) -> Self {
        return RedisPool2 {
            factory: self.factory.clone(),
            fast_pool: self.fast_pool.clone(),
            fast_rr_idx: self.fast_rr_idx.clone(),
            dyn_pool: self.dyn_pool.clone(),
            dyn_sem: self.dyn_sem.clone(),
            dyn_shutdown_cleaner: self.dyn_shutdown_cleaner.clone(),
        };
    }
}

impl<F, M, C> Deref for RedisPool2<F, M, C>
where
    F: ConnectionFactory<C> + Send + Sync + Clone,
    M: redis::aio::ConnectionLike + Clone,
    C: redis::aio::ConnectionLike + Send,
{
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.factory
    }
}

impl<F, M, C> ConnectionLike for RedisPool2<F, M, C>
where
    F: ConnectionFactory<C> + Send + Sync + Clone,
    M: redis::aio::ConnectionLike + Clone + 'static,
    C: redis::aio::ConnectionLike + Send,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let idx = self.fast_rr_idx.fetch_add(1, Ordering::Relaxed);
        let len = self.fast_pool.len();
        (&mut self.fast_pool[idx % len]).req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        let idx = self.fast_rr_idx.fetch_add(1, Ordering::Relaxed);
        let len = self.fast_pool.len();
        (&mut self.fast_pool[idx % len]).req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        let idx = self.fast_rr_idx.fetch_add(1, Ordering::Relaxed);
        (&self.fast_pool[idx % self.fast_pool.len()]).get_db()
    }
}

pub type SingleRedisPool2 = RedisPool2<Client, MultiplexedConnection, Connection>;

impl SingleRedisPool2 {
    pub async fn new_single(
        client: Client,
        pool_size: usize,
        con_limit: Option<usize>,
        ttl: Duration,
    ) -> Result<Self, RedisPoolError> {
        let fast_pool: Vec<MultiplexedConnection> = join_all((0..pool_size).map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.get_multiplexed_async_connection().await })
        }))
        .await
        .into_iter()
        .process_results(|iter| iter.collect::<Vec<_>>())?
        .into_iter()
        .process_results(|iter| iter.collect::<Vec<_>>())?;

        Ok(RedisPool2::new(client, fast_pool, con_limit, ttl))
    }
}

// compile time assert pool thread saftey
const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    fn assert_all<F, M, C>()
    where
        F: ConnectionFactory<C> + Send + Sync + Clone,
        M: redis::aio::ConnectionLike + Send + Sync + Clone,
        C: redis::aio::ConnectionLike + Send,
    {
        assert_send::<RedisPool2<F, M, C>>();
        assert_sync::<RedisPool2<F, M, C>>();
    }
};

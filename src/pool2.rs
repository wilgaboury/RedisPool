use crate::{
    dyn_pool::{DynPool, DynPoolCon},
    errors::RedisPoolError,
    factory::ConnectionFactory,
    multi_pool::MultiPool,
};
use futures::future::join_all;
use redis::{
    aio::{Connection, ConnectionLike, MultiplexedConnection},
    Client, Cmd, RedisFuture, Value,
};
use std::time::Duration;

pub const DEFAULT_POOL_SIZE: usize = 4;
pub const DEFAULT_CON_LIMIT: usize = 512;
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);

pub struct RedisPool2<M, F, C>
where
    M: redis::aio::ConnectionLike + Send + Sync + Clone,
    F: ConnectionFactory<C> + Send + Sync + Clone,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    multi_pool: MultiPool<M>,
    dyn_pool: Option<DynPool<F, C>>,
}

impl<M, F, C> RedisPool2<M, F, C>
where
    M: redis::aio::ConnectionLike + Send + Sync + Clone,
    F: ConnectionFactory<C> + Send + Sync + Clone,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    pub fn new(multi_pool: MultiPool<M>, dyn_pool: Option<DynPool<F, C>>) -> Self {
        RedisPool2 {
            multi_pool,
            dyn_pool,
        }
    }

    pub async fn aquire(&self) -> Result<DynPoolCon<C>, RedisPoolError> {
        self.dyn_pool
            .as_ref()
            .ok_or_else(|| RedisPoolError::MissingDynamicPool)?
            .aquire()
            .await
    }
}

impl<M, F, C> Clone for RedisPool2<M, F, C>
where
    M: redis::aio::ConnectionLike + Send + Sync + Clone,
    F: ConnectionFactory<C> + Send + Sync + Clone,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    fn clone(&self) -> Self {
        return RedisPool2 {
            multi_pool: self.multi_pool.clone(),
            dyn_pool: self.dyn_pool.clone(),
        };
    }
}

impl<M, F, C> ConnectionLike for RedisPool2<M, F, C>
where
    M: redis::aio::ConnectionLike + Send + Sync + Clone,
    F: ConnectionFactory<C> + Send + Sync + Clone,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        self.multi_pool.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        self.multi_pool.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.multi_pool.get_db()
    }
}

pub type SingleRedisPool2 = RedisPool2<MultiplexedConnection, Client, Connection>;

pub struct DynPoolArgs {
    pub ttl: Option<Duration>,
    pub limit: Option<usize>,
}

impl SingleRedisPool2 {
    pub async fn new_single(
        client: Client,
        size: usize,
        dyn_args: Option<DynPoolArgs>,
    ) -> Result<Self, RedisPoolError> {
        let mut pool = Vec::with_capacity(size);
        for res in join_all((0..size).map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.get_multiplexed_async_connection().await })
        }))
        .await
        {
            pool.push(res??);
        }

        Ok(RedisPool2::new(
            MultiPool::new(pool),
            dyn_args.map(|args| DynPool::new(client, args.ttl, args.limit)),
        ))
    }
}

// compile time assert pool thread saftey
const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    fn assert_all<F, M, C>()
    where
        M: redis::aio::ConnectionLike + Send + Sync + Clone,
        F: ConnectionFactory<C> + Send + Sync + Clone,
        C: redis::aio::ConnectionLike + Send,
    {
        assert_send::<RedisPool2<M, F, C>>();
        assert_sync::<RedisPool2<M, F, C>>();
    }
};

use crate::{connection::RedisPoolConnection, errors::RedisPoolError, factory::ConnectionFactory};
use crossbeam_queue::ArrayQueue;
use redis::{aio::Connection, Client, RedisResult};
use std::{ops::Deref, sync::Arc};
use tokio::sync::Semaphore;

pub type DefaultRedisPool = RedisPool<Client, Connection>;

pub(crate) const DEFAULT_POOL_LIMIT: usize = 16;

#[derive(Clone)]
pub struct RedisPool<F, C>
where
    F: ConnectionFactory<C>,
    C: redis::aio::ConnectionLike + Send,
{
    pub(crate) client: F,
    pub(crate) queue: Arc<ArrayQueue<C>>,
    pub(crate) sem: Arc<Semaphore>,
}

impl<F, C> RedisPool<F, C>
where
    F: ConnectionFactory<C>,
    C: redis::aio::ConnectionLike + Send,
{
    pub async fn aquire(&self) -> Result<RedisPoolConnection<C>, RedisPoolError> {
        let permit = self.sem.clone().acquire_owned().await?;
        let con = self.aquire_connection().await?;
        let queue = Arc::downgrade(&self.queue);
        Ok(RedisPoolConnection::new(con, queue, permit))
    }

    async fn aquire_connection(&self) -> RedisResult<C> {
        if let Some(mut con) = self.queue.as_ref().pop() {
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
                    tracing::trace!("{}", e);
                }
            }
        }

        self.client.create().await
    }
}

impl<F, C> Deref for RedisPool<F, C>
where
    F: ConnectionFactory<C>,
    C: redis::aio::ConnectionLike + Send,
{
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DefaultRedisPool {
    pub fn from_client(client: Client, limit: usize) -> Self {
        RedisPool {
            client,
            queue: Arc::new(ArrayQueue::new(limit)),
            sem: Arc::new(Semaphore::new(limit)),
        }
    }
}

impl From<Client> for DefaultRedisPool {
    fn from(value: Client) -> Self {
        RedisPool::from_client(value, DEFAULT_POOL_LIMIT)
    }
}

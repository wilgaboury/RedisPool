use std::ops::{Deref, DerefMut};
use std::{sync::Arc, time::Duration};

use redis::aio::ConnectionLike;
use redis::{Cmd, RedisFuture, RedisResult, Value};
use tokio::{
    select,
    sync::{oneshot, OwnedSemaphorePermit, Semaphore},
};

use crate::errors::RedisPoolError;
use crate::{factory::ConnectionFactory, ttl_queue::TtlQueue};

pub struct DynPool<F, C>
where
    F: ConnectionFactory<C> + Send + Sync + Clone,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    factory: F,
    pool: TtlQueue<C>,
    sem: Option<Arc<Semaphore>>,
    drop_cleaner: Arc<DropSignal>,
}

impl<F, C> DynPool<F, C>
where
    F: ConnectionFactory<C> + Send + Sync + Clone,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    pub fn new(factory: F, ttl: Duration, limit: Option<usize>) -> Self {
        let pool = TtlQueue::new(ttl);

        let (tx, rx) = oneshot::channel();
        Self::launch_cleaner(pool.clone(), rx, ttl);

        return DynPool {
            factory,
            pool,
            sem: limit.map(|limit| Arc::new(Semaphore::new(limit))),
            drop_cleaner: Arc::new(DropSignal::new(tx)),
        };
    }

    /// The cleaner is a service that wakes up on the ttl interval to pop the
    /// oldest connection (the one at the front of the queue), effectivley cleaning
    /// out any connection that are past their ttl
    fn launch_cleaner(dyn_pool: TtlQueue<C>, mut rx: oneshot::Receiver<()>, ttl: Duration) {
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

    pub async fn aquire(&self) -> Result<DynPoolCon<C>, RedisPoolError> {
        let permit = match &self.sem {
            Some(sem) => Some(sem.clone().acquire_owned().await?),
            None => None,
        };
        let con = self.aquire_connection().await?;
        Ok(DynPoolCon::new(con, self.pool.clone(), permit))
    }

    async fn aquire_connection(&self) -> RedisResult<C> {
        while let Some(mut con) = self.pool.pop() {
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

impl<F, C> Clone for DynPool<F, C>
where
    F: ConnectionFactory<C> + Send + Sync + Clone,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    fn clone(&self) -> Self {
        DynPool {
            factory: self.factory.clone(),
            pool: self.pool.clone(),
            sem: self.sem.clone(),
            drop_cleaner: self.drop_cleaner.clone(),
        }
    }
}

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

pub struct DynPoolCon<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    // This field can be safley unwrapped because it is always initialized to Some
    // and only set to None when dropped
    con: Option<C>,
    pool: TtlQueue<C>,
    permit: Option<OwnedSemaphorePermit>,
}

impl<C> DynPoolCon<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    pub fn new(con: C, pool: TtlQueue<C>, permit: Option<OwnedSemaphorePermit>) -> Self {
        DynPoolCon {
            con: Some(con),
            pool,
            permit,
        }
    }
}

impl<C> Drop for DynPoolCon<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    fn drop(&mut self) {
        let _ = self.pool.push(self.con.take().unwrap());
    }
}

impl<C> Deref for DynPoolCon<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.con.as_ref().unwrap()
    }
}

impl<C> DerefMut for DynPoolCon<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.con.as_mut().unwrap()
    }
}

impl<C> ConnectionLike for DynPoolCon<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        self.con.as_mut().unwrap().req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        self.con
            .as_mut()
            .unwrap()
            .req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.con.as_ref().unwrap().get_db()
    }
}

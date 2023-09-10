use std::ops::{Deref, DerefMut};

use redis::{aio::ConnectionLike, Cmd, RedisFuture, Value};
use tokio::sync::OwnedSemaphorePermit;

use crate::dyn_pool::DynPool;

pub struct RedisPoolConnection2<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    // This field can be safley unwrapped because it is always initialized to Some
    // and only set to None when dropped
    con: Option<C>,
    permit: Option<OwnedSemaphorePermit>,
    dyn_pool: DynPool<C>,
}

impl<C> RedisPoolConnection2<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    pub fn new(con: C, permit: Option<OwnedSemaphorePermit>, dyn_pool: DynPool<C>) -> Self {
        RedisPoolConnection2 {
            con: Some(con),
            permit,
            dyn_pool,
        }
    }
}

impl<C> Drop for RedisPoolConnection2<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    fn drop(&mut self) {
        let _ = self.dyn_pool.push(self.con.take().unwrap());
    }
}

impl<C> Deref for RedisPoolConnection2<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.con.as_ref().unwrap()
    }
}

impl<C> DerefMut for RedisPoolConnection2<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.con.as_mut().unwrap()
    }
}

impl<C> ConnectionLike for RedisPoolConnection2<C>
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

use thiserror::Error;
use tokio::{sync::AcquireError, task::JoinError};

#[derive(Error, Debug)]
pub enum RedisPoolError {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    AcquireError(#[from] AcquireError),
    #[error(transparent)]
    JoinError(#[from] JoinError),
}

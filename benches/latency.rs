use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use criterion::{
    criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup, Criterion,
};
use futures::future::join_all;
use redis_pool::RedisPool;
use testcontainers::clients::Cli;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::oneshot;
use tokio::time::{self, Duration};
use utils::{get_set_byte_array, TestRedis};

use crate::utils::bench::KEY_RANGE;

#[path = "../tests/utils/mod.rs"]
mod utils;

const DATA_SIZE: usize = 1_048_576;
const DATA: [u8; DATA_SIZE] = [1; DATA_SIZE];

const SAMPLES: usize = 100;
const MIN: usize = 1;
const MAX: usize = 8;

fn latency(c: &mut Criterion) {
    let docker = Cli::docker();
    let redis = TestRedis::new(&docker);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut g = c.benchmark_group("latency");
    g.sample_size(SAMPLES);

    for i in MIN..=MAX {
        let pool_size = 1 << i;
        for j in i..=MAX {
            let con_limit = 1 << j;
            latency_inner(&mut g, &rt, redis.client(), pool_size, Some(con_limit));
        }
        latency_inner(&mut g, &rt, redis.client(), pool_size, None);
    }

    g.finish();
}

fn latency_inner<M: Measurement>(
    g: &mut BenchmarkGroup<'_, M>,
    rt: &Runtime,
    client: redis::Client,
    pool_size: usize,
    con_limit: Option<usize>,
) {
    let pool = RedisPool::new(client, pool_size, con_limit);
    rt.block_on(pool.fill());

    let name = format!(
        "pool_{:0>4}_limit_{:0>4}",
        pool_size,
        con_limit
            .map(|i| i.to_string())
            .unwrap_or("none".to_owned())
    );

    g.bench_function(&name, |b| {
        let stop = Arc::new(AtomicBool::new(false));
        let load_pool = pool.clone();

        // keep 500 concurrent connections open
        let joins = (0..500).map(|i| {
            let load_pool_clone = load_pool.clone();
            let stop = stop.clone();
            rt.spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let mut con = load_pool_clone.aquire().await.unwrap();
                    let key = (i % KEY_RANGE).to_string();
                    let _ = get_set_byte_array(key.as_str(), &DATA, &mut con).await;
                }
            })
        });

        b.to_async(rt).iter(|| async {
            let mut con = pool.aquire().await.unwrap();
            let _ = redis::cmd("PING")
                .arg(0)
                .query_async::<_, ()>(&mut con)
                .await;
        });

        stop.store(true, Ordering::Relaxed);
        let _ = rt.block_on(async { join_all(joins).await });
    });

    rt.block_on(async {
        let _ = redis::cmd("FLUSHDB")
            .query_async::<_, ()>(&mut pool.aquire().await.unwrap())
            .await;
    })
}

criterion_group!(benches, latency);
criterion_main!(benches);

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use criterion::{
    criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup, Criterion,
};
use futures::future::join_all;
use redis_pool::factory::ConnectionFactory;
use redis_pool::{ClusterRedisPool, RedisPool};
use testcontainers::clients::Cli;
use tokio::runtime::Runtime;
use utils::bench::{bench_name, bench_pool_sizes_itr, bench_runtime, DATA_1MB, KEYS};
use utils::{get_set_byte_array, TestClusterRedis, TestRedis};

#[path = "../tests/utils/mod.rs"]
mod utils;

const SAMPLES: usize = 25;
const CONCURRENT_CONS: usize = 500;

fn latency_single(c: &mut Criterion) {
    let docker = Cli::docker();
    let redis = TestRedis::new(&docker);
    let rt = bench_runtime();

    let mut g = c.benchmark_group("latency_single");
    g.sample_size(SAMPLES);

    for (pool_size, con_limit) in bench_pool_sizes_itr() {
        let name = bench_name(pool_size, con_limit);
        let pool = RedisPool::new(redis.client(), pool_size, con_limit);
        rt.block_on(pool.fill());

        latency_inner(&mut g, &rt, name.as_str(), &pool);
    }

    g.finish();
}

fn latency_cluster(c: &mut Criterion) {
    let docker = Cli::docker();
    let redis = TestClusterRedis::new(&docker);
    let rt = bench_runtime();

    let mut g = c.benchmark_group("latency_cluster");
    g.sample_size(SAMPLES);

    for (pool_size, con_limit) in bench_pool_sizes_itr() {
        let name = bench_name(pool_size, con_limit);
        let pool = ClusterRedisPool::new(redis.client(), pool_size, con_limit);
        rt.block_on(pool.fill());

        latency_inner(&mut g, &rt, name.as_str(), &pool);
    }

    g.finish();
}

fn latency_proxy_cluster(c: &mut Criterion) {
    let docker = Cli::docker();
    let redis = TestRedis::new(&docker);
    let rt = bench_runtime();

    let mut g = c.benchmark_group("latency_proxy_cluster");
    g.sample_size(SAMPLES);

    for (pool_size, con_limit) in bench_pool_sizes_itr() {
        let name = bench_name(pool_size, con_limit);
        let pool = RedisPool::new(redis.client(), pool_size, con_limit);
        rt.block_on(pool.fill());

        latency_inner(&mut g, &rt, name.as_str(), &pool);
    }

    g.finish();
}

fn latency_inner<M: Measurement, F, C>(
    g: &mut BenchmarkGroup<'_, M>,
    rt: &Runtime,
    name: &str,
    pool: &RedisPool<F, C>,
) where
    F: ConnectionFactory<C> + Send + Sync + Clone + 'static,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    g.bench_function(name, |b| {
        let stop = Arc::new(AtomicBool::new(false));
        let load_pool = pool.clone();

        // keep 500 concurrent connections open
        let joins = (0..CONCURRENT_CONS).map(|i| {
            let load_pool_clone = load_pool.clone();
            let stop = stop.clone();
            rt.spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let mut con = load_pool_clone.aquire().await.unwrap();
                    let key = &KEYS[i % KEYS.len()];
                    let _ = get_set_byte_array(key, &DATA_1MB, &mut con).await;
                }
            })
        });

        b.to_async(rt).iter(|| async {
            let mut con = pool.clone().aquire().await.unwrap();
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

criterion_group!(
    benches,
    latency_single,
    latency_cluster,
    latency_proxy_cluster
);
criterion_main!(benches);

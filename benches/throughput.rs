#[path = "../tests/utils/mod.rs"]
mod utils;

use criterion::{
    black_box, criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup, Criterion,
};
use futures::future::join_all;
use redis_pool::{factory::ConnectionFactory, ClusterRedisPool, RedisPool};
use testcontainers::clients::Cli;
use tokio::runtime::Runtime;
use utils::{
    bench::{bench_name, bench_pool_sizes_itr, bench_runtime, DATA_1MB, KEYS},
    get_set_byte_array, TestClusterRedis, TestRedis,
};

const SAMPLES: usize = 25;
const CONCURRENT_CONS: usize = 500;

fn throughput_single(c: &mut Criterion) {
    let docker = Cli::docker();
    let redis = TestRedis::new(&docker);
    let rt = bench_runtime();

    let mut g = c.benchmark_group("throughput_single");
    g.sample_size(SAMPLES);

    for (pool_size, con_limit) in bench_pool_sizes_itr() {
        let name = bench_name(pool_size, con_limit);
        let pool = RedisPool::new(redis.client(), pool_size, con_limit);
        rt.block_on(pool.fill());

        throughput_inner(&mut g, &rt, name.as_str(), &pool);
    }

    g.finish();
}

fn throughput_cluster(c: &mut Criterion) {
    let docker = Cli::docker();
    let redis = TestClusterRedis::new(&docker);
    let rt = bench_runtime();

    let mut g = c.benchmark_group("throughput_cluster");
    g.sample_size(SAMPLES);

    for (pool_size, con_limit) in bench_pool_sizes_itr() {
        let name = bench_name(pool_size, con_limit);
        let pool = ClusterRedisPool::new(redis.client(), pool_size, con_limit);
        rt.block_on(pool.fill());

        throughput_inner(&mut g, &rt, name.as_str(), &pool);
    }

    g.finish();
}

fn throughput_proxy_cluster(c: &mut Criterion) {
    let docker = Cli::docker();
    let redis = TestRedis::new(&docker);
    let rt = bench_runtime();

    let mut g = c.benchmark_group("throughput_proxy_cluster");
    g.sample_size(SAMPLES);

    for (pool_size, con_limit) in bench_pool_sizes_itr() {
        let name = bench_name(pool_size, con_limit);
        let pool = RedisPool::new(redis.client(), pool_size, con_limit);
        rt.block_on(pool.fill());

        throughput_inner(&mut g, &rt, name.as_str(), &pool);
    }

    g.finish();
}

fn throughput_inner<M: Measurement, F, C>(
    g: &mut BenchmarkGroup<'_, M>,
    rt: &Runtime,
    name: &str,
    pool: &RedisPool<F, C>,
) where
    F: ConnectionFactory<C> + Send + Sync + Clone + 'static,
    C: redis::aio::ConnectionLike + Send + 'static,
{
    g.bench_function(name, |b| {
        b.to_async(rt).iter(|| {
            join_all((0..black_box(CONCURRENT_CONS)).map(|i| {
                let pool = pool.clone();
                tokio::spawn(async move {
                    let mut con = pool.aquire().await.unwrap();
                    let key = &KEYS[i % KEYS.len()];
                    get_set_byte_array(key, &DATA_1MB, &mut con).await
                })
            }))
        })
    });

    rt.block_on(async {
        let _ = redis::cmd("FLUSHDB")
            .query_async::<_, ()>(&mut pool.aquire().await.unwrap())
            .await;
    })
}

criterion_group!(
    benches,
    throughput_single,
    throughput_cluster,
    throughput_proxy_cluster
);
criterion_main!(benches);

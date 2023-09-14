use criterion::{
    black_box, criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup, Criterion,
};
use futures::future::join_all;
use redis_pool::RedisPool;
use testcontainers::clients::Cli;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::oneshot;
use tokio::time::{self, Duration};
use utils::{get_set_byte_array, TestRedis};
use uuid::Uuid;

#[path = "../tests/utils/mod.rs"]
mod utils;

const DATA_SIZE: usize = 1_048_576;
const DATA: [u8; DATA_SIZE] = [1; DATA_SIZE];

fn parallel_latency(c: &mut Criterion) {
    let docker = Cli::docker();
    let redis = TestRedis::new(&docker);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();

    let mut g = c.benchmark_group("Parallel Throughput");

    for i in 4..10 {
        let pool_size = 2_usize.pow(i);
        for j in i..10 {
            let con_limit = 2_usize.pow(j);
            parallel_latency_inner(&mut g, &rt, redis.client(), pool_size, Some(con_limit));
        }
        parallel_latency_inner(&mut g, &rt, redis.client(), pool_size, None);
    }
}

fn parallel_latency_inner<M: Measurement>(
    g: &mut BenchmarkGroup<'_, M>,
    rt: &Runtime,
    client: redis::Client,
    pool_size: usize,
    con_limit: Option<usize>,
) {
    let pool = RedisPool::new(client, pool_size, con_limit);

    let name = format!(
        "(pool: {}, limit: {})",
        pool_size,
        con_limit
            .map(|i| i.to_string())
            .unwrap_or("none".to_owned())
    );

    g.bench_function(&name, |b| {
        let (tx, mut rx) = oneshot::channel::<()>();

        let load_pool = pool.clone();
        let join = rt.spawn(async move {
            let mut interval = time::interval(Duration::from_millis(1));
            loop {
                select! {
                    _ = interval.tick() => {
                        let load_pool_clone = load_pool.clone();
                        tokio::spawn(async move {
                            let mut con = load_pool_clone.aquire().await.unwrap();
                            get_set_byte_array(&Uuid::new_v4().to_string(), &DATA, &mut con).await
                        });
                    }
                    _ = &mut rx => {
                        return;
                    }
                }
            }
        });

        b.to_async(rt).iter(|| {
            join_all((0..black_box(1000)).map(|i| {
                let i = i.to_string();
                let pool = pool.clone();
                tokio::spawn(async move {
                    let mut con = pool.aquire().await.unwrap();
                    get_set_byte_array(&i, &DATA, &mut con).await
                })
            }))
        });

        tx.send(()).unwrap();
        rt.block_on(async { join.await }).unwrap();
    });

    rt.block_on(async {
        let _ = redis::cmd("FLUSHDB")
            .query_async::<_, ()>(&mut pool.aquire().await.unwrap())
            .await;
    })
}

criterion_group!(benches, parallel_latency);
criterion_main!(benches);

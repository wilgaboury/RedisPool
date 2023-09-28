use criterion::{
    criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup, Criterion,
};
use redis_pool::RedisPool;
use testcontainers::clients::Cli;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::oneshot;
use tokio::time::{self, Duration};
use utils::{get_set_byte_array, TestRedis};

#[path = "../tests/utils/mod.rs"]
mod utils;

const DATA_SIZE: usize = 1_048_576;
const DATA: [u8; DATA_SIZE] = [1; DATA_SIZE];

const SAMPLES: usize = 100;
const MIN: usize = 4;
const MAX: usize = 10;

fn parallel_latency(c: &mut Criterion) {
    let docker = Cli::docker();
    let redis = TestRedis::new(&docker);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut g = c.benchmark_group("parallel_latency");
    g.sample_size(SAMPLES);

    for i in MIN..=MAX {
        let pool_size = 1 << i;
        for j in i..=MAX {
            let con_limit = 1 << j;
            parallel_latency_inner(&mut g, &rt, redis.client(), pool_size, Some(con_limit));
        }
        parallel_latency_inner(&mut g, &rt, redis.client(), pool_size, None);
    }

    g.finish();
}

fn parallel_latency_inner<M: Measurement>(
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
        let (tx, mut rx) = oneshot::channel::<()>();

        let load_pool = pool.clone();

        // perform get/set on redis at rate of 500 requests per second
        let join = rt.spawn(async move {
            let mut interval = time::interval(Duration::from_millis(2));
            loop {
                select! {
                    _ = interval.tick() => {
                        let load_pool_clone = load_pool.clone();
                        tokio::spawn(async move {
                            let mut con = load_pool_clone.aquire().await.unwrap();
                            get_set_byte_array("0", &DATA, &mut con).await
                        });
                    }
                    _ = &mut rx => {
                        return;
                    }
                }
            }
        });

        b.to_async(rt).iter(|| async {
            let mut con = pool.aquire().await.unwrap();
            get_set_byte_array("1", &DATA, &mut con).await
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

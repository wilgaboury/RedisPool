pub fn bench_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

pub const MIN: usize = 1;
pub const MAX: usize = 8;
pub const KEY_RANGE: usize = 100;

pub const DATA_SIZE_1MB: usize = 1_048_576;
pub const DATA_1MB: [u8; DATA_SIZE_1MB] = [1; DATA_SIZE_1MB];

pub fn bench_pool_sizes_itr() -> impl Iterator<Item = (usize, Option<usize>)> {
    (MIN..=MAX).flat_map(|i| {
        (i..=MAX)
            .map(|j| Some(1 << j))
            .chain(std::iter::once(None))
            .map(move |j| (1 << i, j))
    })
}

pub fn bench_name(pool_size: usize, con_limit: Option<usize>) -> String {
    format!(
        "pool_{:0>4}_limit_{:0>4}",
        pool_size,
        con_limit
            .map(|i| i.to_string())
            .unwrap_or("none".to_owned())
    )
}

use crate::scenarios::BenchStats;
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redis::AsyncCommands;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let mut stats = BenchStats::new();
    let mut rng = StdRng::seed_from_u64(id as u64);
    let size_10kb = 10 * 1024;
    let size_100kb = 100 * 1024;
    let payload_10kb: String = (0..size_10kb)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect();
    let payload_100kb: String = (0..size_100kb)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect();

    for i in 0..requests {
        let key = format!("bench_large:{}:{}", id, i);
        let val = if i % 10 == 0 {
            &payload_100kb
        } else {
            &payload_10kb
        };
        let _: () = con.set(&key, val).await?;
        let _: String = con.get(&key).await?;
    }
    stats.ops = requests * 2;
    Ok(stats)
}

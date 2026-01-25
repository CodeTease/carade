use redis::AsyncCommands;
use std::time::Instant;
use crate::scenarios::BenchStats;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let mut stats = BenchStats::new();

    for i in 0..requests {
        let key = format!("bench:{}:{}", id, i);
        let val = format!("val_{}", i);

        // SET
        let start = Instant::now();
        let _: () = con.set(&key, &val).await?;
        stats.histogram.record(start.elapsed().as_micros() as u64)?;

        // GET
        let start = Instant::now();
        let _: String = con.get(&key).await?;
        stats.histogram.record(start.elapsed().as_micros() as u64)?;

        // successes += 1 (implied)
    }
    stats.ops = requests * 2;
    Ok(stats)
}

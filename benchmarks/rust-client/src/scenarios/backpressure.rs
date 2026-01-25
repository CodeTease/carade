use crate::scenarios::BenchStats;
use std::time::Instant;

pub async fn run(client: redis::Client, _id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let batch_size = 5000;
    let mut executed = 0;
    let mut stats = BenchStats::new();

    while executed < requests {
        let current_batch = std::cmp::min(batch_size, requests - executed);
        let mut pipe = redis::pipe();
        
        for _ in 0..current_batch {
            pipe.set("bp_key", "val").ignore();
        }
        
        let start = Instant::now();
        let _: () = pipe.query_async(&mut con).await?;
        stats.histogram.record(start.elapsed().as_micros() as u64)?;
        executed += current_batch;
    }

    stats.ops = executed;
    Ok(stats)
}

use redis::AsyncCommands;
use rand_distr::{Zipf, Distribution};
use crate::scenarios::BenchStats;
use std::time::Instant;
use rand::SeedableRng;
use rand::rngs::StdRng;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let zipf = Zipf::new(1000.0, 1.01).unwrap();
    let mut rng = StdRng::seed_from_u64(id as u64); 
    let mut stats = BenchStats::new();

    for _ in 0..requests {
        let k = zipf.sample(&mut rng) as u64;
        let key = format!("skew_key:{}", k);
        
        let start = Instant::now();
        let _: () = con.set(&key, "val").await?;
        stats.histogram.record(start.elapsed().as_micros() as u64)?;
        
        let start = Instant::now();
        let _: String = con.get(&key).await?;
        stats.histogram.record(start.elapsed().as_micros() as u64)?;
    }
    stats.ops = requests * 2;
    Ok(stats)
}

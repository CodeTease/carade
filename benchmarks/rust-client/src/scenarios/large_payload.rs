use redis::AsyncCommands;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rand::distr::Alphanumeric;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<usize> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let mut successes = 0;
    
    // Create random payloads
    // We don't want to generate a new string every time to save client CPU, 
    // but we should vary it a bit. Let's create a few variants.
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
        // 10% chance of large payload
        let val = if i % 10 == 0 { &payload_100kb } else { &payload_10kb };

        // SET
        let _: () = con.set(&key, val).await?;

        // GET
        let _: String = con.get(&key).await?;

        successes += 1;
    }
    Ok(successes)
}

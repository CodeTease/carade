use crate::scenarios::BenchStats;
use redis::Script;
use std::time::Instant;

pub async fn run(client: redis::Client, _id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let script = Script::new("return redis.call('set', KEYS[1], ARGV[1])");
    let mut stats = BenchStats::new();

    for i in 0..requests {
        let key = format!("lua_key:{}", i % 100);
        let start = Instant::now();
        let _: () = script.key(&key).arg(i).invoke_async(&mut con).await?;
        stats.histogram.record(start.elapsed().as_micros() as u64)?;
    }
    stats.ops = requests;
    Ok(stats)
}

use crate::scenarios::BenchStats;

pub async fn run(client: redis::Client, _id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let mut stats = BenchStats::new();
    for _ in 0..requests {
        let mut con = client.get_multiplexed_async_connection().await?;
        let _: String = redis::cmd("PING").query_async(&mut con).await?;
    }
    stats.ops = requests;
    Ok(stats)
}

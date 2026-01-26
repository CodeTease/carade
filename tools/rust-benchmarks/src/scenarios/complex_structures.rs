use crate::scenarios::BenchStats;
use redis::AsyncCommands;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let key_zset = format!("bench_zset:{}", id);
    let key_list = format!("bench_list:{}", id);
    let mut stats = BenchStats::new();

    for i in 0..requests {
        let score = i as f64;
        let member = format!("user_{}", i);
        let _: () = con.zadd(&key_zset, &member, score).await?;
        if i % 100 == 0 {
            let _: Vec<String> = con.zrange(&key_zset, 0, 50).await?;
        }
        let val = format!("msg_{}", i);
        let _: () = con.lpush(&key_list, &val).await?;
        let _: Option<String> = con.rpop(&key_list, None).await?;
    }
    stats.ops = requests * 3; // Approx
    Ok(stats)
}

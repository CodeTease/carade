use crate::scenarios::BenchStats;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let mut stats = BenchStats::new();
    let key = format!("bench_bloom:{}", id);
    let td_key = format!("bench_td:{}", id);

    for i in 0..requests {
        let item = format!("item_{}", i);
        let _: i64 = redis::cmd("BF.ADD")
            .arg(&key)
            .arg(&item)
            .query_async(&mut con)
            .await?;
        let _: i64 = redis::cmd("BF.EXISTS")
            .arg(&key)
            .arg(&item)
            .query_async(&mut con)
            .await?;
        if i % 100 == 0 {
            let val = i as f64;
            let _: () = redis::cmd("TD.ADD")
                .arg(&td_key)
                .arg(val)
                .query_async(&mut con)
                .await?;
            let _: f64 = redis::cmd("TD.QUANTILE")
                .arg(&td_key)
                .arg(0.5)
                .query_async(&mut con)
                .await?;
        }
    }
    stats.ops = requests * 2;
    Ok(stats)
}

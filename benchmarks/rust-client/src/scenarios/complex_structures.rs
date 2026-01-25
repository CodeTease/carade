use redis::AsyncCommands;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<usize> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let key_zset = format!("bench_zset:{}", id);
    let key_list = format!("bench_list:{}", id);
    let mut successes = 0;

    for i in 0..requests {
        // ZSET Operations
        let score = i as f64;
        let member = format!("user_{}", i);

        // ZADD (O(log N))
        let _: () = con.zadd(&key_zset, &member, score).await?;

        // ZRANGE (O(log N + M)) - Occasional heavy read
        if i % 100 == 0 {
            let _: Vec<String> = con.zrange(&key_zset, 0, 50).await?;
        }

        // LIST Operations (Queue)
        let val = format!("msg_{}", i);
        // LPUSH
        let _: () = con.lpush(&key_list, &val).await?;
        
        // RPOP
        // We use Option<String> because list might be empty if LPUSH failed silently (unlikely) or parallel consumption
        let _: Option<String> = con.rpop(&key_list, None).await?;

        successes += 1;
    }
    Ok(successes)
}

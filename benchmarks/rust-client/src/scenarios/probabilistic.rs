pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<usize> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let mut successes = 0;
    
    let key = format!("bench_bloom:{}", id);
    let td_key = format!("bench_td:{}", id);
    
    for i in 0..requests {
        let item = format!("item_{}", i);
        
        // BF.ADD
        // We ignore errors in case Bloom module is not loaded (though Carade has it)
        // But for benchmark we expect success.
        let _: i64 = redis::cmd("BF.ADD").arg(&key).arg(&item).query_async(&mut con).await?;
        
        // BF.EXISTS
        let _: i64 = redis::cmd("BF.EXISTS").arg(&key).arg(&item).query_async(&mut con).await?;
        
        // Occasional T-Digest
        if i % 100 == 0 {
             let val = i as f64;
             // TD.ADD key val
             let _: () = redis::cmd("TD.ADD").arg(&td_key).arg(val).query_async(&mut con).await?;
             // TD.QUANTILE key 0.5
             let _: f64 = redis::cmd("TD.QUANTILE").arg(&td_key).arg(0.5).query_async(&mut con).await?;
        }
        
        successes += 1;
    }
    
    Ok(successes)
}

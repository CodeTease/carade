use crate::scenarios::BenchStats;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let mut con = client.get_multiplexed_async_connection().await?;
    let mut stats = BenchStats::new();
    let batch_size = 100;
    let batches = requests / batch_size;
    
    for b in 0..batches {
        let mut pipe = redis::pipe();
        for i in 0..batch_size {
            let global_idx = b * batch_size + i;
            let key = format!("bench_pipe:{}:{}", id, global_idx);
            let val = "v";
            pipe.set(&key, val).ignore();
        }
        let _: () = pipe.query_async(&mut con).await?;
    }
    
    let remaining = requests % batch_size;
    if remaining > 0 {
         let mut pipe = redis::pipe();
         for i in 0..remaining {
            let global_idx = batches * batch_size + i;
            let key = format!("bench_pipe:{}:{}", id, global_idx);
            let val = "v";
            pipe.set(&key, val).ignore();
         }
         let _: () = pipe.query_async(&mut con).await?;
    }
    
    stats.ops = requests;
    Ok(stats)
}

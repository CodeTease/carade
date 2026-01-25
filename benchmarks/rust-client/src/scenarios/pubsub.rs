use redis::AsyncCommands;
use futures_util::StreamExt;
use crate::scenarios::BenchStats;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<BenchStats> {
    let channel = "bench_pubsub";
    let mut stats = BenchStats::new();

    if id == 0 {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        let mut con = client.get_multiplexed_async_connection().await?;
        for i in 0..requests {
            let msg = format!("msg_{}", i);
            let _: usize = con.publish(channel, &msg).await?;
        }
        stats.ops = requests;
    } else {
        let mut pubsub = client.get_async_pubsub().await?;
        pubsub.subscribe(channel).await?;
        let mut stream = pubsub.on_message();
        let mut received = 0;
        loop {
             let msg_option = tokio::time::timeout(tokio::time::Duration::from_secs(3), stream.next()).await;
             match msg_option {
                 Ok(Some(_)) => {
                     received += 1;
                     if received >= requests { break; }
                 }
                 Ok(None) => break,
                 Err(_) => break,
             }
        }
        stats.ops = received;
    }
    Ok(stats)
}

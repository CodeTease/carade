use redis::AsyncCommands;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<usize> {
    // Establish a new connection for this task to simulate a distinct client
    let mut con = client.get_multiplexed_async_connection().await?;
    let mut successes = 0;

    for i in 0..requests {
        let key = format!("bench:{}:{}", id, i);
        let val = format!("val_{}", i);

        // SET
        let _: () = con.set(&key, &val).await?;

        // GET
        let _: String = con.get(&key).await?;

        successes += 1;
    }
    Ok(successes)
}

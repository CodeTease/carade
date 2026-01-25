
pub async fn run(client: redis::Client, _id: usize, requests: usize) -> anyhow::Result<usize> {
    let mut successes = 0;
    
    for _ in 0..requests {
        // Establish a new connection
        // This simulates a new client connecting, authenticating, and running a command
        let mut con = client.get_multiplexed_async_connection().await?;
        
        // PING
        let _: String = redis::cmd("PING").query_async(&mut con).await?;
        
        // Connection drops when 'con' goes out of scope
        successes += 1;
    }
    
    Ok(successes)
}

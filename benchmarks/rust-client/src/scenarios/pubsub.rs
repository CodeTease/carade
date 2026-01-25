use redis::AsyncCommands;
use futures_util::StreamExt;

pub async fn run(client: redis::Client, id: usize, requests: usize) -> anyhow::Result<usize> {
    let channel = "bench_pubsub";
    
    if id == 0 {
        // Publisher
        // Wait a bit for subscribers to connect
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        
        let mut con = client.get_multiplexed_async_connection().await?;
        
        for i in 0..requests {
            let msg = format!("msg_{}", i);
            let _: usize = con.publish(channel, &msg).await?;
        }
        Ok(requests)
    } else {
        // Subscriber
        let mut pubsub = client.get_async_pubsub().await?;
        pubsub.subscribe(channel).await?;
        let mut stream = pubsub.on_message();
        
        let mut received = 0;
        
        loop {
             // Timeout if no message received for 3 seconds
             let msg_option = tokio::time::timeout(
                 tokio::time::Duration::from_secs(3), 
                 stream.next()
             ).await;
             
             match msg_option {
                 Ok(Some(_)) => {
                     received += 1;
                     // If we reached expected count, we can stop?
                     // But publisher might send more if we are sharing channel?
                     // Here key is hardcoded.
                     if received >= requests {
                         break;
                     }
                 }
                 Ok(None) => break, // Stream closed
                 Err(_) => {
                     // Timeout - publisher probably finished and we missed some, or done
                     break;
                 }
             }
        }
        
        Ok(received)
    }
}

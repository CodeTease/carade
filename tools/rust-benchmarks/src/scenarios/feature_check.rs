use redis::AsyncCommands;
use std::time::Duration;
use tokio::time::sleep;

pub async fn run(client: &redis::Client) -> anyhow::Result<bool> {
    println!("🧪 Verifying system features (TTL & Expiry)...");

    let mut con = client.get_multiplexed_async_connection().await?;

    // 1. Basic Check
    let _: () = con.set("test_key", "benchmark_val").await?;
    let val: Option<String> = con.get("test_key").await?;

    if val.as_deref() != Some("benchmark_val") {
        println!("❌ Basic GET failed. Got: {:?}", val);
        return Ok(false);
    }

    // 2. TTL Test
    let _: () = con.expire("test_key", 1).await?;
    let ttl: i64 = con.ttl("test_key").await?;
    println!("   Initial TTL: {}s", ttl);

    println!("   Waiting for expiry (1.5s)...");
    sleep(Duration::from_millis(1500)).await;

    let expired_val: Option<String> = con.get("test_key").await?;
    if expired_val.is_some() {
        println!("❌ Expiry failed: Key still exists -> {:?}", expired_val);
        return Ok(false);
    }

    println!("✅ Feature tests passed.");
    Ok(true)
}

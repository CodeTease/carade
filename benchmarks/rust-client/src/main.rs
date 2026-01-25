use clap::{Parser, ValueEnum};
use std::time::Instant;
use tokio::task;

mod scenarios;

#[derive(Debug, Clone, ValueEnum)]
enum Scenario {
    Basic,
    Complex,
    LargePayload,
    Pipeline,
    ConnectionChurn,
    PubSub,
    Probabilistic,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = 63790)]
    port: u16,

    #[arg(long, env = "CARADE_PASSWORD", default_value = "teasertopsecret")]
    password: String,

    #[arg(long, default_value_t = 50)]
    clients: usize,

    #[arg(long, default_value_t = 1000)]
    requests: usize,

    #[arg(long, value_enum, default_value_t = Scenario::Basic)]
    scenario: Scenario,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("\n🏋️  CARADE BENCHMARK (RUST)");
    println!("Target: {}:{}", args.host, args.port);
    println!("Scenario: {:?}", args.scenario);

    // Construct connection string: redis://:password@host:port/
    let conn_str = format!("redis://:{}@{}:{}/", args.password, args.host, args.port);
    let client = redis::Client::open(conn_str)?;

    // Feature Check
    if !scenarios::feature_check::run(&client).await? {
        println!("❌ Feature check failed. Aborting.");
        return Ok(());
    }

    println!("\n🚀 Starting stress test with {} clients, {} requests each...", args.clients, args.requests);
    let start_time = Instant::now();
    let mut tasks = Vec::with_capacity(args.clients);

    for i in 0..args.clients {
        let client_clone = client.clone();
        let requests = args.requests;
        let scenario = args.scenario.clone();
        
        tasks.push(task::spawn(async move {
            match scenario {
                Scenario::Basic => scenarios::basic_stress::run(client_clone, i, requests).await,
                Scenario::Complex => scenarios::complex_structures::run(client_clone, i, requests).await,
                Scenario::LargePayload => scenarios::large_payload::run(client_clone, i, requests).await,
                Scenario::Pipeline => scenarios::pipeline::run(client_clone, i, requests).await,
                Scenario::ConnectionChurn => scenarios::connection_churn::run(client_clone, i, requests).await,
                Scenario::PubSub => scenarios::pubsub::run(client_clone, i, requests).await,
                Scenario::Probabilistic => scenarios::probabilistic::run(client_clone, i, requests).await,
            }
        }));
    }

    let mut total_successes = 0;
    for t in tasks {
        match t.await {
            Ok(Ok(successes)) => total_successes += successes,
            Ok(Err(e)) => eprintln!("Task failed: {}", e),
            Err(e) => eprintln!("Join error: {}", e),
        }
    }

    let duration = start_time.elapsed();
    let duration_secs = duration.as_secs_f64();
    
    // Calculate approximate ops based on scenario
    let total_ops = match args.scenario {
        Scenario::Basic => total_successes * 2, // SET + GET
        Scenario::Complex => total_successes * 3, // ZADD + LPUSH + RPOP
        Scenario::LargePayload => total_successes * 2, // SET + GET
        Scenario::Pipeline => total_successes, // Total commands executed
        Scenario::ConnectionChurn => total_successes, // PING
        Scenario::PubSub => total_successes, // Messages published/received
        Scenario::Probabilistic => total_successes * 2, // BF.ADD + BF.EXISTS
    };

    println!("\n==============================");
    println!("🔥 FINAL RESULTS ({:?})", args.scenario);
    println!("✅ Total Ops (approx):    {}", total_ops);
    println!("⏱️  Duration:     {:.2}s", duration_secs);
    if duration_secs > 0.0 {
        println!("🚀 Throughput:   {:.0} ops/sec", total_ops as f64 / duration_secs);
    } else {
        println!("🚀 Throughput:   N/A ops/sec");
    }
    println!("==============================\n");

    Ok(())
}

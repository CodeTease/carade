use clap::{Parser, ValueEnum};
use std::time::Instant;
use tokio::task;
use scenarios::BenchStats;

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
    LuaStress,
    WorkloadSkew,
    Backpressure,
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
                Scenario::LuaStress => scenarios::lua_stress::run(client_clone, i, requests).await,
                Scenario::WorkloadSkew => scenarios::workload_skew::run(client_clone, i, requests).await,
                Scenario::Backpressure => scenarios::backpressure::run(client_clone, i, requests).await,
            }
        }));
    }

    let mut total_stats = BenchStats::new();

    for t in tasks {
        match t.await {
            Ok(Ok(stats)) => total_stats.merge(&stats),
            Ok(Err(e)) => eprintln!("Task failed: {}", e),
            Err(e) => eprintln!("Join error: {}", e),
        }
    }

    let duration = start_time.elapsed();
    let duration_secs = duration.as_secs_f64();
    let total_ops = total_stats.ops;

    println!("\n==============================");
    println!("🔥 FINAL RESULTS ({:?})", args.scenario);
    println!("✅ Total Ops (approx):    {}", total_ops);
    println!("⏱️  Duration:     {:.2}s", duration_secs);
    if duration_secs > 0.0 {
        println!("🚀 Throughput:   {:.0} ops/sec", total_ops as f64 / duration_secs);
    } else {
        println!("🚀 Throughput:   N/A ops/sec");
    }

    if !total_stats.histogram.is_empty() {
        println!("\n📊 Latency Distribution (microseconds):");
        println!("   p50:   {}", total_stats.histogram.value_at_quantile(0.5));
        println!("   p90:   {}", total_stats.histogram.value_at_quantile(0.9));
        println!("   p99:   {}", total_stats.histogram.value_at_quantile(0.99));
        println!("   p99.9: {}", total_stats.histogram.value_at_quantile(0.999));
        println!("   Max:   {}", total_stats.histogram.max());
    }

    println!("==============================\n");

    Ok(())
}

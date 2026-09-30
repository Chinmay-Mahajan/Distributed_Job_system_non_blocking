use std::net::TcpListener;
use std::process::Command;

use distributed_jobv2::config::{Config, SandboxType};
use distributed_jobv2::server::BrokerServer;

fn check_docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn main() -> std::io::Result<()> {
    let mut config = Config::from_env();

    // Parse command line arguments with support for flags and positional values
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--local" => config.sandbox_type = SandboxType::Local,
            "--mock" => config.sandbox_type = SandboxType::Mock,
            "--docker" => config.sandbox_type = SandboxType::Docker,
            "--workers" | "-w" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse::<usize>() {
                        config.worker_count = val;
                    }
                    i += 1;
                }
            }
            "--client-cap" | "-c" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse::<usize>() {
                        config.max_in_flight_per_client = val;
                    }
                    i += 1;
                }
            }
            "--pool-size" | "-p" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse::<usize>() {
                        config.prewarmed_pool_size = val;
                    }
                    i += 1;
                }
            }
            "--shards" | "-s" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse::<usize>() {
                        config.store_shards = val;
                    }
                    i += 1;
                }
            }
            "--bind" | "-b" => {
                if i + 1 < args.len() {
                    config.bind_address = args[i + 1].clone();
                    i += 1;
                }
            }
            "--ttl" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse::<u64>() {
                        config.result_ttl = std::time::Duration::from_secs(val);
                    }
                    i += 1;
                }
            }
            "--help" | "-h" => {
                println!("Sandboxed Job Execution Broker (v2)");
                println!("Usage: cargo run -- [OPTIONS]");
                println!();
                println!("Options:");
                println!("  --local                  Run using Local Process sandbox");
                println!("  --docker                 Run using Docker sandbox (default)");
                println!("  --mock                   Run using Mock sandbox");
                println!("  --workers, -w <N>        Number of worker threads (default: 4)");
                println!("  --client-cap, -c <N>     Max in-flight jobs per client (default: 2)");
                println!("  --pool-size, -p <N>      Prewarmed Docker pool size (default: 4)");
                println!("  --shards, -s <N>         ResultStore lock striping shard count (default: 32)");
                println!("  --bind, -b <ADDR>        TCP bind address (default: 127.0.0.1:7000)");
                println!("  --ttl <SECS>             ResultStore TTL in seconds (default: 600)");
                println!("  --help, -h               Show this help message");
                println!();
                println!("Environment variables (can be set inline before 'cargo run'):");
                println!("  DISTJOB_WORKERS          e.g. DISTJOB_WORKERS=16");
                println!("  DISTJOB_CLIENT_CAP       e.g. DISTJOB_CLIENT_CAP=4");
                println!("  DISTJOB_PREWARMED_POOL_SIZE e.g. DISTJOB_PREWARMED_POOL_SIZE=8");
                println!("  DISTJOB_STORE_SHARDS     e.g. DISTJOB_STORE_SHARDS=32");
                println!("  DISTJOB_SANDBOX          e.g. DISTJOB_SANDBOX=local");
                println!("  DISTJOB_BIND_ADDR        e.g. DISTJOB_BIND_ADDR=127.0.0.1:7000");
                println!("  DISTJOB_TTL_SECS         e.g. DISTJOB_TTL_SECS=600");
                return Ok(());
            }
            addr if !addr.starts_with('-') => {
                config.bind_address = addr.to_string();
            }
            unknown => {
                eprintln!("[WARN] Unknown CLI argument: {unknown}");
            }
        }
        i += 1;
    }

    if config.sandbox_type == SandboxType::Docker && !check_docker_available() {
        eprintln!(
            "[WARN] Docker daemon appears unreachable. Starting with Docker sandbox will cause jobs to fail if Docker is not started."
        );
        eprintln!(
            "[INFO] To use local process sandbox for testing/development, run with --local or set DISTJOB_SANDBOX=local."
        );
    }

    println!("============================================================");
    println!("          SANDBOXED JOB EXECUTION BROKER (v2)               ");
    println!("============================================================");
    println!("  Bind Address:        {}", config.bind_address);
    println!("  Worker Threads:      {}", config.worker_count);
    println!("  Per-Client Cap:      {}", config.max_in_flight_per_client);
    println!("  Sandbox Type:        {:?}", config.sandbox_type);
    println!("  Prewarmed Pool:      {} (enabled: {})", config.prewarmed_pool_size, config.enable_prewarmed_pool);
    println!("  ResultStore Shards:  {}", config.store_shards);
    println!("  Result TTL:          {:?}", config.result_ttl);
    println!("============================================================");

    let bind_addr = config.bind_address.clone();
    let listener = TcpListener::bind(&bind_addr)?;
    let server = std::sync::Arc::new(BrokerServer::new(config));

    // Register Ctrl+C handler for graceful shutdown
    let server_shutdown = std::sync::Arc::clone(&server);
    ctrlc::set_handler(move || {
        println!("\n[INFO] Received shutdown signal (Ctrl+C). Cleaning up standby containers...");
        server_shutdown.shutdown();
        println!("[INFO] Shutdown complete. All containers cleaned up.");
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");

    server.run_listener(listener)
}

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

    // Check arguments for quick overrides
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        match args[1].as_str() {
            "--local" => config.sandbox_type = SandboxType::Local,
            "--mock" => config.sandbox_type = SandboxType::Mock,
            "--docker" => config.sandbox_type = SandboxType::Docker,
            "--help" | "-h" => {
                println!("Sandboxed Job Execution Broker (v2)");
                println!("Usage: distributed_jobv2 [--docker | --local | --mock] [bind_addr]");
                println!();
                println!("Environment variables:");
                println!("  DISTJOB_BIND_ADDR       TCP bind address (default: 127.0.0.1:7000)");
                println!("  DISTJOB_WORKERS         Worker thread count (default: 4)");
                println!("  DISTJOB_CLIENT_CAP      Max in-flight jobs per client (default: 2)");
                println!("  DISTJOB_SANDBOX         Sandbox backend: 'docker', 'local', or 'mock'");
                println!("  DISTJOB_DOCKER_RUNTIME  Docker runtime flag, e.g. 'runsc' for gVisor");
                println!("  DISTJOB_TTL_SECS        Result store TTL in seconds (default: 600)");
                return Ok(());
            }
            addr => config.bind_address = addr.to_string(),
        }
    }

    if args.len() > 2 {
        config.bind_address = args[2].clone();
    }

    if config.sandbox_type == SandboxType::Docker && !check_docker_available() {
        eprintln!(
            "[WARN] Docker daemon appears unreachable. Starting with Docker sandbox will cause jobs to fail if Docker is not started."
        );
        eprintln!(
            "[INFO] To use local process sandbox for testing/development, run with --local or set DISTJOB_SANDBOX=local."
        );
    }

    let bind_addr = config.bind_address.clone();
    let listener = TcpListener::bind(&bind_addr)?;
    let server = BrokerServer::new(config);

    server.run_listener(listener)
}

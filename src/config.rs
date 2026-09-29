use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxType {
    /// Full Docker sandbox with gVisor / limits as specified in plan.md
    Docker,
    /// Local process sandbox (useful for local development or when Docker is inactive)
    Local, // runs code directly on my machine (with timeouts but no virtualisation.. )
    /// Mock sandbox for deterministic testing , returns fake results for the sake of testing 
    Mock,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_address: String,
    pub worker_count: usize,
    pub max_in_flight_per_client: usize,
    pub default_timeout_secs: u64,
    pub max_timeout_secs: u64,
    pub result_ttl: Duration,
    pub cleanup_interval: Duration,
    pub sandbox_type: SandboxType,
    pub docker_runtime: Option<String>,
    pub docker_memory: String,
    pub docker_cpus: String,
    pub docker_pids_limit: u64,
    pub docker_tmpfs_size: String,
    pub enable_queue_limit: bool,
    pub queue_limit_multiplier: usize,
    pub enable_two_queue_scheduler: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:7000".to_string(), // the port that we are serving our server on. (locally) 
            worker_count: 4,
            max_in_flight_per_client: 2, // max num of current running jobs for each client 
            default_timeout_secs: 10, //  wall clock time for a single job (running) 
            max_timeout_secs: 60,
            result_ttl: Duration::from_secs(600), // 10 minutes (per plan.md §2.6) WE DONT WANT RESULTS TO STAY IN THE MEMORY FOREVER ! 
            cleanup_interval: Duration::from_secs(30), // A background thread clears the entries in the store with time elapsed > result_ttl
            sandbox_type: SandboxType::Docker,
            docker_runtime: std::env::var("DISTJOB_DOCKER_RUNTIME").ok(), // e.g. "runsc"
            docker_memory: "128m".to_string(),
            docker_cpus: "0.5".to_string(),
            docker_pids_limit: 64,
            docker_tmpfs_size: "64m".to_string(),
            enable_queue_limit: true,
            queue_limit_multiplier: 5,
            enable_two_queue_scheduler: true,
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(addr) = std::env::var("DISTJOB_BIND_ADDR") {
            config.bind_address = addr;
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_WORKERS").map(|w| w.parse::<usize>()) {
            config.worker_count = val;
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_CLIENT_CAP").map(|c| c.parse::<usize>()) {
            config.max_in_flight_per_client = val;
        }
        if let Ok(s_type) = std::env::var("DISTJOB_SANDBOX") {
            match s_type.to_lowercase().as_str() {
                "local" => config.sandbox_type = SandboxType::Local,
                "mock" => config.sandbox_type = SandboxType::Mock,
                "docker" => config.sandbox_type = SandboxType::Docker,
                _ => {}
            }
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_TTL_SECS").map(|t| t.parse::<u64>()) {
            config.result_ttl = Duration::from_secs(val);
        }
        if let Ok(val) = std::env::var("DISTJOB_ENABLE_QUEUE_LIMIT") {
            config.enable_queue_limit = matches!(val.to_lowercase().as_str(), "true" | "1" | "yes");
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_QUEUE_LIMIT_MULTIPLIER").map(|m| m.parse::<usize>()) {
            config.queue_limit_multiplier = val;
        }
        if let Ok(val) = std::env::var("DISTJOB_ENABLE_TWO_QUEUE_SCHEDULER") {
            config.enable_two_queue_scheduler = matches!(val.to_lowercase().as_str(), "true" | "1" | "yes");
        }

        config
    }
}

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxType {
    /// Full Docker sandbox with gVisor / limits as specified in plan.md
    Docker,
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
    pub enable_prewarmed_pool: bool,
    pub prewarmed_pool_size: usize,
    pub docker_python_image: String,
    pub store_shards: usize,
    pub enable_async_server: bool,
    pub redis_url: String,
    pub l1_max_capacity: usize,
    pub l1_ttl: Duration,
    pub l2_redis_ttl_secs: u64,
    pub max_polls_per_sec: u32,
    pub max_poll_strikes: u32,
    pub l2_penalty_ttl_secs: u64,
    pub client_idle_timeout: Duration,
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
            docker_cpus: "0.75".to_string(),
            docker_pids_limit: 64,
            docker_tmpfs_size: "64m".to_string(),
            enable_queue_limit: true,
            queue_limit_multiplier: 5,
            enable_two_queue_scheduler: true,
            enable_prewarmed_pool: true,
            prewarmed_pool_size: 4,
            docker_python_image: "distjob-python:latest".to_string(),
            store_shards: crate::store::DEFAULT_SHARDS,
            enable_async_server: false,
            redis_url: "redis://127.0.0.1:6379/1".to_string(),
            l1_max_capacity: 10_000,
            l1_ttl: Duration::from_secs(120),
            l2_redis_ttl_secs: 3600,
            max_polls_per_sec: 25,
            max_poll_strikes: 3,
            l2_penalty_ttl_secs: 60,
            client_idle_timeout: Duration::from_secs(300), // 5 minutes max inactivity before kicking idle client
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
            if s_type.to_lowercase().as_str() == "docker" {
                config.sandbox_type = SandboxType::Docker;
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
        if let Ok(val) = std::env::var("DISTJOB_ENABLE_PREWARMED_POOL") {
            config.enable_prewarmed_pool = matches!(val.to_lowercase().as_str(), "true" | "1" | "yes");
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_PREWARMED_POOL_SIZE").map(|s| s.parse::<usize>()) {
            config.prewarmed_pool_size = val;
        }
        if let Ok(img) = std::env::var("DISTJOB_PYTHON_IMAGE") {
            config.docker_python_image = img;
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_STORE_SHARDS").map(|s| s.parse::<usize>()) {
            config.store_shards = val;
        }
        if let Ok(val) = std::env::var("DISTJOB_ASYNC") {
            config.enable_async_server = matches!(val.to_lowercase().as_str(), "true" | "1" | "yes");
        }
        if let Ok(url) = std::env::var("DISTJOB_REDIS_URL") {
            config.redis_url = url;
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_L1_CAP").map(|v| v.parse::<usize>()) {
            config.l1_max_capacity = val;
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_L1_TTL_SECS").map(|v| v.parse::<u64>()) {
            config.l1_ttl = Duration::from_secs(val);
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_L2_TTL_SECS").map(|v| v.parse::<u64>()) {
            config.l2_redis_ttl_secs = val;
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_MAX_POLLS_PER_SEC").map(|v| v.parse::<u32>()) {
            config.max_polls_per_sec = val;
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_MAX_POLL_STRIKES").map(|v| v.parse::<u32>()) {
            config.max_poll_strikes = val;
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_L2_PENALTY_SECS").map(|v| v.parse::<u64>()) {
            config.l2_penalty_ttl_secs = val;
        }
        if let Ok(Ok(val)) = std::env::var("DISTJOB_IDLE_TIMEOUT_SECS").map(|v| v.parse::<u64>()) {
            config.client_idle_timeout = Duration::from_secs(val);
        }

        config
    }
}

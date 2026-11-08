// reads machines , num of cores , memory available , and memory (max as well as cpu max) given to docker (if at all running in a VM) 
use sysinfo::System; 
use std::process::Command;

const BYTES_PER_GIB: f64 = 1024.0 * 1024.0 * 1024.0;

#[derive(Debug, Clone, PartialEq)]
pub struct MachineProfile {
    pub num_core: u32,
    /// Estimated available memory on the host in bytes
    pub mem_size: u64,
    /// Total physical RAM on the host in bytes
    pub total_mem: u64,
    /// CPU cores allocated to Docker (if Docker is running)
    pub docker_cpu: Option<u32>,
    /// Memory in bytes allocated to Docker (if Docker is running)
    pub docker_mem: Option<u64>,
    /// Docker memory in GiB (e.g. 7.75 GiB)
    pub docker_gib: Option<f64>,
    /// Whether Docker daemon is reachable and responding
    pub docker_available: bool,
}

impl MachineProfile {
    /// Detects current machine and Docker resources without panicking.
    pub fn make_from_machine() -> Self {
        let num_cores = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1);

        let (total_mem, mem_size) = get_memory_info();

        let (docker_cpu, docker_mem) = match get_docker_resources() {
            Some((c, m)) => (Some(c), Some(m)),
            None => (get_docker_cpu(), get_docker_mem()),
        };

        let docker_available = docker_cpu.is_some() || docker_mem.is_some();
        let docker_gib = docker_mem.map(|bytes| (bytes as f64) / BYTES_PER_GIB);

        MachineProfile {
            num_core: num_cores,
            mem_size,
            total_mem,
            docker_cpu,
            docker_mem,
            docker_gib,
            docker_available,
        }
    }

    /// Effective CPU count for sizing worker pools and containers.
    /// Prefers Docker's assigned CPUs if available; otherwise falls back to host cores.
    pub fn effective_cpu(&self) -> u32 {
        self.docker_cpu.unwrap_or(self.num_core)
    }

    /// Effective memory limit in bytes for sizing container limits.
    /// Prefers Docker's assigned memory if available; otherwise falls back to available host memory.
    pub fn effective_mem(&self) -> u64 {
        self.docker_mem.unwrap_or(self.mem_size)
    }

    /// Effective memory in GiB.
    pub fn effective_mem_gib(&self) -> f64 {
        (self.effective_mem() as f64) / BYTES_PER_GIB
    }

    /// Automatically computes optimal server configuration parameters
    /// derived from detected physical CPU cores and RAM constraints.
    pub fn autoscale(&self) -> AutoScaledConfig {
        let cpu_workers = self.effective_cpu() as usize;

        // Container RAM constraint:
        // Assume default container limit is 128 MB. Total peak containers = Workers + WarmPool = 2 * Workers.
        // We allocate at most 60% of available/assigned RAM to containers, preserving 40% for OS & broker buffers.
        const CONTAINER_MEM_BYTES: f64 = 128.0 * 1024.0 * 1024.0;
        let mem_budget = (self.effective_mem() as f64) * 0.60;
        let mem_workers = (mem_budget / (2.0 * CONTAINER_MEM_BYTES)) as usize;

        // Balance CPU and RAM with safety floor (2) and ceiling (16 for single-node scale)
        let worker_count = cpu_workers.min(mem_workers).clamp(2, 16);

        // Pre-warmed pool matches worker count so all workers can burst without cold starts
        let prewarmed_pool_size = worker_count;

        // Concurrency cap per client: ~33% of worker pool (prevents single-client monopolization)
        let max_in_flight_per_client = (worker_count / 3).max(2);

        // ResultStore lock-striping shards: 4x worker count, rounded up to next power of two
        let store_shards = (worker_count * 4).max(16).next_power_of_two();

        AutoScaledConfig {
            worker_count,
            prewarmed_pool_size,
            max_in_flight_per_client,
            store_shards,
        }
    }
}

/// Automatically scaled server parameters derived from machine hardware profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoScaledConfig {
    pub worker_count: usize,
    pub prewarmed_pool_size: usize,
    pub max_in_flight_per_client: usize,
    pub store_shards: usize,
}

impl std::fmt::Display for MachineProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Host: {} cores, {:.2} GiB total RAM, {:.2} GiB available",
            self.num_core,
            self.total_mem as f64 / BYTES_PER_GIB,
            self.mem_size as f64 / BYTES_PER_GIB
        )?;
        if self.docker_available {
            writeln!(
                f,
                "Docker: {} cores, {:.2} GiB allocated RAM",
                self.docker_cpu.unwrap_or(0),
                self.docker_gib.unwrap_or(0.0)
            )?;
        } else {
            writeln!(f, "Docker: Not available / Not running")?;
        }
        write!(
            f,
            "Effective for Auto-Scaling: {} cores, {:.2} GiB RAM",
            self.effective_cpu(),
            self.effective_mem_gib()
        )
    }
}

/// Fetches (total_memory_bytes, available_memory_bytes) using sysinfo with Darwin/Linux compatibility.
fn get_memory_info() -> (u64, u64) {
    let mut sys = System::new();
    sys.refresh_memory();
    let total = sys.total_memory();
    let avail = sys.available_memory();

    // On macOS / Darwin, sysinfo may return 0 for available_memory().
    // In that case, compute an estimate using free_memory and total - used.
    let effective_avail = if avail > 0 {
        avail
    } else {
        sys.free_memory().max(total.saturating_sub(sys.used_memory()))
    };

    (total, effective_avail)
}

/// Fast single-shot query for both Docker CPU and Memory in one process invocation.
pub fn get_docker_resources() -> Option<(u32, u64)> {
    let output = Command::new("docker")
        .args(["info", "--format", "{{.NCPU}}|{{.MemTotal}}"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let text = std::str::from_utf8(&output.stdout).ok()?;
    let parts: Vec<&str> = text.trim().split('|').collect();
    if parts.len() != 2 {
        return None;
    }

    let cpu = parts[0].trim().parse::<u32>().ok()?;
    let mem = parts[1].trim().parse::<u64>().ok()?;
    Some((cpu, mem))
}

pub fn get_docker_cpu() -> Option<u32> {
    let cpu_output = Command::new("docker")
        .args(["info", "--format", "{{.NCPU}}"])
        .output()
        .ok()?;

    if cpu_output.status.success() {
        let text_output = std::str::from_utf8(&cpu_output.stdout).ok()?;
        text_output.trim().parse::<u32>().ok()
    } else {
        eprintln!(
            "Error getting Docker CPUs: {}",
            String::from_utf8_lossy(&cpu_output.stderr).trim()
        );
        None
    }
}

pub fn get_docker_mem() -> Option<u64> {
    let ram_output = Command::new("docker")
        .args(["info", "--format", "{{.MemTotal}}"])
        .output()
        .ok()?;

    if ram_output.status.success() {
        let ram_bytes_str = String::from_utf8_lossy(&ram_output.stdout).trim().to_string();
        ram_bytes_str.parse::<u64>().ok()
    } else {
        eprintln!(
            "Error getting Docker memory: {}",
            String::from_utf8_lossy(&ram_output.stderr).trim()
        );
        None
    }
}



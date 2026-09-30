use std::collections::VecDeque;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::config::Config;
use crate::model::{Job, JobState};

pub const MAX_OUTPUT_BYTES: u64 = 1024 * 1024; // 1 MB output limit

#[derive(Debug, Clone)]
pub struct SandboxExecutionResult {
    pub status: JobState,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub error_message: Option<String>,
    pub duration_ms: u64,
}

impl From<SandboxExecutionResult> for crate::store::ExecutionOutcome {
    fn from(res: SandboxExecutionResult) -> Self {
        Self {
            status: res.status,
            stdout: res.stdout,
            stderr: res.stderr,
            exit_code: res.exit_code,
            error_message: res.error_message,
            duration_ms: res.duration_ms,
        }
    }
}

pub trait Sandbox: Send + Sync {
    fn execute(&self, job: &Job) -> SandboxExecutionResult;
    fn shutdown(&self) {}
}

// ---------------------------------------------------------------------------
// Pre-Warmed Container Pool ("Bake and Burn")
// ---------------------------------------------------------------------------

fn check_docker_image_exists(image: &str) -> bool {
    Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn resolve_effective_image(configured: &str) -> String {
    if check_docker_image_exists(configured) {
        configured.to_string()
    } else if configured == "distjob-python:latest" {
        for fallback in &["python:3.11-slim", "python:3.11-alpine", "python:3"] {
            if check_docker_image_exists(fallback) {
                eprintln!(
                    "[WARN] Configured Docker image '{}' not found locally. Falling back to '{}'.",
                    configured, fallback
                );
                return fallback.to_string();
            }
        }
        configured.to_string()
    } else {
        configured.to_string()
    }
}

pub struct PrewarmedContainerPool {
    pool_size: usize,
    image: String,
    memory_limit: String,
    cpu_limit: String,
    pids_limit: u64,
    tmpfs_size: String,
    runtime: Option<String>,
    available: Mutex<VecDeque<String>>,
    condvar: Condvar,
    shutdown: Arc<AtomicBool>,
    replenish_thread: Mutex<Option<thread::JoinHandle<()>>>,
}

impl PrewarmedContainerPool {
    pub fn start(config: &Config) -> Arc<Self> {
        // Clean up any orphan containers from previous unclean shutdowns
        if let Ok(output) = Command::new("docker")
            .args(["ps", "-aq", "--filter", "name=distjob-warm-"])
            .output()
        {
            let raw = String::from_utf8_lossy(&output.stdout);
            let ids: Vec<&str> = raw.split_whitespace().collect();
            if !ids.is_empty() {
                let _ = Command::new("docker").arg("rm").arg("-f").args(&ids).output();
            }
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let effective_image = resolve_effective_image(&config.docker_python_image);
        let pool = Arc::new(Self {
            pool_size: config.prewarmed_pool_size,
            image: effective_image,
            memory_limit: config.docker_memory.clone(),
            cpu_limit: config.docker_cpus.clone(),
            pids_limit: config.docker_pids_limit,
            tmpfs_size: config.docker_tmpfs_size.clone(),
            runtime: config.docker_runtime.clone(),
            available: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            shutdown: Arc::clone(&shutdown),
            replenish_thread: Mutex::new(None),
        });

        let pool_clone = Arc::clone(&pool);
        let handle = thread::Builder::new()
            .name("distjob-warm-pool".to_string())
            .spawn(move || {
                pool_clone.replenish_loop();
            })
            .expect("Failed to spawn prewarmed pool thread");

        *pool.replenish_thread.lock().unwrap() = Some(handle);
        pool
    }

    /// Claim a standby container name from the pool in O(1).
    /// If successful, wakes the replenishment thread in the background.
    pub fn claim(&self) -> Option<String> {
        let mut available = self.available.lock().unwrap();
        let container = available.pop_front();
        if container.is_some() {
            self.condvar.notify_one();
        }
        container
    }

    /// Current count of warm standby containers ready for dispatch.
    pub fn available_count(&self) -> usize {
        self.available.lock().unwrap().len()
    }

    /// Target pool capacity.
    pub fn capacity(&self) -> usize {
        self.pool_size
    }

    fn replenish_loop(&self) {
        while !self.shutdown.load(Ordering::Relaxed) {
            let needed = {
                let mut available = self.available.lock().unwrap();
                while available.len() >= self.pool_size && !self.shutdown.load(Ordering::Relaxed) {
                    available = self.condvar.wait(available).unwrap();
                    // if the num of containers are equal to the pool size (basically no container was killed so no need to replenish the containers) 
                    // so the thread that is running this givess up ownership of available for other threads to use the lock . 
                }
                if self.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                self.pool_size.saturating_sub(available.len())
            };

            if needed > 0 {
                // make the needed num of containers 
                let container_name = format!("distjob-warm-{}", &Uuid::new_v4().to_string()[..8]);
                if self.spawn_container(&container_name) {
                    let mut available = self.available.lock().unwrap();
                    available.push_back(container_name);
                } else {
                    // Back off if daemon is down or spawn failed
                    thread::sleep(Duration::from_secs(3));
                }
            }
        }

        // Cleanup: remove all remaining standby containers
        // happens during shutdown. 
        let remaining: Vec<String> = self.available.lock().unwrap().drain(..).collect();
        for name in remaining {
            let _ = Command::new("docker").args(["rm", "-f", &name]).output();
        }
    }

    fn spawn_container(&self, container_name: &str) -> bool {
        let mut cmd = Command::new("docker");
        cmd.arg("run")
            .arg("-d")
            .arg("--name")
            .arg(container_name)
            .arg("--network")
            .arg("none")
            .arg("--memory")
            .arg(&self.memory_limit)
            .arg("--cpus")
            .arg(&self.cpu_limit)
            .arg("--pids-limit")
            .arg(self.pids_limit.to_string())
            .arg("--read-only")
            .arg("--tmpfs")
            .arg(format!("/tmp:rw,noexec,nosuid,size={}", self.tmpfs_size));

        if let Some(ref rt) = self.runtime {
            cmd.arg("--runtime").arg(rt);
        }

        cmd.arg(&self.image);
        cmd.arg("sleep").arg("infinity");
        cmd.stdout(Stdio::null()).stderr(Stdio::null());

        match cmd.output() {
            Ok(output) => output.status.success(),
            Err(e) => {
                eprintln!("[PrewarmedPool] Failed to spawn standby container '{}': {}", container_name, e);
                false
            }
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.condvar.notify_all();
        if let Some(handle) = self.replenish_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for PrewarmedContainerPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ---------------------------------------------------------------------------
// Docker + gVisor Sandbox
// ---------------------------------------------------------------------------

pub struct DockerSandbox {
    runtime: Option<String>,
    memory_limit: String,
    cpu_limit: String,
    pids_limit: u64,
    tmpfs_size: String,
    python_image: String,
    pool: Option<Arc<PrewarmedContainerPool>>,
}

impl DockerSandbox {
    pub fn new(config: &Config) -> Self {
        let effective_image = resolve_effective_image(&config.docker_python_image);
        let pool = if config.enable_prewarmed_pool {
            Some(PrewarmedContainerPool::start(config))
        } else {
            None
        };

        Self {
            runtime: config.docker_runtime.clone(),
            memory_limit: config.docker_memory.clone(),
            cpu_limit: config.docker_cpus.clone(),
            pids_limit: config.docker_pids_limit,
            tmpfs_size: config.docker_tmpfs_size.clone(),
            python_image: effective_image,
            pool,
        }
    }

    pub fn prewarmed_pool(&self) -> Option<&Arc<PrewarmedContainerPool>> {
        self.pool.as_ref()
    }

    fn resolve_language_spec(&self, lang: &str) -> Option<(&str, &'static str, Vec<String>)> {
        match lang.to_lowercase().as_str() {
            "python" | "python3" | "py" => Some((
                self.python_image.as_str(),
                "script.py",
                vec!["python3".to_string(), "/code/script.py".to_string()],
            )),
            "sh" | "bash" | "shell" => Some((
                "alpine:latest",
                "script.sh",
                vec!["sh".to_string(), "/code/script.sh".to_string()],
            )),
            "node" | "javascript" | "js" => Some((
                "node:alpine",
                "script.js",
                vec!["node".to_string(), "/code/script.js".to_string()],
            )),
            _ => None,
        }
    }

    /// Fast execution using a pre-warmed standby container.
    /// Code is injected into the container's tmpfs, executed with hard timeouts,
    /// and the container is burned in the background afterwards.
    fn execute_warm(
        &self,
        container_name: &str,
        job: &Job,
        start: Instant,
        timeout: Duration,
    ) -> SandboxExecutionResult {
        let is_shell = matches!(job.language.to_lowercase().as_str(), "sh" | "bash" | "shell");
        let (script_name, exec_binary) = if is_shell {
            ("script.sh", "sh")
        } else {
            ("script.py", "python3")
        };

        let container_script_path = format!("/tmp/{}", script_name);

        // 1. Stage the script into the container's tmpfs via stdin pipe to cat
        let mut stage_cmd = Command::new("docker");
        stage_cmd
            .args(["exec", "-i", container_name, "sh", "-c", &format!("cat > {}", container_script_path)])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        let stage_ok = (|| -> std::io::Result<bool> {
            let mut child = stage_cmd.spawn()?;
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(job.code.as_bytes())?;
            }
            let status = child.wait()?;
            Ok(status.success())
        })().unwrap_or(false);

        if !stage_ok {
            return SandboxExecutionResult {
                status: JobState::Failed,
                stdout: String::new(),
                stderr: "Failed to stage script in pre-warmed container".to_string(),
                exit_code: Some(1),
                error_message: Some("Pre-warmed staging error".to_string()),
                duration_ms: start.elapsed().as_millis() as u64,
            };
        }

        // 2. Execute the staged script inside the warm container
        let mut exec_cmd = Command::new("docker");
        exec_cmd
            .args(["exec", "-i", container_name, exec_binary, &container_script_path])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = match exec_cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                return SandboxExecutionResult {
                    status: JobState::Failed,
                    stdout: String::new(),
                    stderr: format!("Failed to exec in pre-warmed container: {}", e),
                    exit_code: Some(1),
                    error_message: Some(format!("Pre-warmed exec error: {}", e)),
                    duration_ms: start.elapsed().as_millis() as u64,
                };
            }
        };

        // Write stdin if provided and close pipe to send EOF
        if let Some(mut stdin) = child.stdin.take() {
            if let Some(ref input) = job.stdin {
                let _ = stdin.write_all(input.as_bytes());
            }
        }

        let mut stdout_handle = child.stdout.take().unwrap();
        let mut stderr_handle = child.stderr.take().unwrap();

        let stdout_thread = thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = (&mut stdout_handle).take(MAX_OUTPUT_BYTES).read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).to_string()
        });

        let stderr_thread = thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = (&mut stderr_handle).take(MAX_OUTPUT_BYTES).read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).to_string()
        });

        let mut timed_out = false;
        let mut exit_status = None;

        while start.elapsed() < timeout {
            match child.try_wait() {
                Ok(Some(status)) => {
                    exit_status = Some(status);
                    break;
                }
                Ok(None) => {
                    thread::sleep(Duration::from_millis(15));
                }
                Err(e) => {
                    eprintln!("Error waiting on warm child: {}", e);
                    break;
                }
            }
        }

        if exit_status.is_none() {
            timed_out = true;
            let _ = Command::new("docker").args(["kill", container_name]).output();
            let _ = child.kill();
            let _ = child.wait();
        }

        let stdout = stdout_thread.join().unwrap_or_default();
        let mut stderr = stderr_thread.join().unwrap_or_default();
        let duration_ms = start.elapsed().as_millis() as u64;

        if timed_out {
            stderr.push_str(&format!(
                "\n[Broker] Execution timed out after {} seconds and was forcibly killed.",
                job.timeout_secs
            ));
            SandboxExecutionResult {
                status: JobState::TimedOut,
                stdout,
                stderr,
                exit_code: None,
                error_message: Some(format!("Job timed out after {} seconds", job.timeout_secs)),
                duration_ms,
            }
        } else {
            let code = exit_status.and_then(|s| s.code());
            let status = match code {
                Some(0) => JobState::Done,
                _ => JobState::Failed,
            };
            SandboxExecutionResult {
                status,
                stdout,
                stderr,
                exit_code: code,
                error_message: if status == JobState::Failed {
                    Some(format!("Process exited with code {:?}", code))
                } else {
                    None
                },
                duration_ms,
            }
        }
    }

    /// On-demand fallback execution: spawns a single-use container via 'docker run --rm'.
    fn execute_on_demand(&self, job: &Job, start: Instant, timeout: Duration) -> SandboxExecutionResult {
        let (image, script_filename, run_args) = match self.resolve_language_spec(&job.language) {
            Some(spec) => spec,
            None => {
                return SandboxExecutionResult {
                    status: JobState::Failed,
                    stdout: String::new(),
                    stderr: format!("Unsupported language: '{}'", job.language),
                    exit_code: Some(1),
                    error_message: Some(format!("Unsupported language: '{}'", job.language)),
                    duration_ms: start.elapsed().as_millis() as u64,
                };
            }
        };

        // Create ephemeral host directory for mounting
        let container_name = format!("distjob-{}", job.id);
        let host_temp_dir: PathBuf = std::env::temp_dir().join(&container_name);

        if let Err(e) = fs::create_dir_all(&host_temp_dir) {
            return SandboxExecutionResult {
                status: JobState::Failed,
                stdout: String::new(),
                stderr: format!("Failed to create host temp dir: {}", e),
                exit_code: Some(1),
                error_message: Some(format!("Filesystem error: {}", e)),
                duration_ms: start.elapsed().as_millis() as u64,
            };
        }

        let script_path = host_temp_dir.join(script_filename);
        if let Err(e) = fs::write(&script_path, &job.code) {
            let _ = fs::remove_dir_all(&host_temp_dir);
            return SandboxExecutionResult {
                status: JobState::Failed,
                stdout: String::new(),
                stderr: format!("Failed to write code to file: {}", e),
                exit_code: Some(1),
                error_message: Some(format!("Filesystem error: {}", e)),
                duration_ms: start.elapsed().as_millis() as u64,
            };
        }

        let mut cmd = Command::new("docker");
        cmd.arg("run")
            .arg("-i")
            .arg("--rm")
            .arg("--name")
            .arg(&container_name)
            .arg("--network")
            .arg("none")
            .arg("--memory")
            .arg(&self.memory_limit)
            .arg("--cpus")
            .arg(&self.cpu_limit)
            .arg("--pids-limit")
            .arg(self.pids_limit.to_string())
            .arg("--read-only")
            .arg("--tmpfs")
            .arg(format!("/tmp:rw,noexec,nosuid,size={}", self.tmpfs_size))
            .arg("-v")
            .arg(format!("{}:/code:ro", host_temp_dir.display()));

        if let Some(ref rt) = self.runtime {
            cmd.arg("--runtime").arg(rt);
        }

        cmd.arg(image);
        for arg in run_args {
            cmd.arg(arg);
        }

        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let _ = fs::remove_dir_all(&host_temp_dir);
                return SandboxExecutionResult {
                    status: JobState::Failed,
                    stdout: String::new(),
                    stderr: format!("Failed to spawn docker process (is Docker running?): {}", e),
                    exit_code: Some(1),
                    error_message: Some(format!("Docker invocation error: {}", e)),
                    duration_ms: start.elapsed().as_millis() as u64,
                };
            }
        };

        // Write stdin if provided and close pipe to send EOF
        if let Some(mut stdin) = child.stdin.take() {
            if let Some(ref input) = job.stdin {
                let _ = stdin.write_all(input.as_bytes());
            }
        }

        let mut stdout_handle = child.stdout.take().unwrap();
        let mut stderr_handle = child.stderr.take().unwrap();

        let stdout_thread = thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = (&mut stdout_handle).take(MAX_OUTPUT_BYTES).read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).to_string()
        });

        let stderr_thread = thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = (&mut stderr_handle).take(MAX_OUTPUT_BYTES).read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).to_string()
        });

        let mut timed_out = false;
        let mut exit_status = None;

        while start.elapsed() < timeout {
            match child.try_wait() {
                Ok(Some(status)) => {
                    exit_status = Some(status);
                    break;
                }
                Ok(None) => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(e) => {
                    eprintln!("Error waiting on child: {}", e);
                    break;
                }
            }
        }

        if exit_status.is_none() {
            timed_out = true;
            let _ = Command::new("docker")
                .args(["kill", &container_name])
                .output();
            let _ = child.kill();
            let _ = child.wait();
        }

        let stdout = stdout_thread.join().unwrap_or_default();
        let mut stderr = stderr_thread.join().unwrap_or_default();
        let _ = fs::remove_dir_all(&host_temp_dir);
        let duration_ms = start.elapsed().as_millis() as u64;

        if timed_out {
            stderr.push_str(&format!(
                "\n[Broker] Execution timed out after {} seconds and was forcibly killed.",
                job.timeout_secs
            ));
            SandboxExecutionResult {
                status: JobState::TimedOut,
                stdout,
                stderr,
                exit_code: None,
                error_message: Some(format!("Job timed out after {} seconds", job.timeout_secs)),
                duration_ms,
            }
        } else {
            let code = exit_status.and_then(|s| s.code());
            let status = match code {
                Some(0) => JobState::Done,
                _ => JobState::Failed,
            };
            SandboxExecutionResult {
                status,
                stdout,
                stderr,
                exit_code: code,
                error_message: if status == JobState::Failed {
                    Some(format!("Process exited with code {:?}", code))
                } else {
                    None
                },
                duration_ms,
            }
        }
    }
}

impl Sandbox for DockerSandbox {
    fn execute(&self, job: &Job) -> SandboxExecutionResult {
        let start = Instant::now();
        let timeout = Duration::from_secs(job.timeout_secs);

        let is_python_or_sh = matches!(job.language.to_lowercase().as_str(), "python" | "python3" | "py" | "sh" | "bash" | "shell");

        // Fast path: use a pre-warmed container from standby pool if available
        if is_python_or_sh
            && let Some(pool) = &self.pool
            && let Some(warm_name) = pool.claim()
        {
            let result = self.execute_warm(&warm_name, job, start, timeout);

            // Asynchronously burn the container in background (zero latency impact for user)
            thread::spawn(move || {
                let _ = Command::new("docker").args(["rm", "-f", &warm_name]).output();
            });

            return result;
        }

        // On-demand path: docker run --rm
        self.execute_on_demand(job, start, timeout)
    }

    fn shutdown(&self) {
        if let Some(pool) = &self.pool {
            pool.shutdown();
        }
    }
}

// ---------------------------------------------------------------------------
// Local Process Sandbox (Fallback / Development)
// ---------------------------------------------------------------------------

pub struct LocalProcessSandbox;

impl Sandbox for LocalProcessSandbox {
    fn execute(&self, job: &Job) -> SandboxExecutionResult {
        let start = Instant::now();
        let timeout = Duration::from_secs(job.timeout_secs);

        let (bin, args) = match job.language.to_lowercase().as_str() {
            "python" | "python3" | "py" => ("python3", vec!["-c".to_string(), job.code.clone()]),
            "sh" | "bash" | "shell" => ("sh", vec!["-c".to_string(), job.code.clone()]),
            "node" | "javascript" | "js" => ("node", vec!["-e".to_string(), job.code.clone()]),
            _ => {
                return SandboxExecutionResult {
                    status: JobState::Failed,
                    stdout: String::new(),
                    stderr: format!("Unsupported language for local runner: '{}'", job.language),
                    exit_code: Some(1),
                    error_message: Some(format!("Unsupported language: '{}'", job.language)),
                    duration_ms: start.elapsed().as_millis() as u64,
                };
            }
        };

        let mut cmd = Command::new(bin);
        for arg in args {
            cmd.arg(arg);
        }

        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                return SandboxExecutionResult {
                    status: JobState::Failed,
                    stdout: String::new(),
                    stderr: format!("Failed to spawn local process: {}", e),
                    exit_code: Some(1),
                    error_message: Some(format!("Process spawn error: {}", e)),
                    duration_ms: start.elapsed().as_millis() as u64,
                };
            }
        };

        if let (Some(mut stdin), Some(input)) = (child.stdin.take(), &job.stdin) {
            let _ = stdin.write_all(input.as_bytes());
        }

        let mut stdout_handle = child.stdout.take().unwrap();
        let mut stderr_handle = child.stderr.take().unwrap();

        let stdout_thread = thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = (&mut stdout_handle).take(MAX_OUTPUT_BYTES).read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).to_string()
        });

        let stderr_thread = thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = (&mut stderr_handle).take(MAX_OUTPUT_BYTES).read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).to_string()
        });

        let mut timed_out = false;
        let mut exit_status = None;

        while start.elapsed() < timeout {
            match child.try_wait() {
                Ok(Some(status)) => {
                    exit_status = Some(status);
                    break;
                }
                Ok(None) => {
                    thread::sleep(Duration::from_millis(20));
                }
                Err(e) => {
                    eprintln!("Error waiting on local process: {}", e);
                    break;
                }
            }
        }

        if exit_status.is_none() {
            timed_out = true;
            let _ = child.kill();
            let _ = child.wait();
        }

        let stdout = stdout_thread.join().unwrap_or_default();
        let mut stderr = stderr_thread.join().unwrap_or_default();
        let duration_ms = start.elapsed().as_millis() as u64;

        if timed_out {
            stderr.push_str(&format!(
                "\n[Broker] Execution timed out after {} seconds.",
                job.timeout_secs
            ));
            SandboxExecutionResult {
                status: JobState::TimedOut,
                stdout,
                stderr,
                exit_code: None,
                error_message: Some(format!("Job timed out after {} seconds", job.timeout_secs)),
                duration_ms,
            }
        } else {
            let code = exit_status.and_then(|s| s.code());
            let status = match code {
                Some(0) => JobState::Done,
                _ => JobState::Failed,
            };
            SandboxExecutionResult {
                status,
                stdout,
                stderr,
                exit_code: code,
                error_message: if status == JobState::Failed {
                    Some(format!("Process exited with code {:?}", code))
                } else {
                    None
                },
                duration_ms,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Mock Sandbox (For deterministic testing of worker logic, timeouts, etc.)
// ---------------------------------------------------------------------------

pub struct MockSandbox;

impl Sandbox for MockSandbox {
    fn execute(&self, job: &Job) -> SandboxExecutionResult {
        let start = Instant::now();

        // Emulate timeout if code contains "SLEEP_TIMEOUT"
        if job.code.contains("SLEEP_TIMEOUT") {
            thread::sleep(Duration::from_millis(
                (job.timeout_secs * 1000).min(100),
            ));
            return SandboxExecutionResult {
                status: JobState::TimedOut,
                stdout: String::new(),
                stderr: "Mock timeout triggered".to_string(),
                exit_code: None,
                error_message: Some("Job timed out".to_string()),
                duration_ms: start.elapsed().as_millis() as u64,
            };
        }

        // Emulate failure if code contains "FAIL"
        if job.code.contains("FAIL") {
            return SandboxExecutionResult {
                status: JobState::Failed,
                stdout: String::new(),
                stderr: "Mock failure triggered".to_string(),
                exit_code: Some(1),
                error_message: Some("Mock failure".to_string()),
                duration_ms: start.elapsed().as_millis() as u64,
            };
        }

        // Default mock success: echo code and stdin
        let output = format!(
            "Executed: {} (lang: {}, stdin: {:?})",
            job.code, job.language, job.stdin
        );

        SandboxExecutionResult {
            status: JobState::Done,
            stdout: output,
            stderr: String::new(),
            exit_code: Some(0),
            error_message: None,
            duration_ms: start.elapsed().as_millis() as u64,
        }
    }
}

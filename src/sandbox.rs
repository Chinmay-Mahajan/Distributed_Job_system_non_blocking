use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

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
}

impl DockerSandbox {
    pub fn new(config: &Config) -> Self {
        Self {
            runtime: config.docker_runtime.clone(),
            memory_limit: config.docker_memory.clone(),
            cpu_limit: config.docker_cpus.clone(),
            pids_limit: config.docker_pids_limit,
            tmpfs_size: config.docker_tmpfs_size.clone(),
        }
    }

    fn resolve_language_spec(&self, lang: &str) -> Option<(&'static str, &'static str, Vec<String>)> {
        match lang.to_lowercase().as_str() {
            "python" | "python3" | "py" => Some((
                "python:3.11-slim",
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
}

impl Sandbox for DockerSandbox {
    fn execute(&self, job: &Job) -> SandboxExecutionResult {
        let start = Instant::now();
        let timeout = Duration::from_secs(job.timeout_secs);

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

        // Build docker run command with strict security flags per plan.md §2.5:
        // - Ephemeral: --rm
        // - Unique name: --name distjob-<id>
        // - Network off: --network none
        // - Memory limit: --memory <mem>
        // - CPU limit: --cpus <cpu>
        // - Process limit: --pids-limit <pids>
        // - Read-only root filesystem: --read-only
        // - Small writable tmpfs: --tmpfs /tmp:rw,noexec,nosuid,size=<size>
        // - Read-only host mount for script: -v <host_temp>:/code:ro
        // - No docker socket mounted
        // - gVisor runtime if enabled: --runtime runsc
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

        // Write stdin if provided
        if let (Some(mut stdin), Some(input)) = (child.stdin.take(), &job.stdin) {
            let _ = stdin.write_all(input.as_bytes());
        }

        // Read stdout and stderr asynchronously in background threads to avoid deadlocks
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

        // Enforce hard wall-clock timeout
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
            // Kill the docker container forcibly
            let _ = Command::new("docker")
                .args(["kill", &container_name])
                .output();
            let _ = child.kill();
            let _ = child.wait();
        }

        let stdout = stdout_thread.join().unwrap_or_default();
        let mut stderr = stderr_thread.join().unwrap_or_default();

        // Clean up host temp directory
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

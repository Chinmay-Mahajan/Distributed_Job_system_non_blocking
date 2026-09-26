use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::model::{Job, JobResult, JobState};

struct StoredEntry {
    result: JobResult,
    _inserted_at: Instant,
    expires_at: Instant,
}

pub struct ResultStore {
    entries: RwLock<HashMap<String, StoredEntry>>,
    ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct ExecutionOutcome {
    pub status: JobState,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub error_message: Option<String>,
    pub duration_ms: u64,
}

impl ResultStore {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Record a newly submitted job in `Pending` state.
    pub fn insert_pending(&self, job: &Job) {
        let now = Instant::now();
        let entry = StoredEntry {
            result: JobResult::new_pending(job),
            _inserted_at: now,
            expires_at: now + self.ttl,
        };

        let mut lock = self.entries.write().unwrap();
        lock.insert(job.id.clone(), entry);
    }

    /// Mark a job as `Running` when picked up by a worker.
    pub fn set_running(&self, job_id: &str) {
        let mut lock = self.entries.write().unwrap();
        if let Some(entry) = lock.get_mut(job_id) {
            entry.result.status = JobState::Running;
            // Refresh TTL while running so long-running jobs aren't purged mid-execution
            let now = Instant::now();
            entry.expires_at = now + self.ttl;
        }
    }

    /// Record the final outcome of a job execution.
    pub fn set_completed(&self, job_id: &str, outcome: ExecutionOutcome) {
        let now_sys = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut lock = self.entries.write().unwrap();
        if let Some(entry) = lock.get_mut(job_id) {
            entry.result.status = outcome.status;
            entry.result.stdout = outcome.stdout;
            entry.result.stderr = outcome.stderr;
            entry.result.exit_code = outcome.exit_code;
            entry.result.error_message = outcome.error_message;
            entry.result.completed_at_ms = Some(now_sys);
            entry.result.execution_duration_ms = Some(outcome.duration_ms);

            // Set final TTL from completion time
            let now = Instant::now();
            entry.expires_at = now + self.ttl;
        }
    }

    /// Retrieve the result of a job. If expired, removes it and returns None.
    pub fn get(&self, job_id: &str) -> Option<JobResult> {
        let now = Instant::now();

        // Fast path: read lock
        {
            let lock = self.entries.read().unwrap();
            if let Some(entry) = lock.get(job_id) {
                if now < entry.expires_at {
                    return Some(entry.result.clone());
                }
            } else {
                return None;
            }
        }

        // Expired path: acquire write lock to remove
        let mut lock = self.entries.write().unwrap();
        if let Some(entry) = lock.get(job_id) {
            if now >= entry.expires_at {
                lock.remove(job_id);
                return None;
            }
            return Some(entry.result.clone());
        }

        None
    }

    /// Remove all expired entries.
    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let mut lock = self.entries.write().unwrap();
        let initial_len = lock.len();
        lock.retain(|_, entry| now < entry.expires_at);
        initial_len - lock.len()
    }

    /// Current count of stored entries.
    pub fn count(&self) -> usize {
        let lock = self.entries.read().unwrap();
        lock.len()
    }

    /// Spawns a background worker thread that periodically cleans up expired entries.
    pub fn start_cleanup_thread(
        self: Arc<Self>,
        interval: Duration,
        shutdown: Arc<AtomicBool>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(interval);
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                self.cleanup_expired();
            }
        })
    }
}

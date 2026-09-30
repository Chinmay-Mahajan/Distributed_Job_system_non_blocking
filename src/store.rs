use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::model::{Job, JobResult, JobState};

/// Default number of shards for lock striping.
/// 32 shards provides 32 concurrent write lanes, virtually eliminating
/// lock contention between worker threads and client pollers.
pub const DEFAULT_SHARDS: usize = 32;

struct StoredEntry {
    result: JobResult,
    _inserted_at: Instant,
    expires_at: Instant,
}

pub struct ResultStore {
    shards: Vec<RwLock<HashMap<String, StoredEntry>>>,
    shard_mask: usize,
    // A rwlock is used here and not a mutex as a mutex is blocks all the other threads from even reading when one is reading the shared data . 
    // a rwlock allows multiple thread to read at once as long as they dont mutate the shared data . maintains a reader and also a writer lock so more overhead .
    // but but but in this case the read ops on the shared hashmap dominate the write ops .
    //
    // LOCK SHARDING (STRIPING):
    // Instead of a single monolithic RwLock guarding the entire HashMap (which causes lock contention
    // and cache-line bouncing between CPU cores when workers finish and clients poll), the key space
    // is partitioned into `N` independent shards (default: 32).
    // - Each shard has its own RwLock<HashMap<String, StoredEntry>>.
    // - Jobs are mapped to shards via fast hash & mask: `hash(job_id) & (num_shards - 1)`.
    // - Two operations on different job IDs run completely in parallel with zero contention.
    // - TTL Reaper cleans one shard at a time, eliminating the global stop-the-world broker freeze.
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
    /// Creates a new ResultStore with the default shard count (32).
    pub fn new(ttl: Duration) -> Self {
        Self::with_shards(ttl, DEFAULT_SHARDS)
    }

    /// Creates a new ResultStore with a customized number of shards.
    /// Shard count is rounded up to the nearest power of two for fast bitwise indexing.
    pub fn with_shards(ttl: Duration, num_shards: usize) -> Self {
        let num_shards = num_shards.next_power_of_two().max(1);
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self {
            shards,
            shard_mask: num_shards - 1,
            ttl,
        }
    }

    /// Returns the number of shards configured in this store.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Computes the shard for a given job_id using hash & mask.
    #[inline]
    fn shard_for(&self, job_id: &str) -> &RwLock<HashMap<String, StoredEntry>> {
        let mut hasher = DefaultHasher::new();
        job_id.hash(&mut hasher);
        let index = (hasher.finish() as usize) & self.shard_mask;
        &self.shards[index]
    }

    /// Record a newly submitted job in `Pending` state.
    pub fn insert_pending(&self, job: &Job) {
        let now = Instant::now();
        let entry = StoredEntry {
            result: JobResult::new_pending(job),
            _inserted_at: now,
            expires_at: now + self.ttl,
        };

        let shard = self.shard_for(&job.id);
        let mut lock = shard.write().unwrap(); // acquire write lock on this shard only
        lock.insert(job.id.clone(), entry);
    }

    /// Mark a job as `Running` when picked up by a worker.
    pub fn set_running(&self, job_id: &str) {
        let shard = self.shard_for(job_id);
        let mut lock = shard.write().unwrap();
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

        let shard = self.shard_for(job_id);
        let mut lock = shard.write().unwrap();
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
        let shard = self.shard_for(job_id);

        // Fast path: read lock on this specific shard
        {
            let lock = shard.read().unwrap();
            if let Some(entry) = lock.get(job_id) {
                if now < entry.expires_at {
                    return Some(entry.result.clone());
                }
            } else {
                return None;
            }
        }

        // Expired path: acquire write lock on this specific shard to remove
        let mut lock = shard.write().unwrap();
        if let Some(entry) = lock.get(job_id) {
            if now >= entry.expires_at {
                lock.remove(job_id);
                return None;
            }
            return Some(entry.result.clone());
        }

        None
    }

    /// Remove all expired entries across all shards.
    ///
    /// Locks one shard at a time to prevent "reaper freeze" — 
    /// other shards remain fully operational while one shard is being pruned.
    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let mut total_removed = 0;
        for shard in &self.shards {
            let mut lock = shard.write().unwrap();
            let initial_len = lock.len();
            lock.retain(|_, entry| now < entry.expires_at);
            total_removed += initial_len - lock.len();
        }
        total_removed
    }

    /// Current count of stored entries across all shards.
    pub fn count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().unwrap().len())
            .sum()
    }

    /// Returns the number of items currently stored in each individual shard.
    /// Useful for monitoring shard distribution balance and testing.
    pub fn shard_counts(&self) -> Vec<usize> {
        self.shards
            .iter()
            .map(|s| s.read().unwrap().len())
            .collect()
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

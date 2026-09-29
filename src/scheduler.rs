use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Condvar, Mutex};

use crate::config::Config;
use crate::model::Job;

struct InnerScheduler {
    client_queues: HashMap<String, VecDeque<Job>>, // client id -> per-client FIFO queue of pending jobs
    client_in_flight: HashMap<String, usize>, // client id -> count of jobs currently running in workers
    max_in_flight_per_client: usize,
    enable_two_queue_scheduler: bool,
    enable_queue_limit: bool,
    queue_limit_multiplier: usize,
    shutdown: bool,

    // Legacy single-queue rotation (used when enable_two_queue_scheduler == false)
    client_order: VecDeque<String>,

    // Two-queue scheduler (used when enable_two_queue_scheduler == true)
    // 1. Dispatchable queue: FIFO order of clients with pending jobs AND in_flight < max
    dispatchable_queue: VecDeque<String>,
    in_dispatchable: HashSet<String>,

    // 2. Waiting queue: Clients with pending jobs BUT in_flight >= max (saturated)
    waiting_queue: HashSet<String>,
}

pub struct TwoLevelScheduler {
    inner: Mutex<InnerScheduler>,
    condvar: Condvar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerStats {
    pub active_clients: usize,
    pub queued_jobs: usize,
    pub in_flight_jobs: usize,
    pub dispatchable_clients: usize,
    pub waiting_clients: usize,
}

impl TwoLevelScheduler {
    /// Create a scheduler with default settings for the given client capacity cap.
    pub fn new(max_in_flight_per_client: usize) -> Self {
        Self::with_config(max_in_flight_per_client, true, true, 5)
    }

    /// Create a scheduler initialized from the application `Config`.
    pub fn from_config(config: &Config) -> Self {
        Self::with_config(
            config.max_in_flight_per_client,
            config.enable_two_queue_scheduler,
            config.enable_queue_limit,
            config.queue_limit_multiplier,
        )
    }

    /// Full constructor allowing fine-grained configuration of all scheduler features.
    pub fn with_config(
        max_in_flight_per_client: usize,
        enable_two_queue_scheduler: bool,
        enable_queue_limit: bool,
        queue_limit_multiplier: usize,
    ) -> Self {
        Self {
            inner: Mutex::new(InnerScheduler {
                client_queues: HashMap::new(),
                client_in_flight: HashMap::new(),
                max_in_flight_per_client,
                enable_two_queue_scheduler,
                enable_queue_limit,
                queue_limit_multiplier,
                shutdown: false,
                client_order: VecDeque::new(),
                dispatchable_queue: VecDeque::new(),
                in_dispatchable: HashSet::new(),
                waiting_queue: HashSet::new(),
            }),
            condvar: Condvar::new(),
        }
    }

    /// Submit a job to the client's FIFO queue.
    ///
    /// If queue limiting is enabled and the client has already reached their maximum
    /// queued jobs (defined as `max_in_flight_per_client * queue_limit_multiplier`),
    /// this returns an `Err` describing the limit violation.
    pub fn submit(&self, job: Job) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        if inner.shutdown {
            return Err("Scheduler is shutting down".to_string());
        }

        let client_id = job.client_id.clone();

        // 1. Enforce queue / rate limit if enabled
        if inner.enable_queue_limit {
            let current_queued = inner.client_queues.get(&client_id).map(|q| q.len()).unwrap_or(0);
            let effective_mult = inner.queue_limit_multiplier.max(1);
            let max_queued = inner.max_in_flight_per_client * effective_mult;

            if current_queued >= max_queued {
                return Err(format!(
                    "Rate limit exceeded: client '{}' reached maximum queue capacity of {} pending jobs (in-flight cap: {}, multiplier: {})",
                    client_id, max_queued, inner.max_in_flight_per_client, inner.queue_limit_multiplier
                ));
            }
        }

        // 2. Enqueue into the client's isolated FIFO queue
        inner
            .client_queues
            .entry(client_id.clone())
            .or_default()
            .push_back(job);

        let in_flight = inner.client_in_flight.get(&client_id).copied().unwrap_or(0);
        let mut notify_worker = false;

        if inner.enable_two_queue_scheduler {
            if in_flight < inner.max_in_flight_per_client {
                // Client has capacity: add to dispatchable queue if not already present
                if !inner.in_dispatchable.contains(&client_id) {
                    inner.dispatchable_queue.push_back(client_id.clone());
                    inner.in_dispatchable.insert(client_id);
                    notify_worker = true;
                }
            } else {
                // Client is saturated: place in waiting queue
                inner.waiting_queue.insert(client_id);
            }
        } else {
            // Legacy single-queue round-robin
            if !inner.client_order.contains(&client_id) {
                inner.client_order.push_back(client_id);
            }
            notify_worker = true;
        }

        drop(inner);
        if notify_worker {
            self.condvar.notify_one();
        }

        Ok(())
    }

    /// Worker dispatch loop:
    /// Blocks until an eligible job is found or scheduler is shut down.
    /// In two-queue mode: provides guaranteed O(1) dispatch lookup via `dispatchable_queue`.
    pub fn pop_next_job(&self) -> Option<Job> {
        let mut inner = self.inner.lock().unwrap();

        loop {
            if inner.shutdown {
                return None;
            }

            if inner.enable_two_queue_scheduler {
                // --- O(1) Two-Queue Scheduler Dispatch ---
                if let Some(client_id) = inner.dispatchable_queue.pop_front() {
                    inner.in_dispatchable.remove(&client_id);

                    let job = inner
                        .client_queues
                        .get_mut(&client_id)
                        .and_then(|q| q.pop_front());

                    if let Some(job) = job {
                        // Increment in-flight counter
                        let in_flight_entry = inner.client_in_flight.entry(client_id.clone()).or_insert(0);
                        *in_flight_entry += 1;
                        let in_flight = *in_flight_entry;

                        let has_more_jobs = inner
                            .client_queues
                            .get(&client_id)
                            .map(|q| !q.is_empty())
                            .unwrap_or(false);

                        if has_more_jobs {
                            if in_flight < inner.max_in_flight_per_client {
                                // Client still has capacity: requeue to back of dispatchable queue (round-robin)
                                inner.dispatchable_queue.push_back(client_id.clone());
                                inner.in_dispatchable.insert(client_id);
                            } else {
                                // Client is saturated: park in waiting queue
                                inner.waiting_queue.insert(client_id);
                            }
                        }

                        // If there is more dispatchable work, notify another worker thread
                        if !inner.dispatchable_queue.is_empty() {
                            self.condvar.notify_one();
                        }

                        return Some(job);
                    }
                }
            } else {
                // --- Legacy Single-Queue Round-Robin Dispatch ---
                let num_clients = inner.client_order.len();
                let mut found_job = None;

                for _ in 0..num_clients {
                    let client_id = match inner.client_order.pop_front() {
                        Some(id) => id,
                        None => break,
                    };

                    let in_flight = inner.client_in_flight.get(&client_id).copied().unwrap_or(0);
                    let can_dispatch = in_flight < inner.max_in_flight_per_client;

                    if can_dispatch
                        && let Some(job) = inner.client_queues.get_mut(&client_id).and_then(|q| q.pop_front())
                    {
                        *inner.client_in_flight.entry(client_id.clone()).or_insert(0) += 1;
                        found_job = Some(job);
                        inner.client_order.push_back(client_id);
                        break;
                    }

                    inner.client_order.push_back(client_id);
                }

                if let Some(job) = found_job {
                    return Some(job);
                }
            }

            // No eligible jobs right now; wait for submission or completion
            inner = self.condvar.wait(inner).unwrap();
        }
    }

    /// Called by a worker when a job finishes. Decrements client's in_flight counter.
    /// If client was waiting (saturated), promotes them back to the dispatchable queue.
    pub fn on_job_completed(&self, client_id: &str) {
        let mut inner = self.inner.lock().unwrap();

        if let Some(count) = inner.client_in_flight.get_mut(client_id) {
            *count = count.saturating_sub(1);
            // count is a mut ref to usize which evals to u64 / u32 based on the machine's architecture. so we must saturate (stop) the subtraction when the result is about to underflow. 
        }

        let in_flight = inner.client_in_flight.get(client_id).copied().unwrap_or(0);
        let is_empty = inner
            .client_queues
            .get(client_id)
            .map(|q| q.is_empty())
            .unwrap_or(true);

        if inner.enable_two_queue_scheduler {
            // If the client was in the waiting queue, promote them to dispatchable!
            if inner.waiting_queue.remove(client_id) {
                inner.dispatchable_queue.push_back(client_id.to_string());
                inner.in_dispatchable.insert(client_id.to_string());
            }

            // Prune client if completely idle (no pending jobs and 0 in-flight)
            if is_empty && in_flight == 0 {
                inner.client_queues.remove(client_id);
                inner.client_in_flight.remove(client_id);
                inner.in_dispatchable.remove(client_id);
                inner.dispatchable_queue.retain(|id| id != client_id);
                inner.waiting_queue.remove(client_id);
            }
        } else {
            // Legacy single-queue pruning
            if is_empty && in_flight == 0 {
                inner.client_queues.remove(client_id);
                inner.client_in_flight.remove(client_id);
                inner.client_order.retain(|id| id != client_id);
            }
        }

        drop(inner);
        // Notify waiting workers that a capacity slot has opened
        self.condvar.notify_one();
    }

    /// Shutdown the scheduler and wake up all waiting workers.
    pub fn shutdown(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.shutdown = true;
        self.condvar.notify_all();
    }

    /// Inspect current scheduler statistics.
    pub fn stats(&self) -> SchedulerStats {
        let inner = self.inner.lock().unwrap();
        let queued_jobs: usize = inner.client_queues.values().map(|q| q.len()).sum();
        let in_flight_jobs: usize = inner.client_in_flight.values().sum();

        let (active_clients, dispatchable_clients, waiting_clients) = if inner.enable_two_queue_scheduler {
            let dispatchable = inner.dispatchable_queue.len();
            let waiting = inner.waiting_queue.len();

            let mut all_active: HashSet<&str> = inner.dispatchable_queue.iter().map(|s| s.as_str()).collect();
            for id in &inner.waiting_queue {
                all_active.insert(id);
            }
            for (id, cnt) in &inner.client_in_flight {
                if *cnt > 0 {
                    all_active.insert(id);
                }
            }

            (all_active.len(), dispatchable, waiting)
        } else {
            let active = inner.client_order.len();
            (active, active, 0)
        };

        SchedulerStats {
            active_clients,
            queued_jobs,
            in_flight_jobs,
            dispatchable_clients,
            waiting_clients,
        }
    }
}

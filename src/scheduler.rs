use std::collections::{HashMap, VecDeque};
use std::sync::{Condvar, Mutex};

use crate::model::Job;

struct InnerScheduler {
    client_queues: HashMap<String, VecDeque<Job>>,
    client_in_flight: HashMap<String, usize>,
    client_order: VecDeque<String>,
    max_in_flight_per_client: usize,
    shutdown: bool,
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
}

impl TwoLevelScheduler {
    pub fn new(max_in_flight_per_client: usize) -> Self {
        Self {
            inner: Mutex::new(InnerScheduler {
                client_queues: HashMap::new(),
                client_in_flight: HashMap::new(),
                client_order: VecDeque::new(),
                max_in_flight_per_client,
                shutdown: false,
            }),
            condvar: Condvar::new(),
        }
    }

    /// Submit a job to the client's FIFO queue.
    pub fn submit(&self, job: Job) {
        let mut inner = self.inner.lock().unwrap();
        if inner.shutdown {
            return;
        }

        let client_id = job.client_id.clone();

        // Enqueue into the client's isolated FIFO queue
        inner
            .client_queues
            .entry(client_id.clone())
            .or_default()
            .push_back(job);

        // Ensure client is in the round-robin order list
        if !inner.client_order.contains(&client_id) {
            inner.client_order.push_back(client_id);
        }

        // Wake up a waiting worker
        self.condvar.notify_one();
    }

    /// Worker dispatch loop:
    /// Blocks until an eligible job is found or scheduler is shut down.
    /// Walks clients in round-robin order, checking in_flight < N and non-empty FIFO queue.
    pub fn pop_next_job(&self) -> Option<Job> {
        let mut inner = self.inner.lock().unwrap();

        loop {
            if inner.shutdown {
                return None;
            }

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
                    // Increment in-flight counter
                    *inner.client_in_flight.entry(client_id.clone()).or_insert(0) += 1;
                    found_job = Some(job);

                    // Push client back to end of order (round-robin)
                    inner.client_order.push_back(client_id);
                    break;
                }

                // If not dispatched, still maintain client in rotation
                inner.client_order.push_back(client_id);
            }

            if let Some(job) = found_job {
                return Some(job);
            }

            // No eligible jobs right now; wait for submission or completion
            inner = self.condvar.wait(inner).unwrap();
        }
    }

    /// Called by a worker when a job finishes. Decrements client's in_flight counter.
    pub fn on_job_completed(&self, client_id: &str) {
        let mut inner = self.inner.lock().unwrap();

        if let Some(count) = inner.client_in_flight.get_mut(client_id) {
            *count = count.saturating_sub(1);
        }

        // Prune client if completely idle (no pending jobs and 0 in-flight)
        let is_empty = inner
            .client_queues
            .get(client_id)
            .map(|q| q.is_empty())
            .unwrap_or(true);
        let in_flight = inner.client_in_flight.get(client_id).copied().unwrap_or(0);

        if is_empty && in_flight == 0 {
            inner.client_queues.remove(client_id);
            inner.client_in_flight.remove(client_id);
            inner.client_order.retain(|id| id != client_id);
        }

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
        let active_clients = inner.client_order.len();

        SchedulerStats {
            active_clients,
            queued_jobs,
            in_flight_jobs,
        }
    }
}

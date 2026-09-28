use std::collections::{HashMap, VecDeque};
use std::sync::{Condvar, Mutex};

use crate::model::Job;

struct InnerScheduler {
    client_queues: HashMap<String, VecDeque<Job>>, // the client id and per cliinet queue 
    client_in_flight: HashMap<String, usize>, // clinet id | num of jobs in flight for each client 
    client_order: VecDeque<String>, // the order in which the clients jobs are gonna be scheduled.. 
    max_in_flight_per_client: usize,
    shutdown: bool,
}

pub struct TwoLevelScheduler {
    inner: Mutex<InnerScheduler>, // to make this safe for sharing amongst threads 
    condvar: Condvar, // basically -> avoids the situation when we have threads just sitting idly waiting for data from another thread 
    // prevents the while(!flag){..} which uses 100% of the cpu core the thread is running on. BURNS CPU , instead condvar (condtional var) asks OS 
    // to drop this thread form its schedulling loop (so this thread never runs and sleeps  0% cpu) and when the producer thread does produce the 
    // required data the condvar notifies the consumer thread to wake up with the mutex lock. 
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
        drop(inner); // A new thing i learnt , we should ideally drop the mutex guard before calling the condvar.notify_one() , as when the sleeping thread wakes up the first thing it does it try to aquire the mutex (the condvar ensures this)
        // now if the thread is not able to aquire the lock the thread now moves to the mutex wait queue (what is it called exactly??). Until the main thread gives up the ownership of the lock this worker thread can not start . 
        // also to prevent race condition (between the worker thread and the main thread to aquire and to give up the lock) we must drop the inner guard
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
                // this just fetches the id of the client who is currently the one to go (according to round robin manner)

                let in_flight = inner.client_in_flight.get(&client_id).copied().unwrap_or(0); // how many jobs in flight are there for this client 
                let can_dispatch = in_flight < inner.max_in_flight_per_client;

                if can_dispatch
                    && let Some(job) = inner.client_queues.get_mut(&client_id).and_then(|q| q.pop_front())
                // has that client submitted a job that is pending ??? (it will be in the client's queue)
                {
                    // Increment in-flight counter
                    *inner.client_in_flight.entry(client_id.clone()).or_insert(0) += 1;
                    found_job = Some(job);

                    // Push client back to end of order (round-robin)
                    inner.client_order.push_back(client_id);
                    break;
                }

                // If not dispatched, still maintain client in rotation
                // WHAT IF A CLIENT JUST SITS IDLE AND DOESNT SUBMIT ANY REQUESTS ???? , THEN THAT CLIENT IS TAKING UP RAM ON THE SERVER MACHINE WASTING IT !!!!. (I WILL TRY TO TEST THISS.)
                inner.client_order.push_back(client_id);
            }

            if let Some(job) = found_job {
                return Some(job);
            }

            // No eligible jobs right now; wait for submission or completion
            inner = self.condvar.wait(inner).unwrap();
            // look at submit function of the twolevelscheduler , there we use CondVar to notify the sleeping workers once a client has submitted a job . 
            // had we not used it , using a traditional boolean flag which would be read by all worker threads continously then that would create severe wastage of cpu cycles . 
            

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

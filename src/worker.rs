use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::sandbox::Sandbox;
use crate::scheduler::TwoLevelScheduler;
use crate::store::ResultStore;

pub struct WorkerPool {
    worker_handles: Vec<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
    scheduler: Arc<TwoLevelScheduler>,
}

impl WorkerPool {
    pub fn start(
        worker_count: usize,
        scheduler: Arc<TwoLevelScheduler>,
        result_store: Arc<ResultStore>,
        sandbox: Arc<dyn Sandbox>,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut worker_handles = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let sched = Arc::clone(&scheduler);
            let store = Arc::clone(&result_store);
            let sb = Arc::clone(&sandbox);
            let sd = Arc::clone(&shutdown);

            let handle = thread::Builder::new()
                .name(format!("distjob-worker-{}", worker_id))
                .spawn(move || {
                    while !sd.load(Ordering::Relaxed) {
                        let job = match sched.pop_next_job() {
                            Some(j) => j,
                            None => break, // Scheduler shut down
                        };

                        // Mark as Running in the result store
                        store.set_running(&job.id);

                        // Execute within sandbox
                        let execution = sb.execute(&job);

                        // Update result store with completed output
                        store.set_completed(&job.id, execution.into());

                        // Release concurrency slot for this client
                        sched.on_job_completed(&job.client_id);
                    }
                })
                .expect("Failed to spawn worker thread");

            worker_handles.push(handle);
        }

        Self {
            worker_handles,
            shutdown,
            scheduler,
        }
    }

    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.scheduler.shutdown();
        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }
    }
}

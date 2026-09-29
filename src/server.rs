use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use uuid::Uuid;

use crate::config::{Config, SandboxType};
use crate::model::{ClientCommand, Job};
use crate::protocol::{
    format_error, format_poll_ok, format_pong, format_stats, format_status_ok, format_submit_ok,
    parse_line, ProtocolFormat,
};
use crate::sandbox::{DockerSandbox, LocalProcessSandbox, MockSandbox, Sandbox};
use crate::scheduler::TwoLevelScheduler;
use crate::store::ResultStore;
use crate::worker::WorkerPool;

pub struct BrokerServer {
    pub config: Config,
    pub scheduler: Arc<TwoLevelScheduler>,
    pub result_store: Arc<ResultStore>,
    worker_pool: std::sync::Mutex<Option<WorkerPool>>,
    reaper_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
    pub shutdown: Arc<AtomicBool>,
}

impl BrokerServer {
    pub fn new(config: Config) -> Self {
        let sandbox: Arc<dyn Sandbox> = match config.sandbox_type {
            SandboxType::Docker => Arc::new(DockerSandbox::new(&config)),
            SandboxType::Local => Arc::new(LocalProcessSandbox),
            SandboxType::Mock => Arc::new(MockSandbox),
        };

        let scheduler = Arc::new(TwoLevelScheduler::from_config(&config));
        let result_store = Arc::new(ResultStore::new(config.result_ttl));
        let shutdown = Arc::new(AtomicBool::new(false));

        // Start background result reaper
        let reaper_store = Arc::clone(&result_store);
        let reaper_shutdown = Arc::clone(&shutdown);
        let reaper_handle = reaper_store.start_cleanup_thread(
            config.cleanup_interval,
            reaper_shutdown,
        );

        // Start worker pool
        let worker_pool = WorkerPool::start(
            config.worker_count,
            Arc::clone(&scheduler),
            Arc::clone(&result_store),
            sandbox,
        );

        Self {
            config,
            scheduler,
            result_store,
            worker_pool: std::sync::Mutex::new(Some(worker_pool)),
            reaper_handle: std::sync::Mutex::new(Some(reaper_handle)),
            shutdown,
        }
    }

    /// Run the server loop accepting connections.
    /// Spawns one OS thread per connection.
    pub fn run_listener(&self, listener: TcpListener) -> std::io::Result<()> {
        println!(
            "Sandboxed Job Execution Broker (v2) listening on {}",
            listener.local_addr()?
        );
        println!(
            "Configuration: workers={}, max_in_flight={}, queue_limit={:?} (mult={}), two_queue={}, sandbox={:?}, ttl={:?}",
            self.config.worker_count,
            self.config.max_in_flight_per_client,
            self.config.enable_queue_limit,
            self.config.queue_limit_multiplier,
            self.config.enable_two_queue_scheduler,
            self.config.sandbox_type,
            self.config.result_ttl
        );

        for stream in listener.incoming() {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            match stream {
                Ok(stream) => {
                    let scheduler = Arc::clone(&self.scheduler);
                    let result_store = Arc::clone(&self.result_store);
                    let config = self.config.clone();

                    thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, scheduler, result_store, config) {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    if !self.shutdown.load(Ordering::Relaxed) {
                        eprintln!("Failed to accept incoming connection: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(pool) = self.worker_pool.lock().ok().and_then(|mut l| l.take()) {
            pool.shutdown();
        }
        if let Some(reaper) = self.reaper_handle.lock().ok().and_then(|mut l| l.take()) {
            let _ = reaper.join();
        }
    }
}

impl Drop for BrokerServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn handle_connection(
    stream: TcpStream,
    scheduler: Arc<TwoLevelScheduler>,
    result_store: Arc<ResultStore>,
    config: Config,
) -> std::io::Result<()> {
    let peer_addr = stream.peer_addr()?;
    let default_client_id = format!("client-{}", peer_addr);

    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            // Client closed connection
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parsed = match parse_line(trimmed, &default_client_id) {
            Ok(p) => p,
            Err(err_msg) => {
                let err_resp = format_error(&err_msg, ProtocolFormat::Text);
                writer.write_all(err_resp.as_bytes())?;
                writer.flush()?;
                continue;
            }
        };

        match parsed.command {
            ClientCommand::Submit {
                client_id,
                language,
                code,
                stdin,
                timeout_secs,
            } => {
                let client = client_id.unwrap_or_else(|| default_client_id.clone());
                let timeout = timeout_secs
                    .unwrap_or(config.default_timeout_secs)
                    .min(config.max_timeout_secs);

                let job_id = Uuid::new_v4().to_string();
                let job = Job::new(job_id.clone(), client, language, code, stdin, timeout);

                match scheduler.submit(job.clone()) {
                    Ok(()) => {
                        // Insert into result store as Pending only if admitted
                        result_store.insert_pending(&job);

                        // Return opaque job_id immediately without blocking!
                        let resp = format_submit_ok(&job_id, parsed.format);
                        writer.write_all(resp.as_bytes())?;
                    }
                    Err(err_msg) => {
                        let resp = format_error(&err_msg, parsed.format);
                        writer.write_all(resp.as_bytes())?;
                    }
                }
            }

            ClientCommand::Poll { job_id } => {
                let resp = match result_store.get(&job_id) {
                    Some(result) => format_poll_ok(&result, parsed.format),
                    None => format_error("Job not found or expired", parsed.format),
                };
                writer.write_all(resp.as_bytes())?;
            }

            ClientCommand::Status { job_id } => {
                let resp = match result_store.get(&job_id) {
                    Some(result) => format_status_ok(&job_id, result.status, parsed.format),
                    None => format_error("Job not found or expired", parsed.format),
                };
                writer.write_all(resp.as_bytes())?;
            }

            ClientCommand::Ping => {
                let resp = format_pong(parsed.format);
                writer.write_all(resp.as_bytes())?;
            }

            ClientCommand::Stats => {
                let stats = scheduler.stats();
                let stored_count = result_store.count();
                let resp = format_stats(&stats, stored_count, parsed.format);
                writer.write_all(resp.as_bytes())?;
            }
        }

        writer.flush()?;
    }

    Ok(())
}
